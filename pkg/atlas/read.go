package atlas

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type Stats struct {
	Header       Header `json:"header"`
	Tier1Keys    int64  `json:"tier1_keys"`
	Tier2Content int64  `json:"tier2_content"`
	Tier3Shards  int64  `json:"tier3_shard_content"`
	Diagnostics  int64  `json:"diagnostics"`
	ShardFiles   int    `json:"shard_files"`
}

func ReadHeader(dir string) (*Header, error) {
	data, err := os.ReadFile(filepath.Join(dir, HeaderFile)) // #nosec G304 -- atlas directory is an explicit operator CLI input.
	if err != nil {
		return nil, fmt.Errorf("read atlas header: %w", err)
	}
	var header Header
	if err := json.Unmarshal(data, &header); err != nil {
		return nil, fmt.Errorf("parse atlas header: %w", err)
	}
	if header.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("unsupported atlas schema_version %q", header.SchemaVersion)
	}
	return &header, nil
}

func ComputeStats(dir string) (*Stats, error) {
	header, err := ReadHeader(dir)
	if err != nil {
		return nil, err
	}
	stats := &Stats{Header: *header}
	contentHashes := map[string]struct{}{}
	shardContent := map[string]struct{}{}

	shardDir := filepath.Join(dir, ShardsDir)
	entries, err := os.ReadDir(shardDir)
	if err != nil {
		return nil, fmt.Errorf("read atlas shards: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		stats.ShardFiles++
		if err := scanObjectRows(filepath.Join(shardDir, entry.Name()), func(row ObjectRow) {
			stats.Tier1Keys++
			contentHashes[row.ContentHash] = struct{}{}
			shardKey := entry.Name() + "\x00" + row.ContentHash
			shardContent[shardKey] = struct{}{}
		}); err != nil {
			return nil, err
		}
	}

	stats.Tier2Content = int64(len(contentHashes))
	stats.Tier3Shards = int64(len(shardContent))
	diagCount, err := countDiagnostics(filepath.Join(dir, DiagnosticsFile))
	if err != nil {
		return nil, err
	}
	stats.Diagnostics = diagCount
	return stats, nil
}

func scanObjectRows(path string, fn func(ObjectRow)) error {
	f, err := os.Open(path) // #nosec G304 -- shard path is rooted under the explicit atlas directory.
	if err != nil {
		return fmt.Errorf("open atlas shard %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	line := 0
	for scanner.Scan() {
		line++
		var env recordEnvelope
		if err := json.Unmarshal(scanner.Bytes(), &env); err != nil {
			return fmt.Errorf("parse atlas shard %s line %d: %w", path, line, err)
		}
		if env.Type != "gonimbus.atlas.object.v1" {
			return fmt.Errorf("parse atlas shard %s line %d: unexpected record type %q", path, line, env.Type)
		}
		var row ObjectRow
		if err := json.Unmarshal(env.Data, &row); err != nil {
			return fmt.Errorf("parse atlas object row %s line %d: %w", path, line, err)
		}
		fn(row)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan atlas shard %s: %w", path, err)
	}
	return nil
}

func countDiagnostics(path string) (int64, error) {
	f, err := os.Open(path) // #nosec G304 -- diagnostics path is rooted under the explicit atlas directory.
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("open atlas diagnostics: %w", err)
	}
	defer func() { _ = f.Close() }()

	var count int64
	scanner := bufio.NewScanner(f)
	line := 0
	for scanner.Scan() {
		line++
		if strings.TrimSpace(scanner.Text()) == "" {
			continue
		}
		var env recordEnvelope
		if err := json.Unmarshal(scanner.Bytes(), &env); err != nil {
			return 0, fmt.Errorf("parse atlas diagnostics line %d: %w", line, err)
		}
		if env.Type != "gonimbus.atlas.diagnostic.v1" {
			return 0, fmt.Errorf("parse atlas diagnostics line %d: unexpected record type %q", line, env.Type)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("scan atlas diagnostics: %w", err)
	}
	return count, nil
}
