package reflowthroughput

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// GeneratedCorpus is one verified immutable source corpus for a harness invocation.
type GeneratedCorpus struct {
	Recipe          Recipe
	Root            string // absolute source root (operator-supplied test root subdir)
	Manifest        Manifest
	ManifestPath    string
	ReflowInputPath string
	ProbeConfigPath string
	ObjectCount     int
}

// GenerateOptions controls corpus materialization.
type GenerateOptions struct {
	Recipe Recipe
	// RunRoot is the external run directory (ownership ledger lives here).
	RunRoot string
	// SourceDirName is the relative name under RunRoot for the immutable source.
	SourceDirName string
}

// Generate materializes a deterministic corpus under RunRoot and writes
// reflow-input JSONL + sterile probe config. Source objects are never mutated
// after return; callers treat the corpus as immutable for the invocation.
func Generate(opts GenerateOptions) (GeneratedCorpus, error) {
	if opts.RunRoot == "" {
		return GeneratedCorpus{}, fmt.Errorf("run root is required")
	}
	if opts.SourceDirName == "" {
		opts.SourceDirName = "source"
	}
	if err := opts.Recipe.Validate(); err != nil {
		return GeneratedCorpus{}, err
	}
	root, err := filepath.Abs(filepath.Join(opts.RunRoot, opts.SourceDirName))
	if err != nil {
		return GeneratedCorpus{}, err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return GeneratedCorpus{}, fmt.Errorf("mkdir source: %w", err)
	}
	// EvalSymlinks: transfer reflow refuses file sources under symlink components
	// (common on macOS where /var → /private/var in temp roots).
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		root = resolved
	}

	entries := make([]ManifestEntry, 0, opts.Recipe.ObjectCount)
	var inputLines []string
	for i := 0; i < opts.Recipe.ObjectCount; i++ {
		rel := opts.Recipe.RelativeKey(i)
		abs := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			return GeneratedCorpus{}, err
		}
		body := opts.Recipe.ObjectContent(i)
		if err := os.WriteFile(abs, body, 0o644); err != nil {
			return GeneratedCorpus{}, err
		}
		digest := ContentDigest(body)
		entries = append(entries, ManifestEntry{
			RelativeKey:   rel,
			SizeBytes:     int64(len(body)),
			ContentDigest: digest,
		})
		// Absolute file:// URI form accepted by transfer reflow stdin file records.
		srcURI := fileURIFromAbs(abs)
		line, err := marshalReflowInputLine(srcURI, rel, int64(len(body)), digest)
		if err != nil {
			return GeneratedCorpus{}, err
		}
		inputLines = append(inputLines, line)
	}

	manifest, err := BuildManifest(opts.Recipe, entries)
	if err != nil {
		return GeneratedCorpus{}, err
	}
	manifestPath := filepath.Join(opts.RunRoot, "corpus.manifest.json")
	mb, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return GeneratedCorpus{}, err
	}
	if err := os.WriteFile(manifestPath, mb, 0o644); err != nil {
		return GeneratedCorpus{}, err
	}

	reflowInputPath := filepath.Join(opts.RunRoot, "reflow.input.jsonl")
	if err := os.WriteFile(reflowInputPath, []byte(strings.Join(inputLines, "\n")+"\n"), 0o644); err != nil {
		return GeneratedCorpus{}, err
	}

	probeConfigPath := filepath.Join(opts.RunRoot, "probe-config.yaml")
	if err := os.WriteFile(probeConfigPath, []byte(SyntheticProbeConfigYAML), 0o644); err != nil {
		return GeneratedCorpus{}, err
	}

	// Ownership marker lives in the external run directory, never inside a dest.
	marker := filepath.Join(opts.RunRoot, "ownership.marker")
	markerBody := fmt.Sprintf("reflowthroughput\ncreated=%s\nmanifest_digest=%s\n", monoNow().UTC().Format(time.RFC3339), manifest.Digest)
	if err := os.WriteFile(marker, []byte(markerBody), 0o644); err != nil {
		return GeneratedCorpus{}, err
	}

	return GeneratedCorpus{
		Recipe:          opts.Recipe,
		Root:            root,
		Manifest:        manifest,
		ManifestPath:    manifestPath,
		ReflowInputPath: reflowInputPath,
		ProbeConfigPath: probeConfigPath,
		ObjectCount:     opts.Recipe.ObjectCount,
	}, nil
}

func marshalReflowInputLine(sourceURI, destRel string, size int64, etag string) (string, error) {
	// ETag is not meaningful for local files; use content digest as a sterile stand-in
	// so records remain well-formed for skip-if-duplicate comparisons when applicable.
	data := map[string]any{
		"source_uri":           sourceURI,
		"source_key":           destRel,
		"source_etag":          etag,
		"source_size_bytes":    size,
		"source_last_modified": "2026-01-17T00:00:00Z",
		"dest_rel_key":         destRel,
	}
	env := map[string]any{
		"type": "gonimbus.reflow.input.v1",
		"data": data,
	}
	b, err := json.Marshal(env)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func fileURIFromAbs(abs string) string {
	// Match product: ToSlash then file:// prefix.
	p := filepath.ToSlash(abs)
	return "file://" + p
}

// VerifyManifestUnchanged re-reads the on-disk manifest and checks digest identity.
func VerifyManifestUnchanged(path string, wantDigest string) error {
	b, err := os.ReadFile(path) // #nosec G304 -- harness-owned path under operator run root
	if err != nil {
		return err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	if m.Digest != wantDigest {
		return fmt.Errorf("corpus manifest digest changed: got %s want %s", m.Digest, wantDigest)
	}
	got := DigestManifestEntries(m.Entries)
	if got != wantDigest {
		return fmt.Errorf("corpus manifest entries re-digest mismatch: got %s want %s", got, wantDigest)
	}
	return nil
}
