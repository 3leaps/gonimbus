package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/indexreader"
	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
	"github.com/3leaps/gonimbus/pkg/opcheckpoint"
)

const indexGCPlanType = "gonimbus.index.gc.plan.v1"

type indexGCWarning struct {
	Path       string `json:"path"`
	IndexSetID string `json:"index_set_id,omitempty"`
	Reason     string `json:"reason"`
}

type indexGCTarget struct {
	Kind       string `json:"kind"`
	Path       string `json:"path"`
	SizeBytes  int64  `json:"size_bytes"`
	TreeSHA256 string `json:"tree_sha256"`
}

type indexGCPlanCandidate struct {
	Info     indexstore.IndexListEntry `json:"-"`
	Formats  []string                  `json:"formats"`
	Targets  []indexGCTarget           `json:"targets"`
	PlanSize int64                     `json:"planned_size_bytes"`
}

type indexGCPlan struct {
	Type             string                 `json:"type"`
	PlanSHA256       string                 `json:"plan_sha256"`
	MaxAge           string                 `json:"max_age,omitempty"`
	KeepLast         int                    `json:"keep_last,omitempty"`
	IndexSetsRemoved int                    `json:"index_sets_removed"`
	ObjectsRemoved   int64                  `json:"objects_removed"`
	PlannedSizeBytes int64                  `json:"planned_size_bytes"`
	Candidates       []indexGCPlanCandidate `json:"-"`
	Warnings         []indexGCWarning       `json:"warnings,omitempty"`
}

type indexGCInventoryEntry struct {
	Info      indexstore.IndexListEntry
	Formats   map[string]struct{}
	TargetMap map[string]indexGCTarget
	Blocked   bool
}

type indexGCIdentity struct {
	Dir  string
	File indexreader.LocalIdentityFile
}

func buildIndexGCPlan(ctx context.Context, maxAge time.Duration, maxAgeText string, keepLast int, now time.Time) (*indexGCPlan, error) {
	opts, err := indexReaderResolveOptions()
	if err != nil {
		return nil, err
	}
	journalRoot, err := appDataPath(appDataClassCrawlJournals)
	if err != nil {
		return nil, err
	}
	for _, root := range []string{opts.IndexesRoot, opts.SegmentCacheRoot, journalRoot} {
		if err := validateIndexGCArtifactRoot(root); err != nil {
			return nil, err
		}
	}

	inventory := make(map[string]*indexGCInventoryEntry)
	warnings := make([]indexGCWarning, 0)
	identities, sqliteWarnings, err := inventoryIndexGCSQLiteReadOnly(ctx, opts, inventory)
	if err != nil {
		return nil, fmt.Errorf("discover read-only SQLite indexes: %w", err)
	}
	warnings = append(warnings, sqliteWarnings...)

	// Durable discovery is safe through the existing marker reader when the
	// indexes root is deliberately excluded: this prevents its SQLite probe
	// from opening or migrating index.db before GC containment checks.
	durableOpts := opts
	durableOpts.IndexesRoot = ""
	listed, err := indexreader.ListIndexReaders(ctx, durableOpts)
	if err != nil {
		return nil, fmt.Errorf("discover durable indexes: %w", err)
	}

	for _, item := range listed {
		meta := item.Meta
		id := strings.TrimSpace(meta.IndexSetID)
		if !validFullIndexSetID(id) {
			warnings = append(warnings, indexGCWarning{Path: meta.SourcePath, IndexSetID: id, Reason: "invalid or incomplete index-set identity; retained"})
			continue
		}
		entry := inventory[id]
		if entry == nil {
			entry = &indexGCInventoryEntry{Formats: map[string]struct{}{}, TargetMap: map[string]indexGCTarget{}}
			inventory[id] = entry
		}
		if identity, ok := identities[id]; ok {
			meta.IdentityDir = identity.Dir
			meta.BaseURI = identity.File.Payload.BaseURI
			meta.Provider = identity.File.Payload.Provider
			item.Meta = meta
		}

		display, displayErr := loadIndexListDisplayEntry(ctx, opts, item)
		if displayErr != nil {
			entry.Blocked = true
			warnings = append(warnings, indexGCWarning{Path: meta.SourcePath, IndexSetID: id, Reason: fmt.Sprintf("cannot verify index metadata; retained: %v", displayErr)})
			continue
		}
		if entry.Info.IndexSetID == "" || meta.Format == indexreader.FormatSQLiteV1 {
			entry.Info = display.Info
		}
		entry.Formats[display.Format] = struct{}{}

		if strings.TrimSpace(meta.IdentityDir) == "" {
			if !indexGCHasTargetKind(entry, "identity") {
				entry.Blocked = true
				warnings = append(warnings, indexGCWarning{Path: meta.SourcePath, IndexSetID: id, Reason: "identity root is not marker-proven; retained"})
			}
		} else if target, targetErr := verifiedIndexGCTarget("identity", opts.IndexesRoot, meta.IdentityDir); targetErr != nil {
			entry.Blocked = true
			warnings = append(warnings, indexGCWarning{Path: meta.IdentityDir, IndexSetID: id, Reason: fmt.Sprintf("unsafe identity root; retained: %v", targetErr)})
		} else {
			identity, identityErr := indexreader.ReadLocalIdentityFile(filepath.Join(meta.IdentityDir, "identity.json"), opts.MaxMarkerBytes)
			if identityErr != nil || identity.IndexSetID != id {
				entry.Blocked = true
				reason := "identity marker does not match discovered set; retained"
				if identityErr != nil {
					reason = fmt.Sprintf("identity marker is not authoritative; retained: %v", identityErr)
				}
				warnings = append(warnings, indexGCWarning{Path: meta.IdentityDir, IndexSetID: id, Reason: reason})
			} else {
				entry.TargetMap[target.Path] = target
			}
		}

		if meta.Format == indexreader.FormatDurableV2 {
			segmentSetRoot := filepath.Dir(meta.SourcePath)
			target, targetErr := verifiedIndexGCTarget("segment-set", opts.SegmentCacheRoot, segmentSetRoot)
			if targetErr != nil || filepath.Base(segmentSetRoot) != id {
				entry.Blocked = true
				warnings = append(warnings, indexGCWarning{Path: segmentSetRoot, IndexSetID: id, Reason: fmt.Sprintf("unsafe segment-set root; retained: %v", targetErr)})
			} else {
				entry.TargetMap[target.Path] = target
				if leaseErr := indexsubstrate.CheckWriteLeaseAvailable(segmentSetRoot); leaseErr != nil {
					entry.Blocked = true
					warnings = append(warnings, indexGCWarning{Path: segmentSetRoot, IndexSetID: id, Reason: fmt.Sprintf("durable writer lease is not available; retained: %v", leaseErr)})
				}
			}
			journalSetRoot := filepath.Join(journalRoot, id)
			if _, statErr := os.Lstat(journalSetRoot); statErr == nil {
				journalTarget, targetErr := verifiedIndexGCTarget("journals", journalRoot, journalSetRoot)
				if targetErr != nil {
					entry.Blocked = true
					warnings = append(warnings, indexGCWarning{Path: journalSetRoot, IndexSetID: id, Reason: fmt.Sprintf("unsafe journal root; retained: %v", targetErr)})
				} else {
					entry.TargetMap[journalTarget.Path] = journalTarget
				}
			} else if !os.IsNotExist(statErr) {
				entry.Blocked = true
				warnings = append(warnings, indexGCWarning{Path: journalSetRoot, IndexSetID: id, Reason: fmt.Sprintf("cannot inspect journal root; retained: %v", statErr)})
			}
		}
	}

	warnings = append(warnings, blockUnprovenGCRoots(opts, journalRoot, inventory)...)
	activeWarnings, err := blockActiveGCState(inventory)
	if err != nil {
		return nil, err
	}
	warnings = append(warnings, activeWarnings...)

	entries := make([]*indexGCInventoryEntry, 0, len(inventory))
	for _, entry := range inventory {
		if !entry.Blocked && len(entry.TargetMap) > 0 && len(entry.Formats) > 0 && entry.Info.IndexSetID != "" {
			entries = append(entries, entry)
		}
	}
	candidates, err := selectIndexGCPlanCandidates(entries, maxAge, maxAgeText != "", keepLast, now)
	if err != nil {
		return nil, err
	}
	plan := &indexGCPlan{Type: indexGCPlanType, MaxAge: maxAgeText, KeepLast: keepLast, Candidates: candidates, Warnings: warnings}
	for i := range candidates {
		plan.ObjectsRemoved += candidates[i].Info.ObjectCount
		plan.PlannedSizeBytes += candidates[i].PlanSize
	}
	plan.IndexSetsRemoved = len(candidates)
	sort.Slice(plan.Warnings, func(i, j int) bool {
		if plan.Warnings[i].Path != plan.Warnings[j].Path {
			return plan.Warnings[i].Path < plan.Warnings[j].Path
		}
		return plan.Warnings[i].Reason < plan.Warnings[j].Reason
	})
	digest, err := indexGCPlanDigest(plan)
	if err != nil {
		return nil, err
	}
	plan.PlanSHA256 = digest
	return plan, nil
}

func inventoryIndexGCSQLiteReadOnly(ctx context.Context, opts indexreader.ResolveOptions, inventory map[string]*indexGCInventoryEntry) (map[string]indexGCIdentity, []indexGCWarning, error) {
	identities := make(map[string]indexGCIdentity)
	duplicates := make(map[string]struct{})
	var warnings []indexGCWarning
	entries, err := os.ReadDir(opts.IndexesRoot)
	if os.IsNotExist(err) {
		return identities, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	for _, dirEntry := range entries {
		dir := filepath.Join(opts.IndexesRoot, dirEntry.Name())
		info, lstatErr := os.Lstat(dir)
		if lstatErr != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			continue
		}
		identity, identityErr := indexreader.ReadLocalIdentityFile(filepath.Join(dir, "identity.json"), opts.MaxMarkerBytes)
		if identityErr != nil || !validFullIndexSetID(identity.IndexSetID) {
			continue
		}
		id := identity.IndexSetID
		entry := inventory[id]
		if entry == nil {
			entry = &indexGCInventoryEntry{Formats: map[string]struct{}{}, TargetMap: map[string]indexGCTarget{}}
			inventory[id] = entry
		}
		if prior, exists := identities[id]; exists && prior.Dir != dir {
			entry.Blocked = true
			duplicates[id] = struct{}{}
			warnings = append(warnings, indexGCWarning{Path: dir, IndexSetID: id, Reason: "duplicate authoritative identity roots; retained"})
		} else {
			identities[id] = indexGCIdentity{Dir: dir, File: identity}
		}

		dbPath := filepath.Join(dir, "index.db")
		dbInfo, dbErr := os.Lstat(dbPath)
		hasSQLite := dbErr == nil
		if dbErr != nil && !os.IsNotExist(dbErr) {
			entry.Blocked = true
			warnings = append(warnings, indexGCWarning{Path: dbPath, IndexSetID: id, Reason: fmt.Sprintf("cannot inspect SQLite artifact; retained: %v", dbErr)})
			continue
		}
		if hasSQLite && (dbInfo.Mode()&os.ModeSymlink != 0 || !dbInfo.Mode().IsRegular()) {
			entry.Blocked = true
			warnings = append(warnings, indexGCWarning{Path: dbPath, IndexSetID: id, Reason: "SQLite artifact is not a proven regular file; retained"})
			continue
		}
		if hasSQLite {
			sidecars, sidecarErr := sqliteTransactionSidecars(dbPath)
			if sidecarErr != nil || len(sidecars) > 0 {
				entry.Blocked = true
				reason := fmt.Sprintf("cannot inspect SQLite transaction sidecars; retained: %v", sidecarErr)
				if sidecarErr == nil {
					reason = fmt.Sprintf("SQLite transaction sidecars are present; retained: %s", strings.Join(sidecars, ", "))
				}
				warnings = append(warnings, indexGCWarning{Path: dbPath, IndexSetID: id, Reason: reason})
				continue
			}
		}

		target, targetErr := verifiedIndexGCTarget("identity", opts.IndexesRoot, dir)
		if targetErr != nil {
			entry.Blocked = true
			warnings = append(warnings, indexGCWarning{Path: dir, IndexSetID: id, Reason: fmt.Sprintf("unsafe identity root; retained: %v", targetErr)})
			continue
		}
		entry.TargetMap[target.Path] = target
		if !hasSQLite {
			continue
		}

		db, openErr := indexstore.OpenLocalReadOnly(ctx, dbPath)
		if openErr != nil {
			entry.Blocked = true
			warnings = append(warnings, indexGCWarning{Path: dbPath, IndexSetID: id, Reason: fmt.Sprintf("cannot inspect SQLite read-only; retained: %v", openErr)})
			continue
		}
		schemaErr := indexstore.ValidateCurrentSchemaReadOnly(ctx, db)
		var stats []indexstore.IndexListEntry
		var statsErr error
		if schemaErr == nil {
			stats, statsErr = indexstore.ListIndexSetsWithStats(ctx, db)
		}
		closeErr := db.Close()
		if schemaErr != nil || statsErr != nil || closeErr != nil {
			entry.Blocked = true
			inspectErr := schemaErr
			if inspectErr == nil {
				inspectErr = statsErr
			}
			if inspectErr == nil {
				inspectErr = closeErr
			}
			warnings = append(warnings, indexGCWarning{Path: dbPath, IndexSetID: id, Reason: fmt.Sprintf("SQLite schema or metadata cannot be proven without migration; retained: %v", inspectErr)})
			continue
		}
		if len(stats) != 1 || stats[0].IndexSetID != id {
			entry.Blocked = true
			warnings = append(warnings, indexGCWarning{Path: dbPath, IndexSetID: id, Reason: "SQLite identity does not match its authoritative marker; retained"})
			continue
		}
		afterTarget, afterErr := verifiedIndexGCTarget("identity", opts.IndexesRoot, dir)
		if afterErr != nil || afterTarget.TreeSHA256 != target.TreeSHA256 || afterTarget.SizeBytes != target.SizeBytes {
			entry.Blocked = true
			warnings = append(warnings, indexGCWarning{Path: dir, IndexSetID: id, Reason: "SQLite identity tree changed during read-only inspection; retained"})
			continue
		}
		sidecars, sidecarErr := sqliteTransactionSidecars(dbPath)
		if sidecarErr != nil || len(sidecars) > 0 {
			entry.Blocked = true
			reason := fmt.Sprintf("cannot revalidate SQLite transaction sidecars; retained: %v", sidecarErr)
			if sidecarErr == nil {
				reason = fmt.Sprintf("SQLite transaction sidecars appeared during inspection; retained: %s", strings.Join(sidecars, ", "))
			}
			warnings = append(warnings, indexGCWarning{Path: dbPath, IndexSetID: id, Reason: reason})
			continue
		}
		entry.Info = stats[0]
		entry.Formats[string(indexreader.FormatSQLiteV1)] = struct{}{}
		entry.TargetMap[target.Path] = target
	}
	for id := range duplicates {
		delete(identities, id)
	}
	return identities, warnings, nil
}

func sqliteTransactionSidecars(dbPath string) ([]string, error) {
	dir := filepath.Dir(dbPath)
	base := filepath.Base(dbPath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var found []string
	for _, entry := range entries {
		name := entry.Name()
		suffix := strings.TrimPrefix(name, base)
		if suffix == name {
			continue
		}
		switch {
		case suffix == "-wal", suffix == "-shm", suffix == "-journal",
			strings.HasPrefix(suffix, "-mj "), strings.HasPrefix(suffix, "-stmtjrnl"):
			found = append(found, name)
		}
	}
	sort.Strings(found)
	return found, nil
}

func validateIndexGCArtifactRoot(root string) error {
	abs, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return err
	}
	info, err := os.Lstat(abs)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("index GC artifact root must be a real directory: %s", abs)
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return err
	}
	if filepath.Clean(resolved) != abs {
		return fmt.Errorf("index GC artifact root is a symlink or path alias: %s", abs)
	}
	return nil
}

func indexGCHasTargetKind(entry *indexGCInventoryEntry, kind string) bool {
	if entry == nil {
		return false
	}
	for _, target := range entry.TargetMap {
		if target.Kind == kind {
			return true
		}
	}
	return false
}

func validFullIndexSetID(id string) bool {
	if !strings.HasPrefix(id, "idx_") || len(id) != len("idx_")+64 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(id, "idx_"))
	return err == nil
}

func verifiedIndexGCTarget(kind, parentRoot, targetPath string) (indexGCTarget, error) {
	parentAbs, err := filepath.Abs(filepath.Clean(parentRoot))
	if err != nil {
		return indexGCTarget{}, err
	}
	targetAbs, err := filepath.Abs(filepath.Clean(targetPath))
	if err != nil {
		return indexGCTarget{}, err
	}
	if filepath.Dir(targetAbs) != parentAbs {
		return indexGCTarget{}, fmt.Errorf("target is not a direct child of its canonical root")
	}
	parentResolved, err := filepath.EvalSymlinks(parentAbs)
	if err != nil {
		return indexGCTarget{}, fmt.Errorf("resolve parent root: %w", err)
	}
	if filepath.Clean(parentResolved) != parentAbs {
		return indexGCTarget{}, fmt.Errorf("parent root is a symlink or path alias")
	}
	targetInfo, err := os.Lstat(targetAbs)
	if err != nil {
		return indexGCTarget{}, err
	}
	if targetInfo.Mode()&os.ModeSymlink != 0 || !targetInfo.IsDir() {
		return indexGCTarget{}, fmt.Errorf("target must be a real directory")
	}
	targetResolved, err := filepath.EvalSymlinks(targetAbs)
	if err != nil {
		return indexGCTarget{}, fmt.Errorf("resolve target: %w", err)
	}
	if filepath.Dir(targetResolved) != parentResolved {
		return indexGCTarget{}, fmt.Errorf("target resolves outside its canonical root")
	}
	size, digest, err := hashIndexGCTree(targetAbs)
	if err != nil {
		return indexGCTarget{}, err
	}
	return indexGCTarget{Kind: kind, Path: targetAbs, SizeBytes: size, TreeSHA256: digest}, nil
}

func hashIndexGCTree(root string) (int64, string, error) {
	h := sha256.New()
	var size int64
	treeRoot, err := os.OpenRoot(root)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = treeRoot.Close() }()
	err = fs.WalkDir(treeRoot.FS(), ".", func(rel string, dirEntry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		path := filepath.Join(root, filepath.FromSlash(rel))
		info, err := dirEntry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink is not allowed in deletion target: %s", path)
		}
		if info.IsDir() {
			_, _ = fmt.Fprintf(h, "%s\x00%o\x00%d\x00", rel, info.Mode().Perm(), info.Size())
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("non-regular artifact is not allowed in deletion target: %s", path)
		}
		f, boundInfo, err := openBoundIndexGCRootFile(treeRoot, rel, info)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(h, "%s\x00%o\x00%d\x00", rel, boundInfo.Mode().Perm(), boundInfo.Size())
		n, copyErr := io.Copy(h, f)
		verifyErr := verifyBoundIndexGCRootFile(treeRoot, rel, f, boundInfo)
		closeErr := f.Close()
		if copyErr != nil {
			return copyErr
		}
		if verifyErr != nil {
			return verifyErr
		}
		if closeErr != nil {
			return closeErr
		}
		size += n
		return nil
	})
	if err != nil {
		return 0, "", err
	}
	return size, hex.EncodeToString(h.Sum(nil)), nil
}

func openBoundIndexGCRootFile(root *os.Root, name string, expected fs.FileInfo) (*os.File, fs.FileInfo, error) {
	namedInfo, err := root.Lstat(name)
	if err != nil {
		return nil, nil, err
	}
	if namedInfo.Mode()&os.ModeSymlink != 0 || !namedInfo.Mode().IsRegular() {
		return nil, nil, fmt.Errorf("root-scoped artifact must be a regular non-symlink file: %s", name)
	}
	if expected != nil && !os.SameFile(expected, namedInfo) {
		return nil, nil, fmt.Errorf("root-scoped artifact binding changed before open: %s", name)
	}
	f, err := root.Open(name)
	if err != nil {
		return nil, nil, err
	}
	boundInfo, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	if !boundInfo.Mode().IsRegular() || !os.SameFile(namedInfo, boundInfo) {
		_ = f.Close()
		return nil, nil, fmt.Errorf("root-scoped artifact binding changed during open: %s", name)
	}
	return f, boundInfo, nil
}

func verifyBoundIndexGCRootFile(root *os.Root, name string, f *os.File, before fs.FileInfo) error {
	after, err := f.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(before, after) || before.Mode() != after.Mode() || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return fmt.Errorf("root-scoped artifact changed during read: %s", name)
	}
	namedAfter, err := root.Lstat(name)
	if err != nil {
		return err
	}
	if namedAfter.Mode()&os.ModeSymlink != 0 || !namedAfter.Mode().IsRegular() || !os.SameFile(before, namedAfter) {
		return fmt.Errorf("root-scoped artifact binding changed after read: %s", name)
	}
	return nil
}

func blockUnprovenGCRoots(opts indexreader.ResolveOptions, journalRoot string, inventory map[string]*indexGCInventoryEntry) []indexGCWarning {
	var warnings []indexGCWarning
	for _, root := range []string{opts.IndexesRoot, opts.SegmentCacheRoot, journalRoot} {
		entries, err := os.ReadDir(root)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			warnings = append(warnings, indexGCWarning{Path: root, Reason: fmt.Sprintf("cannot enumerate artifact root; no affected artifacts are eligible: %v", err)})
			for _, item := range inventory {
				item.Blocked = true
			}
			continue
		}
		for _, dir := range entries {
			path := filepath.Join(root, dir.Name())
			info, lstatErr := os.Lstat(path)
			if lstatErr != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
				warnings = append(warnings, indexGCWarning{Path: path, Reason: "unproven artifact-root entry retained"})
				continue
			}
			if root == opts.IndexesRoot {
				identity, identityErr := indexreader.ReadLocalIdentityFile(filepath.Join(path, "identity.json"), opts.MaxMarkerBytes)
				if identityErr != nil || !validFullIndexSetID(identity.IndexSetID) {
					warnings = append(warnings, indexGCWarning{Path: path, Reason: "legacy or corrupt identity root retained"})
					continue
				}
				if item := inventory[identity.IndexSetID]; item != nil {
					if filepath.Base(path) != "idx_"+strings.TrimPrefix(identity.IndexSetID, "idx_")[:16] {
						item.Blocked = true
						warnings = append(warnings, indexGCWarning{Path: path, IndexSetID: identity.IndexSetID, Reason: "identity directory name does not match authoritative identity; retained"})
					}
				}
				continue
			}
			id := dir.Name()
			item := inventory[id]
			if !validFullIndexSetID(id) || item == nil {
				warnings = append(warnings, indexGCWarning{Path: path, IndexSetID: id, Reason: "orphan or unproven durable root retained"})
				continue
			}
			requiredKind := "segment-set"
			if root == journalRoot {
				requiredKind = "journals"
			}
			if !indexGCHasTargetKind(item, requiredKind) {
				item.Blocked = true
				warnings = append(warnings, indexGCWarning{Path: path, IndexSetID: id, Reason: "durable root is unproven by markers; retained"})
			}
		}
	}
	return warnings
}

func blockActiveGCState(inventory map[string]*indexGCInventoryEntry) ([]indexGCWarning, error) {
	var warnings []indexGCWarning
	jobsRoot, err := indexJobsRootDir()
	if err != nil {
		return nil, err
	}
	jobs, err := jobregistry.NewStore(jobsRoot).ListReadOnlyStrict()
	if err != nil {
		return nil, fmt.Errorf("inspect active index jobs: %w", err)
	}
	for _, job := range jobs {
		switch job.State {
		case jobregistry.JobStateStopped,
			jobregistry.JobStateSuccess,
			jobregistry.JobStatePartial,
			jobregistry.JobStateFailed,
			jobregistry.JobStateUnknown:
			continue
		case jobregistry.JobStateQueued,
			jobregistry.JobStateRunning,
			jobregistry.JobStateStopping:
			// Active states are bound to a set below, or conservatively block all
			// sets when the persisted record has no provable set identity.
		default:
			return nil, fmt.Errorf("inspect active index jobs: unrecognized persisted job state %q for job %s", job.State, job.JobID)
		}
		id := strings.TrimSpace(job.IndexSetID)
		if id == "" && job.Receipt != nil {
			id = strings.TrimSpace(job.Receipt.IndexSetID)
		}
		if id == "" {
			for candidateID, item := range inventory {
				item.Blocked = true
				warnings = append(warnings, indexGCWarning{Path: jobsRoot, IndexSetID: candidateID, Reason: fmt.Sprintf("active managed job %s has no bound set identity; retained conservatively", job.JobID)})
			}
			continue
		}
		if item := inventory[id]; item != nil {
			item.Blocked = true
			warnings = append(warnings, indexGCWarning{Path: jobsRoot, IndexSetID: id, Reason: fmt.Sprintf("active managed job %s is bound to this set; retained", job.JobID)})
		}
	}

	checkpointRoot, err := appDataPath(appDataClassOperationCheckpoints)
	if err != nil {
		return nil, err
	}
	checkpointTree, err := os.OpenRoot(checkpointRoot)
	if os.IsNotExist(err) {
		return warnings, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = checkpointTree.Close() }()
	err = fs.WalkDir(checkpointTree.FS(), ".", func(rel string, dirEntry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		path := filepath.Join(checkpointRoot, filepath.FromSlash(rel))
		info, err := dirEntry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("inspect operation checkpoints: symlink is not allowed: %s", path)
		}
		if info.IsDir() || info.Name() != "checkpoint.json" {
			return nil
		}
		if info.Size() > 4<<20 {
			return fmt.Errorf("inspect operation checkpoint: file exceeds 4 MiB: %s", path)
		}
		f, boundInfo, err := openBoundIndexGCRootFile(checkpointTree, rel, info)
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(io.LimitReader(f, (4<<20)+1))
		verifyErr := verifyBoundIndexGCRootFile(checkpointTree, rel, f, boundInfo)
		closeErr := f.Close()
		if readErr != nil {
			return readErr
		}
		if verifyErr != nil {
			return verifyErr
		}
		if closeErr != nil {
			return closeErr
		}
		if len(data) > 4<<20 {
			return fmt.Errorf("inspect operation checkpoint: file exceeds 4 MiB: %s", path)
		}
		var env opcheckpoint.Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			return fmt.Errorf("inspect operation checkpoint %s: %w", path, err)
		}
		if env.Status == opcheckpoint.StatusSuccess {
			return nil
		}
		var payload any
		if len(env.Payload) > 0 {
			if err := json.Unmarshal(env.Payload, &payload); err != nil {
				return fmt.Errorf("inspect operation checkpoint payload %s: %w", path, err)
			}
		}
		matched := false
		for id, item := range inventory {
			if jsonFieldHasString(payload, "index_set_id", id) {
				matched = true
				item.Blocked = true
				warnings = append(warnings, indexGCWarning{Path: path, IndexSetID: id, Reason: "active operation checkpoint is bound to this set; retained"})
			}
		}
		if !matched {
			for id, item := range inventory {
				item.Blocked = true
				warnings = append(warnings, indexGCWarning{Path: path, IndexSetID: id, Reason: "active operation checkpoint has no provable set identity; retained conservatively"})
			}
		}
		return nil
	})
	return warnings, err
}

func jsonFieldHasString(value any, field, want string) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if key == field {
				if got, ok := child.(string); ok && got == want {
					return true
				}
			}
			if jsonFieldHasString(child, field, want) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if jsonFieldHasString(child, field, want) {
				return true
			}
		}
	}
	return false
}

func selectIndexGCPlanCandidates(entries []*indexGCInventoryEntry, maxAge time.Duration, maxAgeSupplied bool, keepLast int, now time.Time) ([]indexGCPlanCandidate, error) {
	if keepLast < 0 {
		return nil, fmt.Errorf("keep-last must be greater than or equal to zero")
	}
	if maxAgeSupplied && maxAge <= 0 {
		return nil, fmt.Errorf("max-age must be greater than zero")
	}
	byBaseURI := make(map[string][]*indexGCInventoryEntry)
	for _, entry := range entries {
		byBaseURI[entry.Info.BaseURI] = append(byBaseURI[entry.Info.BaseURI], entry)
	}
	cutoff := time.Time{}
	if maxAge > 0 {
		cutoff = now.UTC().Add(-maxAge)
	}
	var out []indexGCPlanCandidate
	for _, grouped := range byBaseURI {
		sort.Slice(grouped, func(i, j int) bool {
			if grouped[i].Info.CreatedAt.Equal(grouped[j].Info.CreatedAt) {
				return grouped[i].Info.IndexSetID < grouped[j].Info.IndexSetID
			}
			return grouped[i].Info.CreatedAt.After(grouped[j].Info.CreatedAt)
		})
		start := keepLast
		if start > len(grouped) {
			start = len(grouped)
		}
		if keepLast == 0 {
			start = 0
		}
		for _, entry := range grouped[start:] {
			if maxAge > 0 && entry.Info.CreatedAt.After(cutoff) {
				continue
			}
			candidate := indexGCPlanCandidate{Info: entry.Info}
			for format := range entry.Formats {
				candidate.Formats = append(candidate.Formats, format)
			}
			for _, target := range entry.TargetMap {
				candidate.Targets = append(candidate.Targets, target)
				candidate.PlanSize += target.SizeBytes
			}
			sort.Strings(candidate.Formats)
			sort.Slice(candidate.Targets, func(i, j int) bool { return candidate.Targets[i].Path < candidate.Targets[j].Path })
			out = append(out, candidate)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Info.IndexSetID < out[j].Info.IndexSetID })
	return out, nil
}

func indexGCPlanDigest(plan *indexGCPlan) (string, error) {
	type digestCandidate struct {
		IndexSetID string          `json:"index_set_id"`
		BaseURI    string          `json:"base_uri"`
		Provider   string          `json:"provider"`
		CreatedAt  string          `json:"created_at"`
		Objects    int64           `json:"object_count"`
		Formats    []string        `json:"formats"`
		Targets    []indexGCTarget `json:"targets"`
	}
	body := struct {
		Type       string            `json:"type"`
		MaxAge     string            `json:"max_age,omitempty"`
		KeepLast   int               `json:"keep_last,omitempty"`
		Candidates []digestCandidate `json:"candidates"`
		Warnings   []indexGCWarning  `json:"warnings,omitempty"`
	}{Type: plan.Type, MaxAge: plan.MaxAge, KeepLast: plan.KeepLast, Warnings: plan.Warnings}
	for _, candidate := range plan.Candidates {
		body.Candidates = append(body.Candidates, digestCandidate{
			IndexSetID: candidate.Info.IndexSetID,
			BaseURI:    candidate.Info.BaseURI,
			Provider:   candidate.Info.Provider,
			CreatedAt:  candidate.Info.CreatedAt.UTC().Format(time.RFC3339Nano),
			Objects:    candidate.Info.ObjectCount,
			Formats:    candidate.Formats,
			Targets:    candidate.Targets,
		})
	}
	data, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}
