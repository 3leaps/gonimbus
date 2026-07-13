package cmd

import (
	"bytes"
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

	"github.com/google/uuid"

	"github.com/3leaps/gonimbus/internal/indexsubstrate"
	"github.com/3leaps/gonimbus/pkg/opcheckpoint"
)

const (
	operationIndexGCControl      = "index-gc-control"
	operationIndexGCDelete       = "index-gc-delete"
	indexGCDeletePayloadV1       = "gonimbus.index.gc.delete.v1"
	indexGCResultType            = "gonimbus.index.gc.result.v1"
	indexGCDefaultMaxIntentBytes = 4 << 20
	indexGCMaxCheckpointEvents   = 64
)

// Variable for deterministic capacity-boundary tests. Production retains the
// four-MiB bounded-record contract used by recovery reads.
var indexGCMaxIntentBytes int64 = indexGCDefaultMaxIntentBytes

type indexGCDeleteTarget struct {
	indexGCTarget
	QuarantinePath string               `json:"quarantine_path"`
	State          string               `json:"state"`
	DeleteEntries  []indexGCDeleteEntry `json:"delete_entries,omitempty"`
}

type indexGCDeleteEntry struct {
	Path   string      `json:"path"`
	Kind   string      `json:"kind"`
	Mode   os.FileMode `json:"mode"`
	Size   int64       `json:"size,omitempty"`
	SHA256 string      `json:"sha256,omitempty"`
}

type indexGCDeleteCandidate struct {
	IndexSetID string                `json:"index_set_id"`
	Targets    []indexGCDeleteTarget `json:"targets"`
}

type indexGCDeletePayload struct {
	Type           string                   `json:"type"`
	TransactionID  string                   `json:"transaction_id"`
	PlanSHA256     string                   `json:"plan_sha256"`
	ObjectsRemoved int64                    `json:"objects_removed"`
	Candidates     []indexGCDeleteCandidate `json:"candidates"`
	RemovedBytes   int64                    `json:"verified_removed_bytes,omitempty"`
	CompletedAt    *time.Time               `json:"completed_at,omitempty"`
}

type indexGCExecutionResult struct {
	Type             string    `json:"type"`
	SchemaVersion    string    `json:"schema_version"`
	Status           string    `json:"status"`
	TransactionID    string    `json:"transaction_id"`
	PlanSHA256       string    `json:"plan_sha256"`
	IndexSetsRemoved int       `json:"index_sets_removed"`
	ObjectsRemoved   int64     `json:"objects_removed"`
	RemovedBytes     int64     `json:"verified_removed_bytes"`
	Recovered        bool      `json:"recovered"`
	CompletedAt      time.Time `json:"completed_at"`
}

type indexGCExecutionHooks struct {
	afterBoundary func(string) error
}

var indexGCTestExecutionHooks indexGCExecutionHooks

type indexGCHeldAuthority struct {
	setGuards     []*indexSetMaintenanceGuard
	durable       []*indexsubstrate.WriteLease
	durableByRoot map[string]*indexsubstrate.WriteLease
}

func (h *indexGCHeldAuthority) release() error {
	var first error
	for i := len(h.durable) - 1; i >= 0; i-- {
		if err := h.durable[i].Release(); err != nil && first == nil {
			first = err
		}
	}
	for i := len(h.setGuards) - 1; i >= 0; i-- {
		if err := h.setGuards[i].Release(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (h *indexGCHeldAuthority) assertHeld() error {
	for _, guard := range h.setGuards {
		if err := guard.AssertHeld(); err != nil {
			return err
		}
	}
	for _, lease := range h.durable {
		if err := lease.AssertHeld(); err != nil {
			return err
		}
	}
	return nil
}

func executeIndexGCPlan(
	ctx context.Context,
	store *opcheckpoint.Store,
	plan *indexGCPlan,
	maxAge time.Duration,
	maxAgeText string,
	keepLast int,
	now time.Time,
	hooks indexGCExecutionHooks,
) (indexGCExecutionResult, error) {
	if plan == nil || strings.TrimSpace(plan.PlanSHA256) == "" {
		return indexGCExecutionResult{}, fmt.Errorf("immutable GC plan is required")
	}
	if len(plan.Candidates) == 0 {
		return indexGCExecutionResult{Type: indexGCResultType, SchemaVersion: "1.0.0", Status: "success", PlanSHA256: plan.PlanSHA256, CompletedAt: time.Now().UTC()}, nil
	}
	runID := "gc_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	authority, err := acquireIndexGCAuthority(ctx, plan.Candidates, runID, false)
	if err != nil {
		return indexGCExecutionResult{}, fmt.Errorf("acquire GC authority: %w", err)
	}
	defer func() { _ = authority.release() }()

	revalidated, err := buildIndexGCPlanWithLeases(ctx, maxAge, maxAgeText, keepLast, now, authority.durableByRoot, runID)
	if err != nil {
		return indexGCExecutionResult{}, fmt.Errorf("revalidate immutable GC plan under leases: %w", err)
	}
	if revalidated.PlanSHA256 != plan.PlanSHA256 {
		return indexGCExecutionResult{}, fmt.Errorf("GC plan changed before execution: planned %s revalidated %s", plan.PlanSHA256, revalidated.PlanSHA256)
	}

	payload, err := newIndexGCDeletePayload(runID, revalidated)
	if err != nil {
		return indexGCExecutionResult{}, err
	}
	env := opcheckpoint.Envelope{
		Operation:         operationIndexGCDelete,
		RunID:             runID,
		ConfigFingerprint: revalidated.PlanSHA256,
		Status:            opcheckpoint.StatusResuming,
		Events: []opcheckpoint.CheckpointEvent{{
			Type: "delete_intent_persisted",
			At:   time.Now().UTC(),
		}},
	}
	if err := authorizeIndexGCDeletePayload(ctx, &env, &payload, authority); err != nil {
		return indexGCExecutionResult{}, fmt.Errorf("prepare GC deletion authorization before mutation: %w", err)
	}
	if err := writeIndexGCDeleteCheckpoint(context.Background(), store, &env, &payload); err != nil {
		return indexGCExecutionResult{}, fmt.Errorf("persist GC deletion intent: %w", err)
	}
	if err := runIndexGCBoundaryHook(hooks, "intent"); err != nil {
		return indexGCExecutionResult{}, err
	}
	result, err := applyIndexGCDeletePayload(ctx, store, &env, &payload, authority, hooks, false)
	if err != nil {
		if env.Status != opcheckpoint.StatusSuccess {
			env.Status = opcheckpoint.StatusFailedResumable
			env.Events = appendIndexGCCheckpointEvent(env.Events, opcheckpoint.CheckpointEvent{Type: "delete_interrupted", At: time.Now().UTC(), Detail: "recovery required"})
			_ = writeIndexGCDeleteCheckpoint(context.Background(), store, &env, &payload)
		}
		return indexGCExecutionResult{}, err
	}
	result.ObjectsRemoved = revalidated.ObjectsRemoved
	return result, nil
}

func recoverIndexGCDeletes(ctx context.Context, store *opcheckpoint.Store, hooks indexGCExecutionHooks) ([]indexGCExecutionResult, error) {
	intents, err := listIndexGCDeleteCheckpoints(store)
	if err != nil {
		return nil, err
	}
	var out []indexGCExecutionResult
	for i := range intents {
		env := intents[i]
		payload, err := parseAndValidateIndexGCDeletePayload(store, &env)
		if err != nil {
			return nil, fmt.Errorf("validate GC recovery record %s: %w", env.RunID, err)
		}
		if env.Status == opcheckpoint.StatusSuccess {
			continue
		}
		if env.Status != opcheckpoint.StatusResuming && env.Status != opcheckpoint.StatusFailedResumable {
			return nil, fmt.Errorf("GC checkpoint %s has unsupported status %q", env.RunID, env.Status)
		}
		candidates := make([]indexGCPlanCandidate, 0, len(payload.Candidates))
		for _, candidate := range payload.Candidates {
			planned := indexGCPlanCandidate{}
			planned.Info.IndexSetID = candidate.IndexSetID
			for _, target := range candidate.Targets {
				planned.Targets = append(planned.Targets, target.indexGCTarget)
			}
			candidates = append(candidates, planned)
		}
		authority, err := acquireIndexGCAuthority(ctx, candidates, env.RunID, true)
		if err != nil {
			return nil, fmt.Errorf("acquire GC recovery authority for %s: %w", env.RunID, err)
		}
		env.Status = opcheckpoint.StatusResuming
		env.Events = appendIndexGCCheckpointEvent(env.Events, opcheckpoint.CheckpointEvent{Type: "delete_recovery_started", At: time.Now().UTC()})
		if err := writeIndexGCDeleteCheckpoint(context.Background(), store, &env, &payload); err != nil {
			_ = authority.release()
			return nil, err
		}
		result, applyErr := applyIndexGCDeletePayload(ctx, store, &env, &payload, authority, hooks, true)
		releaseErr := authority.release()
		if applyErr != nil {
			if env.Status != opcheckpoint.StatusSuccess {
				env.Status = opcheckpoint.StatusFailedResumable
				_ = writeIndexGCDeleteCheckpoint(context.Background(), store, &env, &payload)
			}
			return nil, applyErr
		}
		if releaseErr != nil {
			return nil, releaseErr
		}
		out = append(out, result)
	}
	return out, nil
}

func newIndexGCDeletePayload(runID string, plan *indexGCPlan) (indexGCDeletePayload, error) {
	payload := indexGCDeletePayload{Type: indexGCDeletePayloadV1, TransactionID: runID, PlanSHA256: plan.PlanSHA256, ObjectsRemoved: plan.ObjectsRemoved}
	for _, candidate := range plan.Candidates {
		item := indexGCDeleteCandidate{IndexSetID: candidate.Info.IndexSetID}
		for _, target := range candidate.Targets {
			quarantine := filepath.Join(filepath.Dir(target.Path), indexGCQuarantineName(runID, target))
			item.Targets = append(item.Targets, indexGCDeleteTarget{
				indexGCTarget:  target,
				QuarantinePath: quarantine,
				State:          "planned",
			})
		}
		sortIndexGCDeleteTargets(item.Targets)
		payload.Candidates = append(payload.Candidates, item)
	}
	if err := validateIndexGCDeletePayload(payload); err != nil {
		return indexGCDeletePayload{}, err
	}
	return payload, nil
}

// authorizeIndexGCDeletePayload materializes every target's deletion manifest
// and proves that the largest bounded checkpoint state fits before any target
// can be renamed. A capacity or persistence failure therefore leaves all roots
// canonical and permits a clean later retry.
func authorizeIndexGCDeletePayload(ctx context.Context, env *opcheckpoint.Envelope, payload *indexGCDeletePayload, authority *indexGCHeldAuthority) error {
	for candidateIndex := range payload.Candidates {
		candidate := &payload.Candidates[candidateIndex]
		for targetIndex := range candidate.Targets {
			target := &candidate.Targets[targetIndex]
			if err := assertIndexGCMutationAuthority(ctx, payload, authority); err != nil {
				return err
			}
			originalExists, quarantineExists, err := indexGCDeleteTargetState(*target)
			if err != nil {
				return err
			}
			if !originalExists || quarantineExists {
				return fmt.Errorf("GC target is not canonical before deletion authorization: %s", target.Path)
			}
			entries, err := snapshotIndexGCDeletionManifest(target.indexGCTarget, target.Path)
			if err != nil {
				return err
			}
			target.DeleteEntries = entries
			target.State = "authorized"
		}
	}
	if err := validateIndexGCDeletePayload(*payload); err != nil {
		return err
	}
	return validateIndexGCDeleteCheckpointCapacity(env, payload)
}

func applyIndexGCDeletePayload(
	ctx context.Context,
	store *opcheckpoint.Store,
	env *opcheckpoint.Envelope,
	payload *indexGCDeletePayload,
	authority *indexGCHeldAuthority,
	hooks indexGCExecutionHooks,
	recovered bool,
) (indexGCExecutionResult, error) {
	if err := validateIndexGCDeletePayload(*payload); err != nil {
		return indexGCExecutionResult{}, err
	}
	for candidateIndex := range payload.Candidates {
		candidate := &payload.Candidates[candidateIndex]
		for targetIndex := range candidate.Targets {
			target := &candidate.Targets[targetIndex]
			if err := assertIndexGCMutationAuthority(ctx, payload, authority); err != nil {
				return indexGCExecutionResult{}, err
			}
			originalExists, quarantineExists, err := indexGCDeleteTargetState(*target)
			if err != nil {
				return indexGCExecutionResult{}, err
			}
			if originalExists && quarantineExists {
				return indexGCExecutionResult{}, fmt.Errorf("both GC target and quarantine exist for %s", target.Path)
			}
			if originalExists {
				if target.State != "authorized" {
					return indexGCExecutionResult{}, fmt.Errorf("GC target reappeared after deletion authorization: %s", target.Path)
				}
				if err := revalidateIndexGCDeleteTree(target.indexGCTarget, target.Path); err != nil {
					return indexGCExecutionResult{}, fmt.Errorf("revalidate GC target immediately before mutation: %w", err)
				}
				if err := quarantineIndexGCTarget(*target); err != nil {
					return indexGCExecutionResult{}, err
				}
				if err := runIndexGCBoundaryHook(hooks, "quarantine:"+candidate.IndexSetID+":"+target.Kind); err != nil {
					return indexGCExecutionResult{}, err
				}
				quarantineExists = true
			}
			if quarantineExists {
				switch target.State {
				case "authorized", "quarantined":
					if err := revalidateIndexGCDeleteTree(target.indexGCTarget, target.QuarantinePath); err != nil {
						return indexGCExecutionResult{}, fmt.Errorf("revalidate GC quarantine: %w", err)
					}
					if err := validateIndexGCDeletionRemainder(*target); err != nil {
						return indexGCExecutionResult{}, err
					}
					target.State = "quarantined"
					if err := writeIndexGCDeleteCheckpoint(context.Background(), store, env, payload); err != nil {
						return indexGCExecutionResult{}, err
					}
				case "deleting":
					if err := validateIndexGCDeletionRemainder(*target); err != nil {
						return indexGCExecutionResult{}, err
					}
				case "deleted":
					return indexGCExecutionResult{}, fmt.Errorf("GC quarantine reappeared after deletion: %s", target.QuarantinePath)
				default:
					return indexGCExecutionResult{}, fmt.Errorf("invalid GC target state %q", target.State)
				}
			}
		}
	}

	for candidateIndex := range payload.Candidates {
		candidate := &payload.Candidates[candidateIndex]
		for targetIndex := range candidate.Targets {
			target := &candidate.Targets[targetIndex]
			if err := assertIndexGCMutationAuthority(ctx, payload, authority); err != nil {
				return indexGCExecutionResult{}, err
			}
			originalExists, quarantineExists, err := indexGCDeleteTargetState(*target)
			if err != nil {
				return indexGCExecutionResult{}, err
			}
			if originalExists {
				return indexGCExecutionResult{}, fmt.Errorf("GC target reappeared after quarantine: %s", target.Path)
			}
			if quarantineExists {
				if target.State != "deleting" {
					if target.State != "authorized" && target.State != "quarantined" {
						return indexGCExecutionResult{}, fmt.Errorf("GC target is not deletion-authorized: %s", target.QuarantinePath)
					}
					if err := validateIndexGCDeletionRemainder(*target); err != nil {
						return indexGCExecutionResult{}, err
					}
					target.State = "deleting"
					if err := writeIndexGCDeleteCheckpoint(context.Background(), store, env, payload); err != nil {
						return indexGCExecutionResult{}, fmt.Errorf("persist GC deletion authorization: %w", err)
					}
				} else if err := validateIndexGCDeletionRemainder(*target); err != nil {
					return indexGCExecutionResult{}, err
				}
				if err := removeIndexGCQuarantine(*target, hooks, "delete-entry:"+candidate.IndexSetID+":"+target.Kind); err != nil {
					return indexGCExecutionResult{}, err
				}
				if err := runIndexGCBoundaryHook(hooks, "delete:"+candidate.IndexSetID+":"+target.Kind); err != nil {
					return indexGCExecutionResult{}, err
				}
			}
			target.State = "deleted"
			if err := writeIndexGCDeleteCheckpoint(context.Background(), store, env, payload); err != nil {
				return indexGCExecutionResult{}, err
			}
		}
	}

	completed := time.Now().UTC()
	payload.CompletedAt = &completed
	payload.RemovedBytes = 0
	for _, candidate := range payload.Candidates {
		for _, target := range candidate.Targets {
			payload.RemovedBytes += target.SizeBytes
		}
	}
	env.Status = opcheckpoint.StatusSuccess
	env.Events = appendIndexGCCheckpointEvent(env.Events, opcheckpoint.CheckpointEvent{Type: "delete_completed", At: completed})
	if err := writeIndexGCDeleteCheckpoint(context.Background(), store, env, payload); err != nil {
		payload.CompletedAt = nil
		payload.RemovedBytes = 0
		return indexGCExecutionResult{}, fmt.Errorf("persist GC deletion receipt: %w", err)
	}
	if err := runIndexGCBoundaryHook(hooks, "receipt"); err != nil {
		return indexGCExecutionResult{}, err
	}
	return indexGCExecutionResult{
		Type:             indexGCResultType,
		SchemaVersion:    "1.0.0",
		Status:           "success",
		TransactionID:    payload.TransactionID,
		PlanSHA256:       payload.PlanSHA256,
		IndexSetsRemoved: len(payload.Candidates),
		ObjectsRemoved:   payload.ObjectsRemoved,
		RemovedBytes:     payload.RemovedBytes,
		Recovered:        recovered,
		CompletedAt:      completed,
	}, nil
}

func acquireIndexGCAuthority(ctx context.Context, candidates []indexGCPlanCandidate, holder string, recovering bool) (*indexGCHeldAuthority, error) {
	h := &indexGCHeldAuthority{durableByRoot: make(map[string]*indexsubstrate.WriteLease)}
	ids := make([]string, 0, len(candidates))
	seenIDs := make(map[string]struct{})
	for _, candidate := range candidates {
		id := strings.TrimSpace(candidate.Info.IndexSetID)
		if !validFullIndexSetID(id) {
			return nil, fmt.Errorf("invalid candidate index_set_id %q", id)
		}
		if _, ok := seenIDs[id]; !ok {
			seenIDs[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		guard, err := acquireIndexSetMaintenance(ctx, id, "index-gc-"+holder)
		if err != nil {
			_ = h.release()
			return nil, fmt.Errorf("acquire set maintenance lease for %s: %w", id, err)
		}
		h.setGuards = append(h.setGuards, guard)
	}

	type durableTarget struct {
		id   string
		root string
	}
	var roots []durableTarget
	for _, candidate := range candidates {
		for _, target := range candidate.Targets {
			if target.Kind != "segment-set" {
				continue
			}
			root := target.Path
			if recovering {
				// The canonical root may already have been quarantined. Recovery
				// still holds the outside-target maintenance lease, so no new CLI
				// writer can enter while the old lock file is absent.
				if _, err := os.Lstat(root); os.IsNotExist(err) {
					continue
				}
			}
			roots = append(roots, durableTarget{id: candidate.Info.IndexSetID, root: filepath.Clean(root)})
		}
	}
	sort.Slice(roots, func(i, j int) bool { return roots[i].root < roots[j].root })
	for _, item := range roots {
		lease, err := indexsubstrate.AcquireWriteLeaseForMaintenance(item.root, item.id, "index-gc-"+holder)
		if err != nil {
			_ = h.release()
			return nil, err
		}
		h.durable = append(h.durable, lease)
		// Key by the lease's canonical Abs+Clean root. Plan revalidation may
		// observe Dir(SourcePath) in a different Windows path form (short vs
		// long names); lookup must still find this held lease so it does not
		// fall through to an exclusive availability probe against our own lock.
		h.durableByRoot[lease.SegmentSetRoot()] = lease
	}
	return h, nil
}

func assertIndexGCMutationAuthority(ctx context.Context, payload *indexGCDeletePayload, authority *indexGCHeldAuthority) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if err := authority.assertHeld(); err != nil {
		return err
	}
	inventory := make(map[string]*indexGCInventoryEntry, len(payload.Candidates))
	for _, candidate := range payload.Candidates {
		inventory[candidate.IndexSetID] = &indexGCInventoryEntry{Formats: map[string]struct{}{}, TargetMap: map[string]indexGCTarget{}}
	}
	warnings, err := blockActiveGCStateExcept(inventory, payload.TransactionID)
	if err != nil {
		return err
	}
	for id, entry := range inventory {
		if entry.Blocked {
			return fmt.Errorf("active state appeared before GC mutation for %s", id)
		}
	}
	if len(warnings) > 0 {
		return fmt.Errorf("active state appeared before GC mutation")
	}
	return nil
}

func indexGCDeleteTargetState(target indexGCDeleteTarget) (bool, bool, error) {
	original, err := pathExistsNoSymlink(target.Path)
	if err != nil {
		return false, false, err
	}
	quarantine, err := pathExistsNoSymlink(target.QuarantinePath)
	if err != nil {
		return false, false, err
	}
	return original, quarantine, nil
}

func pathExistsNoSymlink(path string) (bool, error) {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return false, fmt.Errorf("GC path is not a real directory: %s", path)
	}
	return true, nil
}

func revalidateIndexGCDeleteTree(expected indexGCTarget, actualPath string) error {
	got, err := verifiedIndexGCTarget(expected.Kind, filepath.Dir(actualPath), actualPath)
	if err != nil {
		return err
	}
	if got.SizeBytes != expected.SizeBytes || got.TreeSHA256 != expected.TreeSHA256 {
		return fmt.Errorf("tree authority changed for %s", actualPath)
	}
	return nil
}

func quarantineIndexGCTarget(target indexGCDeleteTarget) error {
	parent := filepath.Dir(target.Path)
	if parent != filepath.Dir(target.QuarantinePath) {
		return fmt.Errorf("GC quarantine must share the canonical target root")
	}
	root, err := os.OpenRoot(parent)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	if _, err := root.Lstat(filepath.Base(target.QuarantinePath)); err == nil {
		return fmt.Errorf("GC quarantine already exists: %s", target.QuarantinePath)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := root.Rename(filepath.Base(target.Path), filepath.Base(target.QuarantinePath)); err != nil {
		return fmt.Errorf("quarantine GC target: %w", err)
	}
	if err := revalidateIndexGCDeleteTree(target.indexGCTarget, target.QuarantinePath); err != nil {
		return fmt.Errorf("verify quarantined GC target: %w", err)
	}
	return nil
}

func removeIndexGCQuarantine(target indexGCDeleteTarget, hooks indexGCExecutionHooks, boundaryPrefix string) error {
	parent := filepath.Dir(target.QuarantinePath)
	parentRoot, err := os.OpenRoot(parent)
	if err != nil {
		return err
	}
	defer func() { _ = parentRoot.Close() }()
	name := filepath.Base(target.QuarantinePath)
	qroot, err := parentRoot.OpenRoot(name)
	if err != nil {
		return err
	}
	entries, err := fs.ReadDir(qroot.FS(), ".")
	if err != nil {
		_ = qroot.Close()
		return err
	}
	for _, entry := range entries {
		if err := qroot.RemoveAll(entry.Name()); err != nil {
			_ = qroot.Close()
			return fmt.Errorf("remove quarantined artifact: %w", err)
		}
		if err := runIndexGCBoundaryHook(hooks, boundaryPrefix+":"+entry.Name()); err != nil {
			_ = qroot.Close()
			return err
		}
	}
	if err := qroot.Close(); err != nil {
		return err
	}
	if err := parentRoot.Remove(name); err != nil {
		return fmt.Errorf("remove GC quarantine root: %w", err)
	}
	return nil
}

func snapshotIndexGCDeletionManifest(expected indexGCTarget, actualPath string) ([]indexGCDeleteEntry, error) {
	h := sha256.New()
	var size int64
	root, err := os.OpenRoot(actualPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	var entries []indexGCDeleteEntry
	err = fs.WalkDir(root.FS(), ".", func(rel string, dirEntry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := dirEntry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink is not allowed in GC quarantine: %s", rel)
		}
		if info.IsDir() {
			_, _ = fmt.Fprintf(h, "%s\x00%o\x00%d\x00", rel, info.Mode().Perm(), info.Size())
			if rel != "." {
				entries = append(entries, indexGCDeleteEntry{Path: filepath.ToSlash(rel), Kind: "dir", Mode: info.Mode().Perm()})
			}
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("non-regular artifact is not allowed in GC quarantine: %s", rel)
		}
		f, boundInfo, err := openBoundIndexGCRootFile(root, rel, info)
		if err != nil {
			return err
		}
		fileHash := sha256.New()
		_, _ = fmt.Fprintf(h, "%s\x00%o\x00%d\x00", rel, boundInfo.Mode().Perm(), boundInfo.Size())
		n, copyErr := io.Copy(io.MultiWriter(h, fileHash), f)
		verifyErr := verifyBoundIndexGCRootFile(root, rel, f, boundInfo)
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
		entries = append(entries, indexGCDeleteEntry{
			Path: filepath.ToSlash(rel), Kind: "file", Mode: boundInfo.Mode().Perm(), Size: n,
			SHA256: hex.EncodeToString(fileHash.Sum(nil)),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	if size != expected.SizeBytes || hex.EncodeToString(h.Sum(nil)) != expected.TreeSHA256 {
		return nil, fmt.Errorf("GC quarantine changed before deletion authorization: %s", actualPath)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	return entries, nil
}

func validateIndexGCDeletionRemainder(target indexGCDeleteTarget) error {
	expected := make(map[string]indexGCDeleteEntry, len(target.DeleteEntries))
	for _, entry := range target.DeleteEntries {
		expected[entry.Path] = entry
	}
	root, err := os.OpenRoot(target.QuarantinePath)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	return fs.WalkDir(root.FS(), ".", func(rel string, dirEntry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if rel == "." {
			return nil
		}
		key := filepath.ToSlash(rel)
		want, ok := expected[key]
		if !ok {
			return fmt.Errorf("unexpected entry appeared in deleting GC quarantine: %s", key)
		}
		info, err := dirEntry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink appeared in deleting GC quarantine: %s", key)
		}
		if want.Kind == "dir" {
			if !info.IsDir() || info.Mode().Perm() != want.Mode.Perm() {
				return fmt.Errorf("directory authority changed in deleting GC quarantine: %s", key)
			}
			return nil
		}
		if want.Kind != "file" || !info.Mode().IsRegular() || info.Mode().Perm() != want.Mode.Perm() || info.Size() != want.Size {
			return fmt.Errorf("file authority changed in deleting GC quarantine: %s", key)
		}
		f, boundInfo, err := openBoundIndexGCRootFile(root, rel, info)
		if err != nil {
			return err
		}
		h := sha256.New()
		n, copyErr := io.Copy(h, f)
		verifyErr := verifyBoundIndexGCRootFile(root, rel, f, boundInfo)
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
		if n != want.Size || hex.EncodeToString(h.Sum(nil)) != want.SHA256 {
			return fmt.Errorf("file content changed in deleting GC quarantine: %s", key)
		}
		return nil
	})
}

func writeIndexGCDeleteCheckpoint(ctx context.Context, store *opcheckpoint.Store, env *opcheckpoint.Envelope, payload *indexGCDeletePayload) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	encodedBytes, err := indexGCDeleteCheckpointEncodedBytes(env, raw)
	if err != nil {
		return err
	}
	if encodedBytes > indexGCMaxIntentBytes {
		return fmt.Errorf("GC deletion record exceeds %d bytes", indexGCMaxIntentBytes)
	}
	env.Payload = raw
	return store.WriteCheckpoint(ctx, *env)
}

// validateIndexGCDeleteCheckpointCapacity models the largest state this
// transaction can persist: all targets still carry their complete authority
// manifests, terminal receipt fields are present, and the bounded audit-event
// budget is full. Passing this check before the first rename proves later state
// transitions remain within the same recovery-record cap.
func validateIndexGCDeleteCheckpointCapacity(env *opcheckpoint.Envelope, payload *indexGCDeletePayload) error {
	probe := *payload
	probe.Candidates = make([]indexGCDeleteCandidate, len(payload.Candidates))
	var removedBytes int64
	for i := range payload.Candidates {
		probe.Candidates[i] = payload.Candidates[i]
		probe.Candidates[i].Targets = make([]indexGCDeleteTarget, len(payload.Candidates[i].Targets))
		copy(probe.Candidates[i].Targets, payload.Candidates[i].Targets)
		for j := range probe.Candidates[i].Targets {
			// "quarantined" is the longest persisted target state.
			probe.Candidates[i].Targets[j].State = "quarantined"
			removedBytes += probe.Candidates[i].Targets[j].SizeBytes
		}
	}
	maxTime := time.Date(2000, 12, 31, 23, 59, 59, 999999999, time.UTC)
	probe.CompletedAt = &maxTime
	probe.RemovedBytes = removedBytes
	raw, err := json.Marshal(probe)
	if err != nil {
		return err
	}
	probeEnv := *env
	probeEnv.Status = opcheckpoint.StatusFailedResumable
	probeEnv.Events = make([]opcheckpoint.CheckpointEvent, indexGCMaxCheckpointEvents)
	for i := range probeEnv.Events {
		probeEnv.Events[i] = opcheckpoint.CheckpointEvent{
			Type:       "delete_recovery_checkpoint_event",
			At:         maxTime,
			ErrorClass: opcheckpoint.ErrorClassCredentialsRefreshFailed,
			Detail:     "bounded recovery audit event detail",
		}
	}
	encodedBytes, err := indexGCDeleteCheckpointEncodedBytes(&probeEnv, raw)
	if err != nil {
		return err
	}
	if encodedBytes > indexGCMaxIntentBytes {
		return fmt.Errorf("GC deletion authorization requires %d bytes and exceeds bounded record capacity %d", encodedBytes, indexGCMaxIntentBytes)
	}
	return nil
}

func indexGCDeleteCheckpointEncodedBytes(env *opcheckpoint.Envelope, rawPayload []byte) (int64, error) {
	if env == nil {
		return 0, fmt.Errorf("GC checkpoint envelope is required")
	}
	probe := *env
	probe.SchemaVersion = opcheckpoint.SchemaVersion
	maxTime := time.Date(2000, 12, 31, 23, 59, 59, 999999999, time.UTC)
	if probe.CreatedAt.IsZero() {
		probe.CreatedAt = maxTime
	}
	probe.UpdatedAt = maxTime
	if probe.CheckpointID == "" {
		probe.CheckpointID = "chk_ffffffffffffffff"
	}
	probe.Payload = rawPayload
	data, err := json.MarshalIndent(probe, "", "  ")
	if err != nil {
		return 0, err
	}
	return int64(len(data) + 1), nil
}

func appendIndexGCCheckpointEvent(events []opcheckpoint.CheckpointEvent, event opcheckpoint.CheckpointEvent) []opcheckpoint.CheckpointEvent {
	if len(events) < indexGCMaxCheckpointEvents {
		return append(events, event)
	}
	// Preserve the original intent event and the most recent recovery history.
	copy(events[1:], events[2:])
	events[len(events)-1] = event
	return events
}

func listIndexGCDeleteCheckpoints(store *opcheckpoint.Store) ([]opcheckpoint.Envelope, error) {
	rootPath := filepath.Join(store.RootDir(), operationIndexGCDelete)
	rootInfo, err := os.Lstat(rootPath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return nil, fmt.Errorf("GC checkpoint operation root is unsafe")
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	entries, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		return nil, err
	}
	var out []opcheckpoint.Envelope
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 || !entry.IsDir() || !strings.HasPrefix(entry.Name(), "gc_") {
			return nil, fmt.Errorf("unrecognized GC checkpoint entry %q", entry.Name())
		}
		runRoot, err := root.OpenRoot(entry.Name())
		if err != nil {
			return nil, err
		}
		info, err := runRoot.Lstat("checkpoint.json")
		if err != nil {
			_ = runRoot.Close()
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > indexGCMaxIntentBytes {
			_ = runRoot.Close()
			return nil, fmt.Errorf("unsafe GC checkpoint for %s", entry.Name())
		}
		f, boundInfo, err := openBoundIndexGCRootFile(runRoot, "checkpoint.json", info)
		if err != nil {
			_ = runRoot.Close()
			return nil, err
		}
		data, readErr := io.ReadAll(io.LimitReader(f, indexGCMaxIntentBytes+1))
		verifyErr := verifyBoundIndexGCRootFile(runRoot, "checkpoint.json", f, boundInfo)
		closeErr := f.Close()
		_ = runRoot.Close()
		if readErr != nil || verifyErr != nil || closeErr != nil || int64(len(data)) > indexGCMaxIntentBytes {
			return nil, fmt.Errorf("read bounded GC checkpoint %s", entry.Name())
		}
		var env opcheckpoint.Envelope
		if err := decodeIndexGCStrict(data, &env); err != nil {
			return nil, err
		}
		if env.SchemaVersion != opcheckpoint.SchemaVersion || env.Operation != operationIndexGCDelete || env.RunID != entry.Name() {
			return nil, fmt.Errorf("GC checkpoint identity mismatch for %s", entry.Name())
		}
		out = append(out, env)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RunID < out[j].RunID })
	return out, nil
}

func parseAndValidateIndexGCDeletePayload(store *opcheckpoint.Store, env *opcheckpoint.Envelope) (indexGCDeletePayload, error) {
	if env == nil || env.Operation != operationIndexGCDelete {
		return indexGCDeletePayload{}, fmt.Errorf("invalid GC checkpoint envelope")
	}
	var payload indexGCDeletePayload
	if err := decodeIndexGCStrict(env.Payload, &payload); err != nil {
		return payload, err
	}
	if payload.TransactionID != env.RunID || payload.PlanSHA256 != env.ConfigFingerprint {
		return payload, opcheckpoint.ErrIdentityMismatch
	}
	if err := validateIndexGCDeletePayload(payload); err != nil {
		return payload, err
	}
	if env.Status == opcheckpoint.StatusSuccess {
		var verifiedBytes int64
		if payload.CompletedAt == nil {
			return payload, fmt.Errorf("completed GC receipt has no completion time")
		}
		for _, candidate := range payload.Candidates {
			for _, target := range candidate.Targets {
				if target.State != "deleted" {
					return payload, fmt.Errorf("completed GC receipt has non-deleted target")
				}
				verifiedBytes += target.SizeBytes
			}
		}
		if payload.RemovedBytes != verifiedBytes {
			return payload, fmt.Errorf("completed GC receipt byte total does not match targets")
		}
	} else if payload.CompletedAt != nil || payload.RemovedBytes != 0 {
		return payload, fmt.Errorf("incomplete GC intent carries terminal receipt fields")
	}
	checkpointPath, err := store.CheckpointPath(env.Operation, env.RunID)
	if err != nil || !strings.HasPrefix(filepath.Clean(checkpointPath), filepath.Clean(store.RootDir())+string(filepath.Separator)) {
		return payload, fmt.Errorf("GC checkpoint is outside its authority root")
	}
	return payload, nil
}

func validateIndexGCDeletePayload(payload indexGCDeletePayload) error {
	if payload.Type != indexGCDeletePayloadV1 || !strings.HasPrefix(payload.TransactionID, "gc_") || !validSHA256(payload.PlanSHA256) {
		return fmt.Errorf("invalid GC deletion record identity")
	}
	opts, err := indexReaderResolveOptions()
	if err != nil {
		return err
	}
	journalRoot, err := appDataPath(appDataClassCrawlJournals)
	if err != nil {
		return err
	}
	roots := map[string]string{"identity": opts.IndexesRoot, "segment-set": opts.SegmentCacheRoot, "journals": journalRoot}
	seenIDs := make(map[string]struct{})
	seenPaths := make(map[string]struct{})
	if len(payload.Candidates) == 0 {
		return fmt.Errorf("GC deletion record has no candidates")
	}
	if payload.ObjectsRemoved < 0 || payload.RemovedBytes < 0 {
		return fmt.Errorf("GC deletion record has invalid counters")
	}
	for _, candidate := range payload.Candidates {
		if !validFullIndexSetID(candidate.IndexSetID) {
			return fmt.Errorf("invalid GC candidate index_set_id")
		}
		if _, ok := seenIDs[candidate.IndexSetID]; ok {
			return fmt.Errorf("duplicate GC candidate")
		}
		seenIDs[candidate.IndexSetID] = struct{}{}
		if len(candidate.Targets) == 0 || len(candidate.Targets) > 3 {
			return fmt.Errorf("invalid GC target count")
		}
		seenKinds := make(map[string]struct{})
		for _, target := range candidate.Targets {
			root, ok := roots[target.Kind]
			if !ok || !validSHA256(target.TreeSHA256) || target.SizeBytes < 0 {
				return fmt.Errorf("invalid GC target authority")
			}
			if _, ok := seenKinds[target.Kind]; ok {
				return fmt.Errorf("duplicate GC target kind")
			}
			seenKinds[target.Kind] = struct{}{}
			wantBase := candidate.IndexSetID
			if target.Kind == "identity" {
				wantBase = "idx_" + strings.TrimPrefix(candidate.IndexSetID, "idx_")[:16]
			}
			if filepath.Dir(filepath.Clean(target.Path)) != filepath.Clean(root) || filepath.Base(target.Path) != wantBase {
				return fmt.Errorf("GC target is outside its canonical root")
			}
			wantQuarantine := filepath.Join(filepath.Clean(root), indexGCQuarantineName(payload.TransactionID, target.indexGCTarget))
			if filepath.Clean(target.QuarantinePath) != wantQuarantine {
				return fmt.Errorf("GC quarantine path does not match the transaction")
			}
			if _, ok := seenPaths[target.Path]; ok {
				return fmt.Errorf("duplicate GC target path")
			}
			seenPaths[target.Path] = struct{}{}
			entryPaths := make(map[string]struct{}, len(target.DeleteEntries))
			for _, entry := range target.DeleteEntries {
				clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(entry.Path)))
				if entry.Path == "" || clean != entry.Path || filepath.IsAbs(filepath.FromSlash(entry.Path)) || entry.Path == "." || strings.HasPrefix(entry.Path, "../") {
					return fmt.Errorf("invalid GC deletion entry path")
				}
				if _, ok := entryPaths[entry.Path]; ok {
					return fmt.Errorf("duplicate GC deletion entry path")
				}
				entryPaths[entry.Path] = struct{}{}
				switch entry.Kind {
				case "dir":
					if entry.SHA256 != "" || entry.Size != 0 {
						return fmt.Errorf("invalid GC directory deletion entry")
					}
				case "file":
					if entry.Size < 0 || !validSHA256(entry.SHA256) {
						return fmt.Errorf("invalid GC file deletion entry")
					}
				default:
					return fmt.Errorf("invalid GC deletion entry kind")
				}
			}
			if target.State == "planned" && len(target.DeleteEntries) != 0 {
				return fmt.Errorf("GC target carries deletion entries before authorization")
			}
			switch target.State {
			case "planned", "authorized", "quarantined", "deleting", "deleted":
			default:
				return fmt.Errorf("invalid GC target state %q", target.State)
			}
		}
		if _, ok := seenKinds["identity"]; !ok {
			return fmt.Errorf("GC candidate has no authoritative identity target")
		}
	}
	return nil
}

func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func decodeIndexGCStrict(data []byte, out any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if decoder.More() {
		return fmt.Errorf("GC record has trailing JSON values")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("GC record has trailing JSON values")
		}
		return err
	}
	return nil
}

func indexGCQuarantineName(runID string, target indexGCTarget) string {
	kind := strings.ReplaceAll(target.Kind, "-", "_")
	return ".gonimbus-gc-" + runID + "-" + kind + "-" + filepath.Base(target.Path)
}

func sortIndexGCDeleteTargets(targets []indexGCDeleteTarget) {
	order := map[string]int{"identity": 0, "journals": 1, "segment-set": 2}
	sort.Slice(targets, func(i, j int) bool {
		if order[targets[i].Kind] != order[targets[j].Kind] {
			return order[targets[i].Kind] < order[targets[j].Kind]
		}
		return targets[i].Path < targets[j].Path
	})
}

func runIndexGCBoundaryHook(hooks indexGCExecutionHooks, boundary string) error {
	if hooks.afterBoundary == nil {
		return nil
	}
	if err := hooks.afterBoundary(boundary); err != nil {
		return fmt.Errorf("injected GC interruption after %s: %w", boundary, err)
	}
	return nil
}
