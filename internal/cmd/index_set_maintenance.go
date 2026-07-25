package cmd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/3leaps/gonimbus/pkg/indexcoord"
	"github.com/3leaps/gonimbus/pkg/opcheckpoint"
)

const (
	operationIndexSetMaintenance = "index-set-maintenance"
	indexSetMaintenanceLeaseTTL  = 2 * time.Minute
	indexSetMaintenanceHeartbeat = 30 * time.Second
)

type indexSetMaintenanceGuard struct {
	ctx       context.Context
	authority *indexcoord.Lease
	store     *opcheckpoint.Store
	operation string
	runID     string
	lease     *opcheckpoint.Lease
	heartbeat *opcheckpoint.LeaseHeartbeat
}

func acquireIndexSetMaintenance(ctx context.Context, indexSetID, holder string) (*indexSetMaintenanceGuard, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	indexSetID = strings.TrimSpace(indexSetID)
	holder = strings.TrimSpace(holder)
	if indexSetID == "" {
		return nil, fmt.Errorf("maintenance index set id is required")
	}
	if holder == "" {
		holder = "gonimbus-" + uuid.NewString()
	}
	segmentRoot, err := indexSubstrateSegmentCacheDir(indexSetID)
	if err != nil {
		return nil, err
	}
	authority, err := indexcoord.Acquire(ctx, segmentRoot, indexSetID, holder)
	if err != nil {
		return nil, err
	}
	return &indexSetMaintenanceGuard{ctx: ctx, authority: authority, operation: operationIndexSetMaintenance, runID: indexSetID}, nil
}

func acquireIndexOperationLease(ctx context.Context, operation, runID, holder string) (*indexSetMaintenanceGuard, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	operation = strings.TrimSpace(operation)
	runID = strings.TrimSpace(runID)
	holder = strings.TrimSpace(holder)
	if operation == "" || runID == "" {
		return nil, fmt.Errorf("maintenance lease operation and run id are required")
	}
	if holder == "" {
		holder = "gonimbus-" + uuid.NewString()
	}
	store, err := openDefaultOperationCheckpointStore(ctx)
	if err != nil {
		return nil, err
	}
	lease, err := store.ClaimLease(ctx, operation, runID, holder, indexSetMaintenanceLeaseTTL)
	if err != nil {
		return nil, err
	}
	heartbeat, err := store.StartLeaseHeartbeat(ctx, operation, lease, indexSetMaintenanceHeartbeat, indexSetMaintenanceLeaseTTL)
	if err != nil {
		_ = store.ReleaseLease(operation, *lease)
		return nil, err
	}
	return &indexSetMaintenanceGuard{store: store, operation: operation, runID: runID, lease: lease, heartbeat: heartbeat}, nil
}

func (g *indexSetMaintenanceGuard) Context() context.Context {
	if g != nil && g.authority != nil {
		return g.ctx
	}
	if g == nil || g.heartbeat == nil {
		return context.Background()
	}
	return g.heartbeat.Context()
}

func (g *indexSetMaintenanceGuard) AssertHeld() error {
	if g != nil && g.authority != nil {
		if err := g.authority.AssertHeld(); err != nil {
			return fmt.Errorf("set maintenance authority lost: %w", err)
		}
		return nil
	}
	if g == nil || g.lease == nil || g.heartbeat == nil {
		return fmt.Errorf("set maintenance lease is not held")
	}
	if err := g.heartbeat.Context().Err(); err != nil {
		return fmt.Errorf("set maintenance lease lost: %w", err)
	}
	return nil
}

func (g *indexSetMaintenanceGuard) AssertHeldFor(runID string) error {
	if err := g.AssertHeld(); err != nil {
		return err
	}
	if strings.TrimSpace(runID) != g.runID {
		return opcheckpoint.ErrIdentityMismatch
	}
	if g.authority != nil {
		segmentRoot, err := indexSubstrateSegmentCacheDir(g.runID)
		if err != nil {
			return err
		}
		return g.authority.AssertHeldFor(g.runID, segmentRoot)
	}
	return nil
}

func (g *indexSetMaintenanceGuard) Authority() *indexcoord.Lease {
	if g == nil {
		return nil
	}
	return g.authority
}

// releaseAuthorityErr releases a guard that owns whole-set authority and returns
// any cleanup failure for the caller to fold into its result.
//
// Release removes the authority artifact this command created and reports a
// refusal when it cannot. Discarding that would let the command exit zero with
// residue behind it, so callers join it with their own error rather than
// replacing it — a failed operation must still say why it failed.
//
// Guards holding only an operation checkpoint lease (no whole-set authority) are
// out of scope here and keep their existing release behavior.
func releaseAuthorityErr(guard *indexSetMaintenanceGuard) error {
	if guard == nil {
		return nil
	}
	ownsAuthority := guard.authority != nil
	err := guard.Release()
	if err == nil || !ownsAuthority {
		return nil
	}
	return fmt.Errorf("release index-set authority: %w", err)
}

// releaseAuthorityInto is releaseAuthorityErr folded directly into a command's
// named result, for the paths that have nothing else to do with the failure.
func releaseAuthorityInto(runErr *error, guard *indexSetMaintenanceGuard) {
	if err := releaseAuthorityErr(guard); err != nil {
		*runErr = errors.Join(*runErr, err)
	}
}

func (g *indexSetMaintenanceGuard) Release() error {
	if g == nil {
		return nil
	}
	if g.authority != nil {
		err := g.authority.Release()
		g.authority = nil
		return err
	}
	if g.lease == nil {
		return nil
	}
	var first error
	if g.heartbeat != nil {
		if err := g.heartbeat.Stop(); err != nil {
			first = err
		}
	}
	if err := g.store.ReleaseLease(g.operation, *g.lease); err != nil && first == nil {
		first = err
	}
	g.lease = nil
	return first
}
