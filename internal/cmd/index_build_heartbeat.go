package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

// managedHeartbeatInterval is the product cadence. Keep in lockstep with
// indexcoord.ManagedHeartbeatInterval (duplicated here to avoid a cmd→indexcoord
// import cycle for the constant alone).
const managedHeartbeatInterval = 30 * time.Second

// heartbeatUnhealthyMarker is written under the job directory when both the
// heartbeat write and the HeartbeatPersistError write fail. Planners treat this
// as independent durable evidence that age cannot authorize termination.
const heartbeatUnhealthyMarker = "heartbeat_unhealthy"

// Test hooks (nil in production). When set, replace store TouchHeartbeat /
// RecordHeartbeatPersistError so dual-failure paths can be exercised without
// filesystem permission races.
var (
	heartbeatTouchHook       func(store *jobregistry.Store, jobID string, pid int, startMS *uint64, bootID string) error
	heartbeatRecordErrorHook func(store *jobregistry.Store, jobID string, persistErr error) error
)

// startManagedHeartbeat begins heartbeating a managed job immediately.
//
// buildCancel must cancel the same context tree used by crawl/provider/publish
// work (and the authority holder). Dual persist failure calls buildCancel so
// blocking work stops; deferred authority release then runs when the command
// unwinds. Process exit + lease release is the reliable fail-closed proof when
// the unhealthy marker cannot be written.
//
// intervalOverride <= 0 uses the product cadence; tests may inject a short
// interval. Never mutates the caller's *JobRecord.
func startManagedHeartbeat(parent context.Context, buildCancel context.CancelFunc, store *jobregistry.Store, job *jobregistry.JobRecord, intervalOverride time.Duration) (stop func(), fatal <-chan error) {
	fatalCh := make(chan error, 1)
	if store == nil || job == nil {
		close(fatalCh)
		return func() {}, fatalCh
	}
	if job.PID <= 0 || strings.TrimSpace(job.JobID) == "" {
		close(fatalCh)
		return func() {}, fatalCh
	}
	if parent == nil {
		parent = context.Background()
	}

	jobID := job.JobID
	pid := job.PID
	var startMS *uint64
	if job.ProcessStartTimeUnixMS != nil {
		v := *job.ProcessStartTimeUnixMS
		startMS = &v
	}
	bootID := job.ProcessBootID
	jobDir := store.JobDir(jobID)

	interval := managedHeartbeatInterval
	if intervalOverride > 0 {
		interval = intervalOverride
	}

	// Local lifecycle cancel: stop() ends the ticker goroutine without failing the build.
	runCtx, runCancel := context.WithCancel(parent)
	t := time.NewTicker(interval)
	stopped := make(chan struct{})
	var fatalOnce sync.Once
	// Ensure runCancel is always reachable for static analysis (G118).
	cancelRun := runCancel

	emitFatal := func(err error) {
		fatalOnce.Do(func() {
			markerPath := filepath.Join(jobDir, heartbeatUnhealthyMarker)
			if werr := writeHeartbeatUnhealthyMarker(markerPath, err); werr != nil {
				err = fmt.Errorf("%w; also failed to write unhealthy marker: %v", err, werr)
			}
			// Cancel the build tree first — reliable fail-closed path.
			if buildCancel != nil {
				buildCancel()
			}
			fatalCh <- err
			close(fatalCh)
			cancelRun()
		})
	}

	go func() {
		defer close(stopped)
		for {
			select {
			case <-runCtx.Done():
				return
			case <-t.C:
				var err error
				if heartbeatTouchHook != nil {
					err = heartbeatTouchHook(store, jobID, pid, startMS, bootID)
				} else {
					err = store.TouchHeartbeat(jobID, pid, startMS, bootID)
				}
				if err != nil {
					var recErr error
					if heartbeatRecordErrorHook != nil {
						recErr = heartbeatRecordErrorHook(store, jobID, err)
					} else {
						recErr = store.RecordHeartbeatPersistError(jobID, err)
					}
					if recErr != nil {
						_, _ = fmt.Fprintf(os.Stderr, "managed heartbeat persist failed for %s: %v (also failed to record error: %v)\n", jobID, err, recErr)
						emitFatal(fmt.Errorf("heartbeat and heartbeat-error persist both failed: %v / %v", err, recErr))
						return
					}
					_, _ = fmt.Fprintf(os.Stderr, "managed heartbeat persist failed for %s: %v\n", jobID, err)
				}
			}
		}
	}()

	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancelRun()
			t.Stop()
			<-stopped
			fatalOnce.Do(func() { close(fatalCh) })
		})
	}
	return stop, fatalCh
}

func writeHeartbeatUnhealthyMarker(path string, cause error) error {
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("heartbeat unhealthy marker is a symlink")
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	dir := filepath.Dir(path)
	if info, err := os.Lstat(dir); err != nil {
		return err
	} else if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("job dir is a symlink")
	}
	msg := "heartbeat unhealthy"
	if cause != nil {
		msg = cause.Error()
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) // #nosec G304
	if err != nil {
		return err
	}
	if _, err := f.WriteString(msg + "\n"); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// heartbeatUnhealthy reports whether the independent unhealthy marker exists.
func heartbeatUnhealthy(store *jobregistry.Store, jobID string) bool {
	if store == nil || strings.TrimSpace(jobID) == "" {
		return false
	}
	path := filepath.Join(store.JobDir(jobID), heartbeatUnhealthyMarker)
	info, err := os.Lstat(path)
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeSymlink == 0 && info.Mode().IsRegular()
}
