package cmd

import (
	"context"
	"time"

	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

const managedHeartbeatInterval = 30 * time.Second

func startManagedHeartbeat(ctx context.Context, store *jobregistry.Store, job *jobregistry.JobRecord) func() {
	if store == nil || job == nil {
		return func() {}
	}

	// Only heartbeat for managed runs.
	if job.PID <= 0 {
		return func() {}
	}

	t := time.NewTicker(managedHeartbeatInterval)
	stopped := make(chan struct{})

	go func() {
		defer close(stopped)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				now := time.Now().UTC()
				job.LastHeartbeat = &now
				_ = store.Write(job)
			}
		}
	}()

	return func() {
		t.Stop()
		<-stopped
	}
}
