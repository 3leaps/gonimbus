package cmd

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

func runIndexJobsGC(cmd *cobra.Command, _ []string) error {
	maxAgeStr, _ := cmd.Flags().GetString("max-age")
	maxAgeStr = strings.TrimSpace(maxAgeStr)
	if maxAgeStr == "" {
		maxAgeStr = "168h"
	}
	maxAge, err := time.ParseDuration(maxAgeStr)
	if err != nil {
		return fmt.Errorf("invalid --max-age: %w", err)
	}
	if maxAge <= 0 {
		return fmt.Errorf("--max-age must be > 0")
	}

	root, err := indexJobsRootDir()
	if err != nil {
		return err
	}
	store := jobregistry.NewStore(root)

	jobs, err := store.List()
	if err != nil {
		return err
	}

	dryRun, _ := cmd.Flags().GetBool("dry-run")

	now := time.Now().UTC()
	deleted := 0
	for _, j := range jobs {
		if j.EndedAt == nil {
			continue
		}
		age := now.Sub(j.EndedAt.UTC())
		if age <= maxAge {
			continue
		}

		// Only prune terminal states.
		switch j.State {
		case jobregistry.JobStateSuccess, jobregistry.JobStatePartial, jobregistry.JobStateFailed, jobregistry.JobStateStopped, jobregistry.JobStateUnknown:
			// ok
		default:
			continue
		}

		if !dryRun {
			if err := os.RemoveAll(store.JobDir(j.JobID)); err != nil {
				return fmt.Errorf("remove job dir: %w", err)
			}
		}
		deleted++
	}

	if dryRun {
		_, _ = fmt.Fprintf(os.Stdout, "would_delete=%d\n", deleted)
		return nil
	}
	_, _ = fmt.Fprintf(os.Stdout, "deleted=%d\n", deleted)
	return nil
}
