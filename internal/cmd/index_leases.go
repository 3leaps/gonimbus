package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/indexcoord"
	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

// Stable machine-readable output discriminators for the lease surface.
const (
	leaseReportDocType = "gonimbus.index.set_authority_lease.v1"
	leaseReapDocType   = "gonimbus.index.set_authority_reap.v1"
)

var (
	indexLeaseCmd = &cobra.Command{
		Use:   "lease",
		Short: "Inspect and reclaim index set-authority leases",
		Long: `Inspect and reclaim whole-set authority lock files.

Each index build takes a stable, cross-process set-authority lock before it
mutates a canonical index set. The lock is advisory: the OS drops it when the
holder exits, but the lock file itself is left behind. 'lease ls' reports the
live lock-state of each file (held / unheld / missing / invalid) and joins it to
the job registry for attribution. 'lease reap' removes provably-unheld residue.

The held/unheld verdict is decided solely by a non-mutating lock probe. A job
record, a PID, or a holder name is attribution only and never authorizes a
removal: a live holder is never reclaimed.`,
	}

	indexLeaseLsCmd = &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list"},
		Short:   "List index set-authority leases and their lock-state",
		Args:    cobra.NoArgs,
		RunE:    runIndexLeaseLs,
	}

	indexLeaseReapCmd = &cobra.Command{
		Use:   "reap [idx_<id-prefix> ...]",
		Short: "Reclaim provably-unheld set-authority leases",
		Long: `Reclaim provably-unheld set-authority lease residue.

With no arguments, every unheld lease is targeted. With one or more idx_ IDs (or
prefixes), only those leases are targeted. A held lease is always refused with
its holder named — it is never removed.

Reaping mutates local state, so it requires explicit opt-in: without --confirm
(or --force) the command performs a dry run and only reports what it would
remove.`,
		RunE: runIndexLeaseReap,
	}
)

func init() {
	indexCmd.AddCommand(indexLeaseCmd)
	indexLeaseCmd.AddCommand(indexLeaseLsCmd)
	indexLeaseCmd.AddCommand(indexLeaseReapCmd)

	indexLeaseLsCmd.Flags().Bool("json", false, "Output as JSON")
	indexLeaseReapCmd.Flags().Bool("json", false, "Output as JSON")
	indexLeaseReapCmd.Flags().Bool("confirm", false, "Perform the reclaim (without this, reap is a dry run)")
	indexLeaseReapCmd.Flags().Bool("force", false, "Reclaim non-interactively (equivalent to --confirm; the held-lease and lock gates always apply)")
}

// guardDoctorLeaseFlags fails closed when doctor's flags are combined in a way
// the lease surface cannot honour. The lease surface derives its own authority
// root and does not consume doctor's index-store health/target flags, so a
// silent mismatch (e.g. `--root X --release-stale` reclaiming the default store)
// must be rejected rather than surprise the operator. `index lease` remains the
// primary surface for lease operations.
//
// It runs on EVERY doctor invocation, not only the lease modes: --confirm and
// --force are lease-mutation opt-ins and must never be quietly accepted as
// ordinary read-only doctor flags. Rejection therefore happens before any target
// resolution, listing, or reclaim.
func guardDoctorLeaseFlags(cmd *cobra.Command, leases, releaseStale bool) error {
	if leases && releaseStale {
		return fmt.Errorf("--leases and --release-stale are mutually exclusive; --leases lists, --release-stale reclaims")
	}
	// Applies in every mode, including plain `index doctor`: a mutation opt-in
	// without the mutating mode is an operator error, not a no-op.
	if !releaseStale {
		for _, name := range []string{"confirm", "force"} {
			if cmd.Flags().Changed(name) {
				return fmt.Errorf("--%s only applies with --release-stale", name)
			}
		}
	}
	if !leases && !releaseStale {
		return nil
	}
	for _, name := range []string{"root", "db", "format", "stats", "detail", "verbose"} {
		if cmd.Flags().Changed(name) {
			return fmt.Errorf("--%s is an index-store health flag and does not apply to the lease surface; use 'index lease' for lease operations", name)
		}
	}
	return nil
}

// leaseAuthorityRoot resolves the stable set-authority root beside the local
// segment cache, matching where index builds place their authority locks.
func leaseAuthorityRoot() (string, error) {
	segmentCacheRoot, err := appDataPath(appDataClassSegmentCache)
	if err != nil {
		return "", err
	}
	return indexcoord.AuthorityRoot(filepath.Join(segmentCacheRoot, "authority-probe"))
}

// leaseJobSnapshot returns a byte-preserving job snapshot for attribution. A
// registry read failure degrades attribution to "unmatched" rather than failing
// the read-only lease report.
func leaseJobSnapshot() []jobregistry.JobRecord {
	jobsRoot, err := indexJobsRootDir()
	if err != nil {
		return nil
	}
	jobs, err := jobregistry.NewStore(jobsRoot).ListReadOnlyStrict()
	if err != nil {
		return nil
	}
	return jobs
}

type leaseAttrJSON struct {
	Matched      bool   `json:"matched"`
	JobID        string `json:"job_id,omitempty"`
	PID          int    `json:"pid,omitempty"`
	ProcessAlive bool   `json:"process_alive"`
	JobState     string `json:"job_state,omitempty"`
}

type leaseReportJSON struct {
	Type        string        `json:"type"`
	IndexSetID  string        `json:"index_set_id"`
	Path        string        `json:"path,omitempty"`
	Verdict     string        `json:"verdict"`
	Holder      string        `json:"holder,omitempty"`
	AcquiredAt  string        `json:"acquired_at,omitempty"`
	AgeSeconds  int64         `json:"age_seconds,omitempty"`
	Detail      string        `json:"detail,omitempty"`
	Attribution leaseAttrJSON `json:"attribution"`
}

type leaseReapJSON struct {
	Type       string `json:"type"`
	IndexSetID string `json:"index_set_id"`
	Verdict    string `json:"verdict"`
	Reclaimed  bool   `json:"reclaimed"`
	Refused    bool   `json:"refused,omitempty"`
	Skipped    bool   `json:"skipped,omitempty"`
	DryRun     bool   `json:"dry_run"`
	Holder     string `json:"holder,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Error      string `json:"error,omitempty"`
}

func leaseAgeSeconds(report indexcoord.LeaseReport, now time.Time) int64 {
	ref := report.AcquiredAt
	if ref.IsZero() {
		ref = report.ModTime
	}
	if ref.IsZero() {
		return 0
	}
	age := now.Sub(ref)
	if age < 0 {
		return 0
	}
	return int64(age.Seconds())
}

func toLeaseReportJSON(report indexcoord.LeaseReport, now time.Time) leaseReportJSON {
	return leaseReportJSON{
		Type:       leaseReportDocType,
		IndexSetID: report.IndexSetID,
		Path:       report.Path,
		Verdict:    string(report.Verdict),
		Holder:     report.Holder,
		AcquiredAt: formatLeaseTime(report.AcquiredAt),
		AgeSeconds: leaseAgeSeconds(report, now),
		Detail:     report.Detail,
		Attribution: leaseAttrJSON{
			Matched:      report.Attribution.Matched,
			JobID:        report.Attribution.JobID,
			PID:          report.Attribution.PID,
			ProcessAlive: report.Attribution.ProcessAlive,
			JobState:     report.Attribution.JobState,
		},
	}
}

func formatLeaseTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func runIndexLeaseLs(cmd *cobra.Command, _ []string) error {
	jsonOut, _ := cmd.Flags().GetBool("json")
	return listLeases(cmd.OutOrStdout(), jsonOut)
}

func listLeases(out io.Writer, jsonOut bool) error {
	authorityRoot, err := leaseAuthorityRoot()
	if err != nil {
		return err
	}
	reports, err := indexcoord.EnumerateLeases(authorityRoot, leaseJobSnapshot())
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if jsonOut {
		rows := make([]leaseReportJSON, 0, len(reports))
		for _, r := range reports {
			rows = append(rows, toLeaseReportJSON(r, now))
		}
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(rows)
	}
	return printLeaseTable(out, reports, now)
}

func printLeaseTable(out io.Writer, reports []indexcoord.LeaseReport, now time.Time) error {
	if len(reports) == 0 {
		_, _ = fmt.Fprintln(out, "No set-authority leases found")
		return nil
	}
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	defer func() { _ = w.Flush() }()
	_, _ = fmt.Fprintln(w, "INDEX_SET\tVERDICT\tHOLDER\tAGE\tJOB\tPID\tALIVE\tATTRIBUTION")
	for _, r := range reports {
		holder := r.Holder
		if holder == "" {
			holder = "-"
		}
		age := formatLeaseAge(leaseAgeSeconds(r, now))
		job := "-"
		pid := "-"
		alive := "-"
		attr := "unmatched"
		if r.Attribution.Matched {
			job = r.Attribution.JobID
			pid = fmt.Sprintf("%d", r.Attribution.PID)
			if r.Attribution.ProcessAlive {
				alive = "yes"
			} else {
				alive = "no"
			}
			attr = "job:" + r.Attribution.JobState
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			shortIndexSetID(r.IndexSetID), r.Verdict, holder, age, job, pid, alive, attr)
	}
	return nil
}

func formatLeaseAge(seconds int64) string {
	if seconds <= 0 {
		return "-"
	}
	return (time.Duration(seconds) * time.Second).String()
}

func runIndexLeaseReap(cmd *cobra.Command, args []string) error {
	jsonOut, _ := cmd.Flags().GetBool("json")
	confirm, _ := cmd.Flags().GetBool("confirm")
	force, _ := cmd.Flags().GetBool("force")
	return reapLeases(cmd.OutOrStdout(), args, confirm || force, jsonOut)
}

// reapLeases reclaims provably-unheld leases. With no targets it reaps every
// unheld lease; with targets it reaps only the matching leases. doReap gates the
// mutation: false performs a dry run. A held lease is always refused, never
// removed — the safety gate is independent of doReap.
func reapLeases(out io.Writer, targets []string, doReap, jsonOut bool) error {
	authorityRoot, err := leaseAuthorityRoot()
	if err != nil {
		return err
	}
	reports, err := indexcoord.EnumerateLeases(authorityRoot, leaseJobSnapshot())
	if err != nil {
		return err
	}

	selected, err := selectReapCandidates(reports, targets)
	if err != nil {
		return err
	}

	results := make([]leaseReapJSON, 0, len(selected))
	for _, lease := range selected {
		res := leaseReapJSON{
			Type:       leaseReapDocType,
			IndexSetID: lease.IndexSetID,
			Verdict:    string(lease.Verdict),
			Holder:     lease.Holder,
			DryRun:     !doReap,
		}
		if lease.Verdict == indexcoord.LeaseHeld {
			res.Refused = true
			_, _ = fmt.Fprintf(os.Stderr, "WARN refusing to reap held set-authority lease %s (holder %q)\n", lease.IndexSetID, lease.Holder)
			results = append(results, res)
			continue
		}
		if lease.Verdict == indexcoord.LeaseInvalid {
			// Indeterminate residue is operator-attention, not blind removal: an
			// unparseable or malformed artifact is never auto-reaped.
			res.Skipped = true
			res.Reason = "invalid/indeterminate lease requires operator attention"
			results = append(results, res)
			continue
		}
		if !doReap {
			// Dry run: report the intended action without mutating.
			results = append(results, res)
			continue
		}
		reclaim, reclaimErr := indexcoord.ReclaimUnheldLease(authorityRoot, lease.IndexSetID)
		res.Verdict = string(reclaim.Verdict)
		res.Reclaimed = reclaim.Reclaimed
		if reclaim.Holder != "" {
			res.Holder = reclaim.Holder
		}
		switch {
		case reclaimErr != nil && reclaim.Verdict == indexcoord.LeaseHeld:
			// Raced from unheld to held between enumerate and reclaim: still refused.
			res.Refused = true
			_, _ = fmt.Fprintf(os.Stderr, "WARN refusing to reap held set-authority lease %s (holder %q)\n", lease.IndexSetID, res.Holder)
		case reclaimErr != nil:
			res.Error = reclaimErr.Error()
		case res.Reclaimed:
			_, _ = fmt.Fprintf(os.Stderr, "WARN reclaimed unheld set-authority lease %s (holder %q)\n", lease.IndexSetID, res.Holder)
		}
		results = append(results, res)
	}

	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(results)
	}
	return printReapSummary(out, results, doReap)
}

// selectReapCandidates chooses which enumerated leases to act on. Without
// targets it selects only unheld leases (conservative auto-reap of dead
// residue). With targets it selects every lease whose ID matches a target
// prefix, regardless of verdict, so an explicitly named held lease is surfaced
// (and then refused by the reap gate).
func selectReapCandidates(reports []indexcoord.LeaseReport, targets []string) ([]indexcoord.LeaseReport, error) {
	if len(targets) == 0 {
		var out []indexcoord.LeaseReport
		for _, r := range reports {
			if r.Verdict == indexcoord.LeaseUnheld {
				out = append(out, r)
			}
		}
		return out, nil
	}
	var out []indexcoord.LeaseReport
	for _, target := range targets {
		want := strings.TrimPrefix(strings.TrimSpace(target), "idx_")
		if want == "" {
			return nil, fmt.Errorf("invalid lease target %q", target)
		}
		matched := 0
		for _, r := range reports {
			hexID := strings.TrimPrefix(r.IndexSetID, "idx_")
			if hexID != "" && strings.HasPrefix(hexID, want) {
				out = append(out, r)
				matched++
			}
		}
		if matched == 0 {
			return nil, fmt.Errorf("no set-authority lease matches %q", target)
		}
	}
	return out, nil
}

func printReapSummary(out io.Writer, results []leaseReapJSON, doReap bool) error {
	if len(results) == 0 {
		_, _ = fmt.Fprintln(out, "No unheld set-authority leases to reclaim")
		return nil
	}
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	defer func() { _ = w.Flush() }()
	_, _ = fmt.Fprintln(w, "INDEX_SET\tVERDICT\tACTION\tHOLDER")
	reclaimed := 0
	wouldReap := 0
	for _, r := range results {
		var action string
		switch {
		case r.Refused:
			action = "refused-held"
		case r.Skipped:
			action = "skipped-invalid"
		case r.Error != "":
			action = "error"
		case r.Reclaimed:
			action = "reclaimed"
			reclaimed++
		case !doReap:
			action = "would-reap"
			wouldReap++
		default:
			action = "skipped"
		}
		holder := r.Holder
		if holder == "" {
			holder = "-"
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", shortIndexSetID(r.IndexSetID), r.Verdict, action, holder)
	}
	_ = w.Flush()
	if !doReap {
		_, _ = fmt.Fprintf(out, "\nDry run: %d unheld lease(s) would be reclaimed. Re-run with --confirm to reclaim.\n", wouldReap)
	} else {
		_, _ = fmt.Fprintf(out, "\nReclaimed %d unheld set-authority lease(s).\n", reclaimed)
	}
	return nil
}
