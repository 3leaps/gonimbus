package reflowthroughput

import (
	"fmt"
	"strings"
	"time"
)

// Default run and per-point budgets. These hold the smoke profile comfortably.
// They are overridable because they are sized for the default corpus, not for
// the corpus an operator asks for: a scale override that raises the object
// count without a matching budget expires mid-run, and the failure surfaces as
// a context deadline during staging rather than as a scale error.
const (
	DefaultRunBudget   = 25 * time.Minute
	DefaultPointBudget = 10 * time.Minute
	// DefaultCleanupAllowance leaves room after the run context expires for
	// verified teardown. At the default deletion fan-out, the observed
	// ~94,000-object teardown takes roughly 10 minutes; 15 minutes preserves
	// margin for service variance.
	DefaultCleanupAllowance = 15 * time.Minute
)

// Env names for the budgets, with the bare aliases the Make target also accepts.
const (
	RunBudgetEnv     = "GONIMBUS_THROUGHPUT_RUN_BUDGET"
	RunBudgetAlias   = "RUN_BUDGET"
	PointBudgetEnv   = "GONIMBUS_THROUGHPUT_POINT_TIMEOUT"
	PointBudgetAlias = "POINT_TIMEOUT"
)

// Budgets is the resolved time envelope for one harness invocation.
type Budgets struct {
	Run   time.Duration
	Point time.Duration
}

// ResolveBudgets reads both budgets through the supplied lookup so the
// resolution is testable without mutating process environment, and so the
// wiring — not just the parsing helper — can be pinned by a control.
//
// A set-but-unparseable or non-positive value is an error rather than a silent
// fallback: falling back would hand the operator a budget that cannot hold the
// scale they asked for, which is the failure this was introduced to remove.
func ResolveBudgets(getenv func(string) string) (Budgets, error) {
	run, err := resolveDuration(getenv, RunBudgetEnv, RunBudgetAlias, DefaultRunBudget)
	if err != nil {
		return Budgets{}, err
	}
	point, err := resolveDuration(getenv, PointBudgetEnv, PointBudgetAlias, DefaultPointBudget)
	if err != nil {
		return Budgets{}, err
	}
	return Budgets{Run: run, Point: point}, nil
}

// validateRunBudgetDeadline refuses a run that Go's test timeout can abort
// before the harness context expires. A test-timeout panic skips deferred
// cleanup, so this check must happen before the harness mints any artifacts.
func validateRunBudgetDeadline(remaining, runBudget time.Duration, hasDeadline bool) error {
	required := runBudget + DefaultCleanupAllowance
	if !hasDeadline || remaining > required {
		return nil
	}
	return fmt.Errorf(
		"go test deadline leaves %s but RUN_BUDGET is %s plus a %s cleanup allowance: the run could abort during cleanup and strand minted prefixes; raise TEST_TIMEOUT above %s",
		remaining.Round(time.Second),
		runBudget,
		DefaultCleanupAllowance,
		required,
	)
}

func resolveDuration(getenv func(string) string, primary, alias string, def time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(getenv(primary))
	if raw == "" {
		raw = strings.TrimSpace(getenv(alias))
	}
	if raw == "" {
		return def, nil
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not a duration: %w", primary, raw, err)
	}
	if v <= 0 {
		return 0, fmt.Errorf("%s: %q must be positive (a non-positive budget expires immediately)", primary, raw)
	}
	return v, nil
}
