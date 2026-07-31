package reflowthroughput

import (
	"strings"
	"testing"
	"time"
)

// The run and per-point budgets used to be hardcoded at 25m and 10m. A scale
// override that raised OBJECT_COUNT without a matching budget then expired
// mid-run and surfaced as a context deadline during corpus staging — the
// failure named the symptom, not the cause.
//
// These drive ResolveBudgets, which is what the Make entry actually calls, so
// reverting the entry to hardcoded constants breaks the build rather than
// leaving a green test behind.

func envFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestResolveBudgetsDefaultsWhenUnset(t *testing.T) {
	got, err := ResolveBudgets(envFrom(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Run != DefaultRunBudget || got.Point != DefaultPointBudget {
		t.Fatalf("got %v/%v, want %v/%v", got.Run, got.Point, DefaultRunBudget, DefaultPointBudget)
	}
}

func TestResolveBudgetsHonorsOverride(t *testing.T) {
	got, err := ResolveBudgets(envFrom(map[string]string{
		RunBudgetEnv:   "3h",
		PointBudgetEnv: "40m",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Run != 3*time.Hour {
		t.Fatalf("run = %v, want 3h: a large-corpus run would expire at the default", got.Run)
	}
	if got.Point != 40*time.Minute {
		t.Fatalf("point = %v, want 40m", got.Point)
	}
}

func TestResolveBudgetsFallsBackToAlias(t *testing.T) {
	got, err := ResolveBudgets(envFrom(map[string]string{
		RunBudgetAlias:   "90m",
		PointBudgetAlias: "12m",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Run != 90*time.Minute || got.Point != 12*time.Minute {
		t.Fatalf("got %v/%v, want 90m/12m", got.Run, got.Point)
	}
}

// The primary name must win over the alias, or an operator who sets both gets
// whichever the implementation happened to read first.
func TestResolveBudgetsPrimaryBeatsAlias(t *testing.T) {
	got, err := ResolveBudgets(envFrom(map[string]string{
		RunBudgetEnv:   "2h",
		RunBudgetAlias: "5m",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Run != 2*time.Hour {
		t.Fatalf("run = %v, want 2h (primary must outrank alias)", got.Run)
	}
}

// A bad budget must fail loudly. Falling back would hand the operator a budget
// that cannot hold the scale they asked for — the exact failure mode this
// replaced.
func TestResolveBudgetsRejectsBadValues(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"unparseable run", map[string]string{RunBudgetEnv: "banana"}, "not a duration"},
		{"zero run", map[string]string{RunBudgetEnv: "0"}, "must be positive"},
		{"negative run", map[string]string{RunBudgetEnv: "-5m"}, "must be positive"},
		{"unparseable point", map[string]string{PointBudgetEnv: "later"}, "not a duration"},
		{"zero point", map[string]string{PointBudgetEnv: "0s"}, "must be positive"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ResolveBudgets(envFrom(tc.env))
			if err == nil {
				t.Fatal("expected an error, got a silent fallback")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

func TestRunBudgetMustFitWithinTestDeadline(t *testing.T) {
	const runBudget = 25 * time.Minute
	cases := []struct {
		name        string
		remaining   time.Duration
		hasDeadline bool
		wantErr     bool
	}{
		{name: "no test deadline", remaining: 0, hasDeadline: false},
		{name: "deadline outlasts run and cleanup", remaining: runBudget + DefaultCleanupAllowance + time.Second, hasDeadline: true},
		{name: "cleanup allowance required", remaining: runBudget + DefaultCleanupAllowance - time.Second, hasDeadline: true, wantErr: true},
		{name: "equal deadline refused", remaining: runBudget, hasDeadline: true, wantErr: true},
		{name: "shorter deadline refused", remaining: runBudget - time.Second, hasDeadline: true, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRunBudgetDeadline(tc.remaining, runBudget, tc.hasDeadline)
			if tc.wantErr && err == nil {
				t.Fatal("expected deadline guard to refuse a run that could bypass cleanup")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected deadline rejection: %v", err)
			}
			if tc.wantErr && !strings.Contains(err.Error(), "raise TEST_TIMEOUT") {
				t.Fatalf("error %q does not tell the operator how to restore the cleanup invariant", err)
			}
			if tc.wantErr && !strings.Contains(err.Error(), DefaultCleanupAllowance.String()) {
				t.Fatalf("error %q does not name the required cleanup allowance", err)
			}
		})
	}
}

// The defaults must still hold the smoke profile so an unset environment keeps
// the behaviour the accepted smoke was measured under.
func TestBudgetDefaultsUnchanged(t *testing.T) {
	if DefaultRunBudget != 25*time.Minute {
		t.Fatalf("DefaultRunBudget = %v, want 25m (smoke provenance assumes it)", DefaultRunBudget)
	}
	if DefaultPointBudget != 10*time.Minute {
		t.Fatalf("DefaultPointBudget = %v, want 10m", DefaultPointBudget)
	}
	if DefaultCleanupAllowance != 15*time.Minute {
		t.Fatalf("DefaultCleanupAllowance = %v, want 15m", DefaultCleanupAllowance)
	}
}
