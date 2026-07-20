package reflowthroughput

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// intEnv reads an integer override from the primary env name, falling back to
// the alias. Empty is 0 (profile default); a set-but-unparseable value fails
// the harness loudly rather than silently reverting to the default.
func intEnv(t *testing.T, primary, alias string) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(primary))
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv(alias))
	}
	if raw == "" {
		return 0
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("%s: %q is not an integer: %v", primary, raw, err)
	}
	return v
}

// TestHarnessMakeEntry is the Make-target entry: honors GONIMBUS_THROUGHPUT_* env.
// When env is unset (normal unit runs), the test is skipped.
func TestHarnessMakeEntry(t *testing.T) {
	bin := os.Getenv("GONIMBUS_THROUGHPUT_BINARY")
	runRoot := os.Getenv("GONIMBUS_THROUGHPUT_RUN_ROOT")
	if bin == "" || runRoot == "" {
		t.Skip("GONIMBUS_THROUGHPUT_BINARY/RUN_ROOT not set (make test-reflow-throughput entry)")
	}
	profile := os.Getenv("GONIMBUS_THROUGHPUT_PROFILE")
	if profile == "" {
		profile = ProfileSmoke
	}
	keep := os.Getenv("GONIMBUS_THROUGHPUT_KEEP") == "1"
	gomem := os.Getenv("GONIMBUS_THROUGHPUT_GOMEMLIMIT")
	// PROVIDER mirrors make test-cloud-real BYO opt-in: file (default),
	// s3-compatible (GONIMBUS_S3_TEST_*), moto (make moto-start).
	provider := os.Getenv("GONIMBUS_THROUGHPUT_PROVIDER")
	tmpfsRoot := os.Getenv("GONIMBUS_THROUGHPUT_TMPFS_CHECKPOINT_ROOT")
	if tmpfsRoot == "" {
		tmpfsRoot = os.Getenv("TMPFS_CHECKPOINT_ROOT")
	}
	// The constraining envelope: a GOMEMLIMIT binds only when it is the lowest
	// candidate in the product's limit chain. CEILING_LIFT_GOMEMLIMIT remains
	// accepted as the older spelling of the same operator value.
	constrained := os.Getenv("GONIMBUS_THROUGHPUT_CONSTRAINED_GOMEMLIMIT")
	if constrained == "" {
		constrained = os.Getenv("CONSTRAINED_GOMEMLIMIT")
	}
	if constrained == "" {
		constrained = os.Getenv("GONIMBUS_THROUGHPUT_CEILING_LIFT_GOMEMLIMIT")
	}
	if constrained == "" {
		constrained = os.Getenv("CEILING_LIFT_GOMEMLIMIT")
	}
	memoryBudget := os.Getenv("GONIMBUS_THROUGHPUT_MEMORY_BUDGET")
	if memoryBudget == "" {
		memoryBudget = os.Getenv("MEMORY_BUDGET")
	}
	// Recipe scale overrides: OBJECT_COUNT / SIZE_BYTES / PARTITIONS scale the
	// profile's synthetic corpus (0 = profile default). A set-but-unparseable
	// value fails loudly rather than silently reverting to the default.
	objectCount := intEnv(t, "GONIMBUS_THROUGHPUT_OBJECT_COUNT", "OBJECT_COUNT")
	sizeBytes := intEnv(t, "GONIMBUS_THROUGHPUT_SIZE_BYTES", "SIZE_BYTES")
	partitions := intEnv(t, "GONIMBUS_THROUGHPUT_PARTITIONS", "PARTITIONS")

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	report, err := Run(ctx, Options{
		Binary:                bin,
		Profile:               profile,
		Provider:              provider,
		RunRoot:               runRoot,
		GOMEMLIMIT:            gomem,
		ConstrainedGOMEMLIMIT: constrained,
		MemoryBudget:          memoryBudget,
		TmpfsCheckpointRoot:   tmpfsRoot,
		Keep:                  keep,
		PointTimeout:          10 * time.Minute,
		RecipeObjectCount:     objectCount,
		RecipeSizeBytes:       sizeBytes,
		RecipePartitions:      partitions,
	})
	if err != nil {
		t.Fatalf("harness: %v", err)
	}
	wantProfile := profile
	if wantProfile == "" {
		wantProfile = ProfileSmoke
	}
	if report.Profile != wantProfile {
		t.Fatalf("profile got %s want %s", report.Profile, wantProfile)
	}
	t.Logf("profile=%s points=%d evidence=%s", report.Profile, len(report.Points), report.ThroughputEvidenceClass)
}
