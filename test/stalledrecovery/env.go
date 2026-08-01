package stalledrecovery

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Env vars for the opt-in evidence lane. Names are generic GONIMBUS_* only —
// never productize private account/bucket identifiers in tree.
const (
	// EvidenceOptInEnv enables the non-CI stalled-recovery evidence suite.
	// Absent/empty/0/false → tests skip (same "opt-in or skip" contract as
	// GONIMBUS_S3_TEST_BUCKET for real-cloud).
	EvidenceOptInEnv = "GONIMBUS_STALLED_EVIDENCE"

	// EvidenceStrictEnv, when truthy, fails instead of skipping when the host
	// cannot run a Bind/destructive path (forces Linux/Windows runners to
	// surface gaps instead of silent skip).
	EvidenceStrictEnv = "GONIMBUS_STALLED_EVIDENCE_STRICT"

	// EvidenceRootEnv optionally places large/temp artifacts under an operator
	// root. Empty → t.TempDir(). Never logged as a secret; path may appear in
	// local diagnostics only.
	EvidenceRootEnv = "GONIMBUS_STALLED_EVIDENCE_ROOT"

	// EvidenceKeepEnv retains minted roots when set (debugging). Default is
	// cleanup via t.Cleanup.
	EvidenceKeepEnv = "GONIMBUS_STALLED_EVIDENCE_KEEP"

	// EvidenceConcurrencyEnv overrides concurrent recovery stress count.
	// Default 128. Fail closed on absurd values.
	EvidenceConcurrencyEnv = "GONIMBUS_STALLED_EVIDENCE_CONCURRENCY"
)

// Config is the harness view of the opt-in evidence lane.
// No bucket/account fields — stalled-recovery authority is local process + set-authority
// leases. Future cloud-adjacent extensions must reuse cloudtest env constants
// rather than inventing private names.
type Config struct {
	// Enabled is true when EvidenceOptInEnv is truthy.
	Enabled bool
	// Strict fails closed on unsupported platform instead of Skip.
	Strict bool
	// Root is an optional operator root for large runs.
	Root string
	// Keep retains minted paths for debugging.
	Keep bool
	// Concurrency is the concurrent recovery stress count.
	Concurrency int
}

// LoadConfig reads opt-in env. ok=false when the lane is not enabled —
// callers Skip, they do not Fail (matches cloudtest.RealS3ConfigFromEnv).
func LoadConfig() (cfg Config, ok bool) {
	if !envTruthy(EvidenceOptInEnv) {
		return Config{}, false
	}
	cfg = Config{
		Enabled:     true,
		Strict:      envTruthy(EvidenceStrictEnv),
		Root:        strings.TrimSpace(os.Getenv(EvidenceRootEnv)),
		Keep:        envTruthy(EvidenceKeepEnv),
		Concurrency: 128,
	}
	if raw := strings.TrimSpace(os.Getenv(EvidenceConcurrencyEnv)); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 10000 {
			// Fail closed at load time only when explicitly opted in with bad value.
			cfg.Concurrency = 0
		} else {
			cfg.Concurrency = n
		}
	}
	return cfg, true
}

// RequireOptIn skips the test when the evidence lane is not enabled.
func RequireOptIn(t *testing.T) Config {
	t.Helper()
	cfg, ok := LoadConfig()
	if !ok {
		t.Skipf("%s not set; skipping stalled-recovery evidence (developer opt-in lane)", EvidenceOptInEnv)
	}
	if cfg.Concurrency == 0 {
		t.Fatalf("%s must be an integer in [1,10000]", EvidenceConcurrencyEnv)
	}
	return cfg
}

// MintRoot returns a unique working directory under cfg.Root or t.TempDir().
func MintRoot(t *testing.T, cfg Config, slug string) string {
	t.Helper()
	slug = strings.Trim(slug, "/")
	if slug == "" {
		slug = "run"
	}
	base := cfg.Root
	if base == "" {
		base = t.TempDir()
		return filepath.Join(base, slug)
	}
	if err := os.MkdirAll(base, 0o750); err != nil {
		t.Fatalf("create evidence root: %v", err)
	}
	dir, err := os.MkdirTemp(base, "stalled-"+slug+"-*")
	if err != nil {
		t.Fatalf("mint evidence dir: %v", err)
	}
	if !cfg.Keep {
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
	}
	return dir
}

func envTruthy(key string) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return false
	}
	switch strings.ToLower(raw) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}
