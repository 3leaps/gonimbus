package reflowthroughput

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestSmokeProfileEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping binary smoke in short mode")
	}
	if runtime.GOOS == "windows" {
		// Pipe/path edge cases are documented; default smoke is unix-first.
		t.Skip("default local smoke is unix-focused; windows full-pipe coverage follows")
	}

	repoRoot := repoRoot(t)
	buildDir := t.TempDir()
	binPath := filepath.Join(buildDir, "gonimbus")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	absBin, sha, err := BuildBinary(ctx, repoRoot, binPath)
	if err != nil {
		t.Fatal(err)
	}
	if sha == "" {
		t.Fatal("empty binary sha")
	}

	runRoot := t.TempDir()
	report, err := Run(ctx, Options{
		Binary:       absBin,
		Profile:      ProfileSmoke,
		RunRoot:      runRoot,
		PointTimeout: 3 * time.Minute,
	})
	if err != nil {
		t.Fatalf("smoke run: %v", err)
	}
	if report.Profile != ProfileSmoke {
		t.Fatalf("profile=%s", report.Profile)
	}
	if report.ThroughputEvidenceClass != "non_provider" {
		t.Fatalf("evidence class=%s", report.ThroughputEvidenceClass)
	}
	if len(report.Points) < 1 {
		t.Fatal("no points")
	}
	pt := report.Points[0]
	if pt.HonestyOK == nil || !*pt.HonestyOK {
		t.Fatalf("honesty: %s", pt.HonestyMessage)
	}
	if pt.CompletedObjects != int64(DefaultSmokeObjects) {
		t.Fatalf("completed=%d want %d", pt.CompletedObjects, DefaultSmokeObjects)
	}
	if pt.StageExitCodes["reflow"] != 0 {
		t.Fatalf("reflow exit=%d", pt.StageExitCodes["reflow"])
	}
	// Report lives under inv-<id>/report.json; cleanup removes dests only.
	foundReport := false
	ents, _ := os.ReadDir(runRoot)
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "dest-") {
			t.Fatalf("destination %s should have been cleaned", e.Name())
		}
		if strings.HasPrefix(e.Name(), "inv-") && e.IsDir() {
			if _, err := os.Stat(filepath.Join(runRoot, e.Name(), "report.json")); err == nil {
				foundReport = true
			}
			// Nested dests under inv should also be gone.
			nested, _ := os.ReadDir(filepath.Join(runRoot, e.Name()))
			for _, n := range nested {
				if strings.HasPrefix(n.Name(), "dest-") {
					t.Fatalf("destination %s should have been cleaned", n.Name())
				}
			}
		}
	}
	if !foundReport {
		t.Fatal("report.json not found under inv-*")
	}
	if report.BinaryCommit == "" && report.BinaryVersion == "" {
		t.Log("warning: binary version/commit empty (ldflags may be absent in go test build)")
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	// test package lives at test/reflowthroughput → two levels up.
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		// Fallback via go env GOMOD
		out, err := exec.Command("go", "env", "GOMOD").Output()
		if err != nil {
			t.Fatal(err)
		}
		root = filepath.Dir(strings.TrimSpace(string(out)))
	}
	return root
}
