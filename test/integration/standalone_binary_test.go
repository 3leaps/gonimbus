package integration

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestStandaloneBinaryVersionAndHelpWorkOutsideRepo(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("standalone binary copy/exec test is unix-focused")
	}
	goModPathBytes, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		t.Fatalf("go env GOMOD: %v", err)
	}
	goModPath := strings.TrimSpace(string(goModPathBytes))
	if goModPath == "" {
		t.Fatalf("go env GOMOD returned empty")
	}
	repoRoot := filepath.Dir(goModPath)

	buildDir := t.TempDir()
	binaryPath := filepath.Join(buildDir, "gonimbus")

	build := exec.Command("go", "build", "-o", binaryPath, "./cmd/gonimbus")
	build.Dir = repoRoot
	build.Env = os.Environ()
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, string(out))
	}

	outside := t.TempDir()
	copiedBinary := filepath.Join(outside, "gonimbus")

	// Use a direct file copy to avoid relying on platform-specific tools.
	data, err := os.ReadFile(binaryPath)
	if err != nil {
		t.Fatalf("read built binary: %v", err)
	}
	if err := os.WriteFile(copiedBinary, data, 0o755); err != nil {
		t.Fatalf("write copied binary: %v", err)
	}

	version := exec.Command(copiedBinary, "version")
	version.Dir = outside
	if out, err := version.CombinedOutput(); err != nil {
		t.Fatalf("version failed: %v\n%s", err, string(out))
	}

	help := exec.Command(copiedBinary, "--help")
	help.Dir = outside
	if out, err := help.CombinedOutput(); err != nil {
		t.Fatalf("--help failed: %v\n%s", err, string(out))
	}
}
