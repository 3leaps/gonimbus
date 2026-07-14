package scope_test

import (
	"os/exec"
	"strings"
	"testing"
)

// Storage-free object-access packages must not inherit index-store or SQLite
// dependencies (ADR-0006).
func TestScopePackageDoesNotDependOnIndexstoreOrSQLite(t *testing.T) {
	cmd := exec.Command("go", "list", "-f", "{{join .Deps \"\\n\"}}", "github.com/3leaps/gonimbus/pkg/scope")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go list deps: %v\n%s", err, out)
	}
	deps := string(out)
	banned := []string{
		"github.com/3leaps/gonimbus/pkg/indexstore",
		"github.com/3leaps/gonimbus/pkg/indexbuild",
		"github.com/3leaps/gonimbus/pkg/indexcoord",
		"modernc.org/sqlite",
	}
	for _, b := range banned {
		if strings.Contains(deps, b) {
			t.Fatalf("pkg/scope must remain storage-free; dependency %q is present in go list -deps", b)
		}
	}
}
