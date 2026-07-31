package partition

import (
	"os/exec"
	"strings"
	"testing"
)

// The plan contract is shared by the side that enumerates and the side that
// verifies before mutating. It stays free of provider SDKs, storage substrates,
// and CLI machinery so that either side can depend on it without inheriting the
// other's graph — the coupling the shared package exists to prevent.
func TestDependencyBoundary(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", ".")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go list dependency boundary failed: %v\n%s", err, out)
	}

	denied := []string{
		"github.com/3leaps/gonimbus/internal/cmd",
		"github.com/3leaps/gonimbus/internal/providerdispatch",
		"github.com/3leaps/gonimbus/pkg/provider",
		"github.com/3leaps/gonimbus/pkg/producer",
		"github.com/3leaps/gonimbus/pkg/reflow",
		"github.com/3leaps/gonimbus/pkg/reflowstate",
		"github.com/3leaps/gonimbus/pkg/indexstore",
		"modernc.org/sqlite",
		"modernc.org/libc",
		"github.com/aws/aws-sdk-go-v2",
		"cloud.google.com/go/storage",
		"google.golang.org/api",
		"github.com/spf13/cobra",
		"github.com/spf13/viper",
		"github.com/fulmenhq/gofulmen",
	}

	deps := strings.Fields(string(out))
	for _, dep := range deps {
		for _, prefix := range denied {
			if dep == prefix || strings.HasPrefix(dep, prefix+"/") {
				t.Fatalf("pkg/partition dependency graph includes denied dependency %q via %q", prefix, dep)
			}
		}
	}
}
