package runbudget

import (
	"os/exec"
	"strings"
	"testing"
)

// The admission contract is shared by the side that enumerates and the side that
// mutates, and by provider adapters that report quota domains. It stays free of
// provider SDKs, storage substrates, and CLI machinery so any of them can depend
// on it without inheriting the others' graphs.
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
		"github.com/3leaps/gonimbus/pkg/partition",
		"github.com/3leaps/gonimbus/pkg/crawler",
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
				t.Fatalf("pkg/runbudget dependency graph includes denied dependency %q via %q", prefix, dep)
			}
		}
	}
}
