package reflow

import (
	"os/exec"
	"strings"
	"testing"
)

func TestDependencyBoundary(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", ".")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go list dependency boundary failed: %v\n%s", err, out)
	}

	denied := []string{
		"github.com/3leaps/gonimbus/internal/cmd",
		"github.com/3leaps/gonimbus/internal/providerdispatch",
		"github.com/3leaps/gonimbus/pkg/provider/s3",
		"github.com/3leaps/gonimbus/pkg/provider/gcs",
		// Storageful packages must not enter the embeddable engine graph: the CLI
		// wraps pkg/reflowstate behind the minimal CheckpointStore interface, so a
		// Sumpter-style consumer never inherits sqlite/indexstore transitively.
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
				t.Fatalf("pkg/reflow dependency graph includes denied dependency %q via %q", prefix, dep)
			}
		}
	}
}
