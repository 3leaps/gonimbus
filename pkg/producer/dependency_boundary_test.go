package producer

import (
	"os/exec"
	"strings"
	"testing"
)

// The lane executor is a library seam over neutral provider, partition, and
// admission contracts. It must not pull concrete provider SDKs, workflow
// engines, storage substrates, or CLI machinery into an embedder.
func TestDependencyBoundary(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", ".")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go list dependency boundary failed: %v\n%s", err, out)
	}

	denied := []string{
		"github.com/3leaps/gonimbus/internal/cmd",
		"github.com/3leaps/gonimbus/internal/providerdispatch",
		"github.com/3leaps/gonimbus/pkg/reflow",
		"github.com/3leaps/gonimbus/pkg/reflowstate",
		"github.com/3leaps/gonimbus/pkg/indexstore",
		"github.com/3leaps/gonimbus/pkg/provider/s3",
		"github.com/3leaps/gonimbus/pkg/provider/gcs",
		"github.com/3leaps/gonimbus/pkg/provider/file",
		"github.com/aws/aws-sdk-go-v2",
		"cloud.google.com/go/storage",
		"google.golang.org/api",
		"modernc.org/sqlite",
		"modernc.org/libc",
		"github.com/spf13/cobra",
		"github.com/spf13/viper",
		"github.com/fulmenhq/gofulmen",
	}

	deps := strings.Fields(string(out))
	for _, dep := range deps {
		for _, prefix := range denied {
			if dep == prefix || strings.HasPrefix(dep, prefix+"/") {
				t.Fatalf("pkg/producer dependency graph includes denied dependency %q via %q", prefix, dep)
			}
		}
	}
}
