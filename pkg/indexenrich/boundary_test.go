package indexenrich

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDependencyBoundary(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", ".")
	cmd.Env = append(os.Environ(), "GOCACHE="+filepath.Join(t.TempDir(), "go-build"))
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))

	denied := []string{
		"github.com/3leaps/gonimbus/internal/cmd",
		"github.com/3leaps/gonimbus/internal/providerdispatch",
		"github.com/3leaps/gonimbus/pkg/indexstore",
		"github.com/3leaps/gonimbus/pkg/provider/s3",
		"github.com/3leaps/gonimbus/pkg/provider/gcs",
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
				t.Fatalf("pkg/indexenrich dependency graph includes denied dependency %q via %q", prefix, dep)
			}
		}
	}
}
