//go:build cloudintegration

package cmd_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/test/cloudtest"
)

// findBinary locates the gonimbus binary for testing.
// Looks in bin/ directory relative to project root.
func findBinary(t *testing.T) string {
	t.Helper()

	// Try relative to current directory (when running from project root)
	candidates := []string{
		"bin/gonimbus",
		"../../bin/gonimbus",
		"../../../bin/gonimbus",
	}

	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			abs, _ := filepath.Abs(path)
			return abs
		}
	}

	t.Skip("gonimbus binary not found - run 'make build' first")
	return ""
}

func TestInspectCommand_CloudIntegration(t *testing.T) {
	cloudtest.SkipIfUnavailable(t)
	ctx := context.Background()
	binary := findBinary(t)

	t.Run("lists objects in bucket", func(t *testing.T) {
		bucket := cloudtest.CreateBucket(t, ctx)
		cloudtest.PutObjects(t, ctx, bucket, []string{
			"file1.txt",
			"file2.txt",
			"subdir/file3.txt",
		})

		cmd := exec.Command(binary, "inspect",
			"s3://"+bucket+"/",
			"--endpoint", cloudtest.Endpoint,
			"--json",
		)
		cmd.Env = append(os.Environ(),
			"AWS_ACCESS_KEY_ID="+cloudtest.TestAccessKeyID,
			"AWS_SECRET_ACCESS_KEY="+cloudtest.TestSecretAccessKey,
			"AWS_REGION="+cloudtest.Region,
		)

		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		err := cmd.Run()
		require.NoError(t, err, "stderr: %s", stderr.String())

		// Parse JSONL output
		lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
		assert.Len(t, lines, 3, "expected 3 objects")

		for _, line := range lines {
			var obj map[string]interface{}
			require.NoError(t, json.Unmarshal([]byte(line), &obj))
			assert.Contains(t, obj, "key")
			assert.Contains(t, obj, "size")
		}
	})

	t.Run("filters by prefix", func(t *testing.T) {
		bucket := cloudtest.CreateBucket(t, ctx)
		cloudtest.PutObjects(t, ctx, bucket, []string{
			"data/file1.txt",
			"data/file2.txt",
			"other/file3.txt",
		})

		cmd := exec.Command(binary, "inspect",
			"s3://"+bucket+"/data/",
			"--endpoint", cloudtest.Endpoint,
			"--json",
		)
		cmd.Env = append(os.Environ(),
			"AWS_ACCESS_KEY_ID="+cloudtest.TestAccessKeyID,
			"AWS_SECRET_ACCESS_KEY="+cloudtest.TestSecretAccessKey,
			"AWS_REGION="+cloudtest.Region,
		)

		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		err := cmd.Run()
		require.NoError(t, err, "stderr: %s", stderr.String())

		lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
		assert.Len(t, lines, 2, "expected 2 objects with data/ prefix")
	})

	t.Run("respects limit flag", func(t *testing.T) {
		bucket := cloudtest.CreateBucket(t, ctx)
		cloudtest.PutObjects(t, ctx, bucket, []string{
			"file1.txt",
			"file2.txt",
			"file3.txt",
			"file4.txt",
			"file5.txt",
		})

		cmd := exec.Command(binary, "inspect",
			"s3://"+bucket+"/",
			"--endpoint", cloudtest.Endpoint,
			"--json",
			"--limit", "2",
		)
		cmd.Env = append(os.Environ(),
			"AWS_ACCESS_KEY_ID="+cloudtest.TestAccessKeyID,
			"AWS_SECRET_ACCESS_KEY="+cloudtest.TestSecretAccessKey,
			"AWS_REGION="+cloudtest.Region,
		)

		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		err := cmd.Run()
		require.NoError(t, err, "stderr: %s", stderr.String())

		lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
		assert.Len(t, lines, 2, "expected 2 objects due to limit")
	})

	t.Run("returns error for non-existent bucket", func(t *testing.T) {
		cmd := exec.Command(binary, "inspect",
			"s3://nonexistent-bucket-12345/",
			"--endpoint", cloudtest.Endpoint,
			"--json",
		)
		cmd.Env = append(os.Environ(),
			"AWS_ACCESS_KEY_ID="+cloudtest.TestAccessKeyID,
			"AWS_SECRET_ACCESS_KEY="+cloudtest.TestSecretAccessKey,
			"AWS_REGION="+cloudtest.Region,
		)

		err := cmd.Run()
		assert.Error(t, err, "expected error for non-existent bucket")
	})
}
