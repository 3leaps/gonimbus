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

func parseInspectJSONLines(t *testing.T, output string) ([]map[string]interface{}, *map[string]interface{}) {
	t.Helper()

	var objects []map[string]interface{}
	var summary *map[string]interface{}
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}

		var record map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		if record["type"] == "gonimbus.inspect.summary.v1" {
			recordCopy := record
			summary = &recordCopy
			continue
		}
		objects = append(objects, record)
	}

	return objects, summary
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

		objects, summary := parseInspectJSONLines(t, stdout.String())
		assert.Len(t, objects, 3, "expected 3 objects")
		assert.Nil(t, summary, "did not expect inspect summary when output is not capped")

		for _, obj := range objects {
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

		objects, summary := parseInspectJSONLines(t, stdout.String())
		assert.Len(t, objects, 2, "expected 2 objects with data/ prefix")
		assert.Nil(t, summary, "did not expect inspect summary when output is not capped")
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

		objects, summary := parseInspectJSONLines(t, stdout.String())
		assert.Len(t, objects, 2, "expected 2 objects due to limit")
		require.NotNil(t, summary, "expected inspect summary when output is capped")
		assert.Equal(t, "gonimbus.inspect.summary.v1", (*summary)["type"])

		data, ok := (*summary)["data"].(map[string]interface{})
		require.True(t, ok, "summary data should be an object")
		assert.Equal(t, float64(2), data["objects_emitted"])
		assert.Equal(t, float64(2), data["limit"])
		assert.Equal(t, true, data["truncated"])
		assert.Equal(t, "limit_reached", data["reason"])
		assert.Equal(t, true, data["may_have_more"])
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
