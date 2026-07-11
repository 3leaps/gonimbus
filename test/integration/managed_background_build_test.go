package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

func TestManagedBackgroundBuildRealChildAllFormats(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("real managed-child process regression is unix-focused")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for fake S3: %v", err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name><Prefix>data/</Prefix><IsTruncated>false</IsTruncated>
  <Contents><Key>data/object.txt</Key><LastModified>2026-07-11T12:00:00Z</LastModified><ETag>"etag-1"</ETag><Size>7</Size><StorageClass>STANDARD</StorageClass></Contents>
</ListBucketResult>`)
	}))
	server.Listener = listener
	server.Start()
	defer server.Close()

	binary := buildManagedTestBinary(t)
	endpoint, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := net.SplitHostPort(endpoint.Host); err != nil {
		t.Fatalf("test endpoint host: %v", err)
	}

	for _, format := range []string{"sqlite", "durable", "both"} {
		t.Run(format, func(t *testing.T) {
			dataRoot := filepath.Join(t.TempDir(), "data")
			manifestPath := filepath.Join(t.TempDir(), "index.yaml")
			manifest := fmt.Sprintf(`version: "1.0"
connection:
  provider: s3
  bucket: test-bucket
  base_uri: s3://test-bucket/data/
  region: us-east-1
  endpoint: %s
identity:
  storage_provider: generic_s3
  cloud_provider: other
  region_kind: aws
  region: us-east-1
  endpoint_host: %s
build:
  source: crawl
  crawl:
    concurrency: 1
`, server.URL, endpoint.Host)
			if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
				t.Fatal(err)
			}
			args := []string{"index", "build", "--job", manifestPath, "--format", format, "--background", "--dedupe", "--name", "real-child-" + format, "--scope-warn-prefixes", "7", "--scope-max-prefixes", "9"}
			start := exec.Command(binary, args...)
			start.Env = managedTestEnv(dataRoot)
			out, err := start.CombinedOutput()
			if err != nil {
				t.Fatalf("start background %s: %v\n%s", format, err, out)
			}
			jobID := strings.TrimSpace(string(out))
			if jobID == "" || strings.Contains(jobID, "\n") {
				t.Fatalf("unexpected background output %q", string(out))
			}

			record := waitForManagedJob(t, binary, dataRoot, jobID)
			if record.State != jobregistry.JobStateSuccess {
				stderrPath := filepath.Join(dataRoot, "jobs", "index-build", jobID, "stderr.log")
				stderr, _ := os.ReadFile(stderrPath)
				t.Fatalf("managed %s state=%s\n%s", format, record.State, stderr)
			}
			if record.Invocation == nil || record.Receipt == nil {
				t.Fatalf("managed %s missing invocation or terminal receipt: %+v", format, record)
			}
			if record.Invocation.EffectiveFormat != format || record.Invocation.Name != "real-child-"+format {
				t.Fatalf("managed %s invocation drift: %+v", format, record.Invocation)
			}
			if record.Invocation.ScopeWarnPrefixes != 7 || record.Invocation.ScopeMaxPrefixes != 9 {
				t.Fatalf("managed %s scope limits drift: %+v", format, record.Invocation)
			}
			if record.Receipt.RequestedFormat != format || record.Receipt.IndexSetID == "" || record.Receipt.RunID == "" {
				t.Fatalf("managed %s terminal identity incomplete: %+v", format, record.Receipt)
			}
		})
	}
}

func TestManagedBackgroundBuildAPIForwardsServerRuntime(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("real managed-child process regression is unix-focused")
	}
	fakeS3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>test-bucket</Name><Prefix>data/</Prefix><IsTruncated>false</IsTruncated></ListBucketResult>`)
	}))
	defer fakeS3.Close()

	binary := buildManagedTestBinary(t)
	dataRoot := filepath.Join(t.TempDir(), "non-default-data")
	configPath := filepath.Join(t.TempDir(), "gonimbus.yaml")
	metricsPort := reserveLoopbackPort(t)
	config := fmt.Sprintf("data_root: %s\nmetrics:\n  port: %d\n", dataRoot, metricsPort)
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	manifest := fmt.Sprintf(`version: "1.0"
connection:
  provider: s3
  bucket: test-bucket
  base_uri: s3://test-bucket/data/
  region: us-east-1
  endpoint: %s
identity:
  storage_provider: generic_s3
  endpoint_host: %s
build:
  source: crawl
`, fakeS3.URL, strings.TrimPrefix(fakeS3.URL, "http://"))
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}

	port := reserveLoopbackPort(t)
	serve := exec.Command(binary, "--config", configPath, "--readonly", "serve", "--host", "127.0.0.1", "--port", fmt.Sprint(port))
	serve.Env = managedTestProviderEnv()
	var serveOutput bytes.Buffer
	serve.Stdout = &serveOutput
	serve.Stderr = &serveOutput
	if err := serve.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = serve.Process.Kill()
		_ = serve.Wait()
	})
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	requireHTTPReady(t, baseURL+"/health", &serveOutput)

	body, _ := json.Marshal(map[string]any{
		"type": "index.build", "manifest_path": manifestPath, "name": "api-runtime",
	})
	resp, err := http.Post(baseURL+"/api/v1/jobs", "application/json", bytes.NewReader(body)) // #nosec G107 -- loopback integration server.
	if err != nil {
		t.Fatalf("submit API job: %v\n%s", err, serveOutput.String())
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("submit status=%d\n%s", resp.StatusCode, serveOutput.String())
	}
	var envelope struct {
		Job jobregistry.JobRecord `json:"job"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	record := waitForManagedJob(t, binary, dataRoot, envelope.Job.JobID)
	if record.State != jobregistry.JobStateSuccess {
		t.Fatalf("API managed state=%s\n%s", record.State, serveOutput.String())
	}
	resolvedDataRoot, err := filepath.EvalSymlinks(dataRoot)
	if err != nil {
		t.Fatal(err)
	}
	if record.Invocation == nil || record.Invocation.ConfigPath != configPath || record.Invocation.DataRoot != resolvedDataRoot || !record.Invocation.ReadOnly {
		t.Fatalf("server runtime invocation drift: %+v", record.Invocation)
	}
}

func TestManagedBackgroundBuildFailureDoesNotPersistSignedMaterial(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("real managed-child process regression is unix-focused")
	}
	const sentinel = "managed-signed-material-sentinel"
	binary := buildManagedTestBinary(t)
	dataRoot := filepath.Join(t.TempDir(), "data")
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	manifest := fmt.Sprintf(`version: "1.0"
connection:
  provider: s3
  bucket: test-bucket
  base_uri: s3://test-bucket/data/
  region: us-east-1
  endpoint: https://user:%s@127.0.0.1:1/path?X-Amz-Signature=%s
identity:
  storage_provider: generic_s3
  endpoint_host: 127.0.0.1:1
build:
  source: crawl
`, sentinel, sentinel)
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	start := exec.Command(binary, "index", "build", "--job", manifestPath, "--background")
	start.Env = managedTestEnv(dataRoot)
	out, err := start.CombinedOutput()
	if err != nil {
		t.Fatalf("enqueue managed failure: %v\n%s", err, out)
	}
	jobID := strings.TrimSpace(string(out))
	record := waitForManagedJob(t, binary, dataRoot, jobID)
	if record.State != jobregistry.JobStateFailed {
		t.Fatalf("failure fixture state=%s", record.State)
	}
	jobDir := filepath.Join(dataRoot, "jobs", "index-build", jobID)
	var persisted bytes.Buffer
	for _, name := range []string{"job.json", "stdout.log", "stderr.log"} {
		content, readErr := os.ReadFile(filepath.Join(jobDir, name))
		if readErr != nil {
			t.Fatal(readErr)
		}
		persisted.Write(content)
	}
	if strings.Contains(persisted.String(), sentinel) {
		t.Fatalf("signed material persisted in managed artifacts: %s", persisted.String())
	}
}

func reserveLoopbackPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	return port
}

func requireHTTPReady(t *testing.T, target string, output *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(target) // #nosec G107 -- loopback integration server.
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server did not become ready\n%s", output.String())
}

func buildManagedTestBinary(t *testing.T) string {
	t.Helper()
	goModPath, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		t.Fatalf("go env GOMOD: %v", err)
	}
	repoRoot := filepath.Dir(strings.TrimSpace(string(goModPath)))
	binary := filepath.Join(t.TempDir(), "gonimbus")
	build := exec.Command("go", "build", "-o", binary, "./cmd/gonimbus")
	build.Dir = repoRoot
	build.Env = os.Environ()
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return binary
}

func managedTestEnv(dataRoot string) []string {
	return append(managedTestProviderEnv(), "GONIMBUS_DATA_DIR="+dataRoot)
}

func managedTestProviderEnv() []string {
	env := make([]string, 0, len(os.Environ())+4)
	for _, item := range os.Environ() {
		if strings.HasPrefix(item, "GONIMBUS_DATA_DIR=") || strings.HasPrefix(item, "GONIMBUS_DATA_ROOT=") {
			continue
		}
		env = append(env, item)
	}
	return append(env,
		"AWS_ACCESS_KEY_ID=test-access-key",
		"AWS_SECRET_ACCESS_KEY=test-secret-key",
		"AWS_EC2_METADATA_DISABLED=true",
		"AWS_REGION=us-east-1",
	)
}

func waitForManagedJob(t *testing.T, binary, dataRoot, jobID string) jobregistry.JobRecord {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		status := exec.Command(binary, "index", "jobs", "status", jobID, "--json")
		status.Env = managedTestEnv(dataRoot)
		out, err := status.Output()
		if err == nil {
			var record jobregistry.JobRecord
			if json.Unmarshal(out, &record) == nil {
				switch record.State {
				case jobregistry.JobStateSuccess, jobregistry.JobStatePartial, jobregistry.JobStateFailed, jobregistry.JobStateStopped, jobregistry.JobStateUnknown:
					return record
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	jobDir := filepath.Join(dataRoot, "jobs", "index-build", jobID)
	recordBytes, _ := os.ReadFile(filepath.Join(jobDir, "job.json"))
	stdoutBytes, _ := os.ReadFile(filepath.Join(jobDir, "stdout.log"))
	stderrBytes, _ := os.ReadFile(filepath.Join(jobDir, "stderr.log"))
	t.Fatalf("timed out waiting for managed job %s\nrecord=%s\nstdout=%s\nstderr=%s", jobID, recordBytes, stdoutBytes, stderrBytes)
	return jobregistry.JobRecord{}
}
