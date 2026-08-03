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

	"github.com/3leaps/gonimbus/internal/ownedproc"
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
			group := newManagedChildGroup(t)
			start := group.launcher(binary, args...)
			start.Env = managedTestEnv(dataRoot)
			out, err := start.CombinedOutput()
			if err != nil {
				t.Fatalf("start background %s: %v\n%s", format, err, out)
			}
			jobID := strings.TrimSpace(string(out))
			if jobID == "" || strings.Contains(jobID, "\n") {
				t.Fatalf("unexpected background output %q", string(out))
			}
			reapManagedJob(t, dataRoot, jobID, group)

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
	group := newManagedChildGroup(t)
	serve := group.launcher(binary, "--config", configPath, "--readonly", "serve", "--host", "127.0.0.1", "--port", fmt.Sprint(port))
	serve.Env = managedTestProviderEnv()
	var serveOutput bytes.Buffer
	serve.Stdout = &serveOutput
	serve.Stderr = &serveOutput
	served, err := ownedproc.Start(serve)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if stopErr := served.Stop(15 * time.Second); stopErr != nil {
			t.Errorf("stop server: %v", stopErr)
		}
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
	reapManagedJob(t, dataRoot, envelope.Job.JobID, group)
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
	group := newManagedChildGroup(t)
	start := group.launcher(binary, "index", "build", "--job", manifestPath, "--background")
	start.Env = managedTestEnv(dataRoot)
	out, err := start.CombinedOutput()
	if err != nil {
		t.Fatalf("enqueue managed failure: %v\n%s", err, out)
	}
	jobID := strings.TrimSpace(string(out))
	reapManagedJob(t, dataRoot, jobID, group)
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

// managedJobIDFlag is the internal flag the executor uses to hand a managed child
// its job id. Spelled out rather than imported: this test pins what is actually
// on the child's command line.
const managedJobIDFlag = "--_managed-job-id"

// afterReapIdentityCheck is a test-only seam between proving a managed child is
// running and signalling it, used to prove the signal cannot be redirected by
// anything that changes in that window.
var afterReapIdentityCheck = func() {}

// reapManagedJob terminates a leaked managed child on every exit path of the
// test, including t.Fatal and a timed-out wait. This is test hygiene for leaked
// test processes; it is not the production lease lifecycle.
func reapManagedJob(t *testing.T, dataRoot, jobID string, group *managedChildGroup) {
	t.Helper()
	t.Cleanup(func() { reapManagedJobNow(t, dataRoot, jobID, group) })
}

// reapManagedJobNow signals the process group this test owns, only while that
// group is still anchored, then confirms death. It reports whether it signalled.
//
// No target is derived from the job record: the record decides only whether the
// job is still meant to be running, and the anchored group is what gets
// signalled. Failures fail the test rather than being logged as success — a reap
// that cannot confirm death has not reaped anything.
func reapManagedJobNow(t *testing.T, dataRoot, jobID string, group *managedChildGroup) bool {
	t.Helper()
	if runtime.GOOS == "windows" {
		// The managed-child integration tests do not run here, and both process
		// groups and command-line inspection would need different mechanisms.
		return false
	}
	if group == nil || group.pgid <= 0 {
		return false
	}
	record, err := readManagedJobRecord(dataRoot, jobID)
	if err != nil {
		return false
	}
	switch record.State {
	case jobregistry.JobStateQueued, jobregistry.JobStateRunning, jobregistry.JobStateStopping:
	default:
		// A terminal record means the child finished; nothing of ours is running.
		return false
	}
	members := managedJobGroupMembers(t, group.pgid, jobID)
	if len(members) == 0 {
		t.Logf("not reaping group %d: no live member names job %s", group.pgid, jobID)
		return false
	}

	afterReapIdentityCheck()

	if !group.alive() {
		// Without the anchor the group id is no longer ours to signal, whatever
		// the enumeration above found a moment ago.
		t.Errorf("refusing to signal group %d: its anchor is gone, so the id is no longer owned", group.pgid)
		return false
	}
	if err := group.signal(); err != nil {
		t.Errorf("kill managed child process group %d: %v", group.pgid, err)
		return false
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(managedJobGroupMembers(t, group.pgid, jobID)) > 0 {
		if time.Now().After(deadline) {
			t.Errorf("managed child of job %s survived the group kill", jobID)
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("reaped leaked managed child pids=%v job=%s group=%d", members, jobID, group.pgid)
	return true
}

// managedJobGroupMembers returns the live processes in pgid whose command line
// carries this job's id as the value of the managed-job flag.
//
// The flag and its value are matched as an adjacent pair rather than as two
// substrings anywhere in the line, so an unrelated command that happens to
// mention both cannot pass as the child.
func managedJobGroupMembers(t *testing.T, pgid int, jobID string) []int {
	t.Helper()
	entries, err := listProcesses()
	if err != nil {
		t.Errorf("list processes: %v", err)
		return nil
	}
	var members []int
	for _, entry := range entries {
		if entry.pgid != pgid {
			continue
		}
		if commandLineNamesJob(entry.args, jobID) {
			members = append(members, entry.pid)
		}
	}
	return members
}

// commandLineNamesJob reports whether args pass the managed-job id as this job's
// id, in either accepted spelling.
func commandLineNamesJob(args []string, jobID string) bool {
	for i, arg := range args {
		if arg == managedJobIDFlag && i+1 < len(args) && args[i+1] == jobID {
			return true
		}
		if arg == managedJobIDFlag+"="+jobID {
			return true
		}
	}
	return false
}

func readManagedJobRecord(dataRoot, jobID string) (jobregistry.JobRecord, error) {
	var record jobregistry.JobRecord
	body, err := os.ReadFile(filepath.Join(dataRoot, "jobs", "index-build", jobID, "job.json")) // #nosec G304 -- test-owned temp path
	if err != nil {
		return record, err
	}
	if err := json.Unmarshal(body, &record); err != nil {
		return record, err
	}
	return record, nil
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
				case jobregistry.JobStateSuccess, jobregistry.JobStatePartial, jobregistry.JobStateFailed, jobregistry.JobStateStopped:
					return record
				// JobStateUnknown is a zombie demotion when a process exits while
				// still marked running. It is not a stable terminal outcome: the
				// child may still write success, or a concurrent re-read may show
				// a terminal state. Keep polling until timeout.
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
