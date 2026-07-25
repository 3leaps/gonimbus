package integration

import (
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/internal/ownedproc"
)

// setAuthorityDirName mirrors the package-owned authority directory name. It is
// spelled out rather than imported so this test pins the on-disk layout an
// operator inspects, not whatever an internal constant happens to hold.
const setAuthorityDirName = ".gonimbus-set-authority"

// TestForegroundBuildCleansSetAuthorityOnCompletion and its interrupt sibling are
// the end-to-end form of the set-authority residue guard: a real gonimbus
// process, its own data root, and the artifact an operator would find afterwards.
// The library-level guard proves the runner's outcomes; these prove the command
// as shipped.
func TestForegroundBuildCleansSetAuthorityOnCompletion(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("real foreground-process regression is unix-focused")
	}
	binary := buildManagedTestBinary(t)
	dataRoot := filepath.Join(t.TempDir(), "data")
	server := fakeListServer(t, 0)
	manifestPath := writeFakeS3Manifest(t, server.URL)

	build := exec.Command(binary, "index", "build", "--job", manifestPath, "--name", "foreground-complete")
	build.Env = managedTestEnv(dataRoot)
	out, err := build.CombinedOutput()
	if err != nil {
		t.Fatalf("foreground build: %v\n%s", err, out)
	}

	requireNoSetAuthorityResidue(t, dataRoot)
}

// TestForegroundBuildInterruptedLeavesNoSetAuthorityResidue interrupts a build
// that is genuinely mid-crawl and holding its authority, then asserts the
// artifact is gone. Cleanup here rides on the interrupt being translated into
// cancellation so the process unwinds through its deferred release.
//
// The untrappable case is deliberately not covered: a SIGKILLed build leaves its
// artifact behind, which is what lease detection and reclaim exist to collect.
func TestForegroundBuildInterruptedLeavesNoSetAuthorityResidue(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signalling another process is not portable on windows")
	}
	binary := buildManagedTestBinary(t)
	dataRoot := filepath.Join(t.TempDir(), "data")
	// A slow LIST keeps the build in-flight, and therefore holding its authority,
	// long enough for the interrupt to land mid-run rather than after completion.
	server := fakeListServer(t, 5*time.Second)
	manifestPath := writeFakeS3Manifest(t, server.URL)

	cmd := exec.Command(binary, "index", "build", "--job", manifestPath, "--name", "foreground-interrupt") // #nosec G204 -- test-built binary
	cmd.Env = managedTestEnv(dataRoot)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	build, err := ownedproc.Start(cmd)
	if err != nil {
		t.Fatal(err)
	}
	// One goroutine owns this child's wait for its whole life; teardown signals
	// the retained process and joins that owner rather than waiting itself.
	t.Cleanup(func() {
		if stopErr := build.Stop(30 * time.Second); stopErr != nil {
			t.Errorf("stop foreground build: %v", stopErr)
		}
	})

	// Interrupt only once the artifact is really on disk: otherwise a green result
	// could mean "nothing was ever acquired" rather than "cleanup ran".
	lockPath := waitForSetAuthorityLock(t, dataRoot)

	if err := build.Signal(syscall.SIGINT); err != nil {
		t.Fatalf("interrupt build: %v", err)
	}
	if _, exited := build.AwaitExit(30 * time.Second); !exited {
		t.Fatal("interrupted build did not exit")
	}

	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("interrupted build left its set-authority artifact at %s (stat err: %v)", lockPath, err)
	}
	requireNoSetAuthorityResidue(t, dataRoot)
}

// fakeBarrierListServer serves a static empty S3 LIST response, but only once the
// test releases it. The returned channel is closed when a request first arrives,
// so a test can tell that the client is actually parked in the call.
//
// Releasing lets the build finish and the process exit on its own, which is how a
// test drives a managed worker to exit without signalling a process id.
func fakeBarrierListServer(t *testing.T) (server *httptest.Server, entered <-chan struct{}, release func()) {
	t.Helper()
	enteredCh := make(chan struct{})
	releaseCh := make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		enterOnce.Do(func() { close(enteredCh) })
		select {
		case <-releaseCh:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name><Prefix>data/</Prefix><IsTruncated>false</IsTruncated>
</ListBucketResult>`)
	}))
	t.Cleanup(srv.Close)
	return srv, enteredCh, func() { releaseOnce.Do(func() { close(releaseCh) }) }
}

// fakeListServer serves one static S3 LIST response, optionally after a delay.
func fakeListServer(t *testing.T, delay time.Duration) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>test-bucket</Name><Prefix>data/</Prefix><IsTruncated>false</IsTruncated>
  <Contents><Key>data/object.txt</Key><LastModified>2026-07-11T12:00:00Z</LastModified><ETag>"etag-1"</ETag><Size>7</Size><StorageClass>STANDARD</StorageClass></Contents>
</ListBucketResult>`)
	}))
	t.Cleanup(server.Close)
	return server
}

func writeFakeS3Manifest(t *testing.T, endpoint string) string {
	t.Helper()
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
`, endpoint, strings.TrimPrefix(endpoint, "http://"))
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	return manifestPath
}

// waitForSetAuthorityLock blocks until a set-authority lock file exists anywhere
// under dataRoot and returns its path. It searches rather than reconstructing the
// layout so it stays honest about where the running binary actually put it.
func waitForSetAuthorityLock(t *testing.T, dataRoot string) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if locks := findSetAuthorityArtifacts(t, dataRoot); len(locks) > 0 {
			return locks[0]
		}
		if time.Now().After(deadline) {
			t.Fatal("build never acquired a set authority; an interrupt test that never saw one proves nothing")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func findSetAuthorityArtifacts(t *testing.T, dataRoot string) []string {
	t.Helper()
	var found []string
	err := filepath.WalkDir(dataRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			// The tree is being written concurrently by the child; a vanished entry
			// is not a test failure.
			if os.IsNotExist(walkErr) {
				return nil
			}
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if filepath.Base(filepath.Dir(path)) == setAuthorityDirName {
			found = append(found, path)
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("walk data root: %v", err)
	}
	return found
}

func requireNoSetAuthorityResidue(t *testing.T, dataRoot string) {
	t.Helper()
	if residue := findSetAuthorityArtifacts(t, dataRoot); len(residue) > 0 {
		t.Fatalf("set-authority residue survived the run: %v", residue)
	}
}
