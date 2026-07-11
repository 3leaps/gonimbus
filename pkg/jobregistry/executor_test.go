package jobregistry

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestManagedChildProcessHelper(t *testing.T) {
	if os.Getenv("GO_WANT_JOB_HELPER") != "1" {
		return
	}
	args := os.Args
	for i, arg := range args {
		if arg == "--" {
			args = args[i+1:]
			break
		}
	}
	b, _ := json.Marshal(args)
	_ = os.WriteFile(os.Getenv("JOB_HELPER_ARGS"), b, 0o600)
	if delay, _ := time.ParseDuration(os.Getenv("JOB_HELPER_DELAY")); delay > 0 {
		time.Sleep(delay)
	}
	jobID := ""
	for i := range args {
		if args[i] == "--_managed-job-id" && i+1 < len(args) {
			jobID = args[i+1]
		}
	}
	if jobID != "" {
		store := NewStore(os.Getenv("JOB_HELPER_ROOT"))
		if rec, err := store.Get(jobID); err == nil {
			now := time.Now().UTC()
			rec.State = JobStateSuccess
			rec.StartedAt = &now
			rec.EndedAt = &now
			rec.PID = os.Getpid()
			_ = store.Write(rec)
		}
	}
	os.Exit(0)
}

func TestIndexBuildBackgroundMetadataIncludesSince(t *testing.T) {
	metadata := indexBuildBackgroundMetadata(BackgroundOptions{
		Since: "auto",
	})

	require.Equal(t, "auto", metadata["since"])
}

func TestIndexBuildBackgroundMetadataOmitsBlankSince(t *testing.T) {
	metadata := indexBuildBackgroundMetadata(BackgroundOptions{
		Since: " ",
	})

	require.Nil(t, metadata)
}

func TestStartIndexBuildBackgroundForwardsExactTypedInvocation(t *testing.T) {
	for _, format := range []string{"sqlite", "durable", "both"} {
		t.Run(format, func(t *testing.T) {
			root := t.TempDir()
			manifestPath := filepath.Join(t.TempDir(), "index.yaml")
			require.NoError(t, os.WriteFile(manifestPath, []byte("version: 1\n"), 0o600))
			capture := filepath.Join(t.TempDir(), "args.json")
			e := NewExecutor(root)
			e.newCommand = helperCommand(t, root, capture, 0)
			rec, err := e.StartIndexBuildBackground(manifestPath, "nightly", BackgroundOptions{
				Invocation: &IndexBuildInvocation{
					SchemaVersion:     IndexBuildInvocationVersion,
					RequestedFormat:   format,
					EffectiveFormat:   format,
					ConfigPath:        "/tmp/gonimbus-test-config.yaml",
					Verbose:           true,
					ReadOnly:          true,
					DBPath:            map[string]string{"sqlite": "custom.db", "both": "custom.db"}[format],
					Since:             "2026-07-01T00:00:00Z",
					StorageProvider:   "generic_s3",
					CloudProvider:     "other",
					RegionKind:        "aws",
					Region:            "us-east-1",
					EndpointHost:      "objects.example.test",
					ScopeWarnPrefixes: 12,
					ScopeMaxPrefixes:  34,
				},
			})
			require.NoError(t, err)
			require.NotNil(t, rec.Invocation)
			require.Equal(t, format, rec.Invocation.EffectiveFormat)
			args := waitHelperArgs(t, capture)
			requireFlagValue(t, args, "--format", format)
			requireFlagValue(t, args, "--config", "/tmp/gonimbus-test-config.yaml")
			require.Contains(t, args, "--verbose")
			require.Contains(t, args, "--readonly")
			requireFlagValue(t, args, "--job", rec.Invocation.ManifestPath)
			requireFlagValue(t, args, "--since", "2026-07-01T00:00:00Z")
			requireFlagValue(t, args, "--endpoint-host", "objects.example.test")
			requireFlagValue(t, args, "--scope-max-prefixes", "34")
			waitHelperCompletion(t, e.Store(), rec)
		})
	}
}

func TestStartIndexBuildBackgroundFastChildCannotBeClobbered(t *testing.T) {
	root := t.TempDir()
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte("version: 1\n"), 0o600))
	e := NewExecutor(root)
	e.newCommand = helperCommand(t, root, filepath.Join(t.TempDir(), "args.json"), 0)
	rec, err := e.StartIndexBuildBackground(manifestPath, "fast", BackgroundOptions{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		stored, getErr := e.Store().Get(rec.JobID)
		return getErr == nil && stored.State == JobStateSuccess
	}, 2*time.Second, 10*time.Millisecond)
	stored, err := e.Store().Get(rec.JobID)
	require.NoError(t, err)
	require.NotNil(t, stored.Invocation)
	require.NotEmpty(t, stored.InvocationFingerprint)
	require.NotZero(t, stored.PID)
	waitHelperCompletion(t, e.Store(), rec)
}

func TestStartIndexBuildBackgroundDedupeIsAtomic(t *testing.T) {
	root := t.TempDir()
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte("version: 1\n"), 0o600))
	e := NewExecutor(root)
	e.newCommand = helperCommand(t, root, filepath.Join(t.TempDir(), "args.json"), 500*time.Millisecond)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := e.StartIndexBuildBackground(manifestPath, "same", BackgroundOptions{Dedupe: true})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	successes, duplicates := 0, 0
	for err := range errs {
		if err == nil {
			successes++
		} else if strings.Contains(err.Error(), "duplicate running job") {
			duplicates++
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, duplicates)
	waitForAllHelperJobs(t, e.Store())
}

func TestStartIndexBuildBackgroundDedupeDistinguishesEffectiveInvocation(t *testing.T) {
	tests := []struct {
		name   string
		first  IndexBuildInvocation
		second IndexBuildInvocation
	}{
		{
			name:   "format",
			first:  IndexBuildInvocation{EffectiveFormat: "sqlite"},
			second: IndexBuildInvocation{EffectiveFormat: "durable"},
		},
		{
			name:   "since",
			first:  IndexBuildInvocation{EffectiveFormat: "sqlite", Since: "auto"},
			second: IndexBuildInvocation{EffectiveFormat: "sqlite", Since: "2026-07-01T00:00:00Z"},
		},
		{
			name:   "identity",
			first:  IndexBuildInvocation{EffectiveFormat: "sqlite", StorageProvider: "aws_s3"},
			second: IndexBuildInvocation{EffectiveFormat: "sqlite", StorageProvider: "generic_s3"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			manifestPath := filepath.Join(t.TempDir(), "index.yaml")
			require.NoError(t, os.WriteFile(manifestPath, []byte("version: 1\n"), 0o600))
			e := NewExecutor(root)
			e.newCommand = helperCommand(t, root, filepath.Join(t.TempDir(), "args.json"), 500*time.Millisecond)
			prepareTestInvocation := func(inv *IndexBuildInvocation) {
				inv.SchemaVersion = IndexBuildInvocationVersion
				inv.RequestedFormat = inv.EffectiveFormat
				inv.ScopeWarnPrefixes = DefaultScopeWarnPrefixes
				inv.ScopeMaxPrefixes = DefaultScopeMaxPrefixes
			}
			prepareTestInvocation(&tc.first)
			prepareTestInvocation(&tc.second)
			first, err := e.StartIndexBuildBackground(manifestPath, "", BackgroundOptions{Dedupe: true, Invocation: &tc.first})
			require.NoError(t, err)
			second, err := e.StartIndexBuildBackground(manifestPath, "", BackgroundOptions{Dedupe: true, Invocation: &tc.second})
			require.NoError(t, err)
			waitHelperCompletion(t, e.Store(), first)
			waitHelperCompletion(t, e.Store(), second)
		})
	}
}

func TestPrepareIndexBuildInvocationRejectsEndpointDisclosure(t *testing.T) {
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte("version: 1\n"), 0o600))
	_, _, err := PrepareIndexBuildInvocation(manifestPath, "", &IndexBuildInvocation{
		SchemaVersion:     IndexBuildInvocationVersion,
		EffectiveFormat:   "durable",
		EndpointHost:      "user:secret@objects.example.test?signature=secret",
		ScopeWarnPrefixes: DefaultScopeWarnPrefixes,
		ScopeMaxPrefixes:  DefaultScopeMaxPrefixes,
	})
	require.ErrorContains(t, err, "without userinfo, path, or query")
}

func TestPrepareIndexBuildInvocationRejectsDBURLDisclosure(t *testing.T) {
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte("version: 1\n"), 0o600))
	_, _, err := PrepareIndexBuildInvocation(manifestPath, "", &IndexBuildInvocation{
		SchemaVersion:     IndexBuildInvocationVersion,
		EffectiveFormat:   "sqlite",
		DBPath:            "libsql://user:secret@example.test/index?token=secret",
		ScopeWarnPrefixes: DefaultScopeWarnPrefixes,
		ScopeMaxPrefixes:  DefaultScopeMaxPrefixes,
	})
	require.ErrorContains(t, err, "must not contain userinfo, query, or fragment")
}

func TestEffectiveInvocationFingerprintNormalizesAliasProvenance(t *testing.T) {
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte("version: 1\n"), 0o600))
	base := &IndexBuildInvocation{
		SchemaVersion:     IndexBuildInvocationVersion,
		RequestedFormat:   "durable",
		EffectiveFormat:   "durable",
		ScopeWarnPrefixes: DefaultScopeWarnPrefixes,
		ScopeMaxPrefixes:  DefaultScopeMaxPrefixes,
	}
	_, explicitFingerprint, err := PrepareIndexBuildInvocation(manifestPath, "", base)
	require.NoError(t, err)
	base.RequestedFormat = "experimental-engine"
	_, aliasFingerprint, err := PrepareIndexBuildInvocation(manifestPath, "", base)
	require.NoError(t, err)
	require.Equal(t, explicitFingerprint, aliasFingerprint)
}

func TestStartIndexBuildBackgroundRejectsCredentialMetadata(t *testing.T) {
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte("version: 1\n"), 0o600))
	_, err := NewExecutor(t.TempDir()).StartIndexBuildBackground(manifestPath, "", BackgroundOptions{
		Metadata: map[string]string{"label": "https://objects.example.test/key?X-Amz-Signature=sentinel-secret"},
	})
	require.ErrorContains(t, err, "metadata is not supported")
}

func TestStartIndexBuildBackgroundEmptyRootDoesNotMutateCWD(t *testing.T) {
	cwd := t.TempDir()
	t.Chdir(cwd)
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte("version: 1\n"), 0o600))
	_, err := NewExecutor("").StartIndexBuildBackground(manifestPath, "", BackgroundOptions{})
	require.ErrorContains(t, err, "root dir is empty")
	entries, readErr := os.ReadDir(cwd)
	require.NoError(t, readErr)
	require.Empty(t, entries)
}

func TestStartIndexBuildBackgroundRejectsSignedMaterialBeforeArtifacts(t *testing.T) {
	const sentinel = "https://objects.example.test/key?X-Amz-Signature=sentinel-secret"
	baseManifest := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(baseManifest, []byte("version: 1\n"), 0o600))
	signedManifest := filepath.Join(t.TempDir(), "X-Amz-Signature=sentinel-secret")
	require.NoError(t, os.WriteFile(signedManifest, []byte("version: 1\n"), 0o600))
	tests := map[string]func(*IndexBuildInvocation) (string, string){
		"manifest_path":    func(*IndexBuildInvocation) (string, string) { return signedManifest, "" },
		"requested_format": func(v *IndexBuildInvocation) (string, string) { v.RequestedFormat = sentinel; return baseManifest, "" },
		"effective_format": func(v *IndexBuildInvocation) (string, string) { v.EffectiveFormat = sentinel; return baseManifest, "" },
		"config_path":      func(v *IndexBuildInvocation) (string, string) { v.ConfigPath = sentinel; return baseManifest, "" },
		"data_root":        func(v *IndexBuildInvocation) (string, string) { v.DataRoot = sentinel; return baseManifest, "" },
		"db_path":          func(v *IndexBuildInvocation) (string, string) { v.DBPath = sentinel; return baseManifest, "" },
		"since":            func(v *IndexBuildInvocation) (string, string) { v.Since = sentinel; return baseManifest, "" },
		"name":             func(*IndexBuildInvocation) (string, string) { return baseManifest, sentinel },
		"storage_provider": func(v *IndexBuildInvocation) (string, string) { v.StorageProvider = sentinel; return baseManifest, "" },
		"cloud_provider":   func(v *IndexBuildInvocation) (string, string) { v.CloudProvider = sentinel; return baseManifest, "" },
		"region_kind":      func(v *IndexBuildInvocation) (string, string) { v.RegionKind = sentinel; return baseManifest, "" },
		"region":           func(v *IndexBuildInvocation) (string, string) { v.Region = sentinel; return baseManifest, "" },
		"endpoint_host":    func(v *IndexBuildInvocation) (string, string) { v.EndpointHost = sentinel; return baseManifest, "" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			inv := IndexBuildInvocation{
				SchemaVersion: IndexBuildInvocationVersion, RequestedFormat: "durable", EffectiveFormat: "durable",
				ScopeWarnPrefixes: DefaultScopeWarnPrefixes, ScopeMaxPrefixes: DefaultScopeMaxPrefixes,
			}
			manifestPath, jobName := mutate(&inv)
			_, err := NewExecutor(root).StartIndexBuildBackground(manifestPath, jobName, BackgroundOptions{Invocation: &inv})
			require.Error(t, err)
			entries, readErr := os.ReadDir(root)
			require.NoError(t, readErr)
			require.Empty(t, entries, "rejected value must not create argv/job/log artifacts")
		})
	}
}

func TestStartIndexBuildBackgroundRejectsEncodedSignedMaterialBeforeArtifacts(t *testing.T) {
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte("version: 1\n"), 0o600))
	values := []string{
		"https://host/key?X-Amz-%53ignature=sentinel",
		"https://host/key?X-Goog-%53ignature=sentinel",
		"https://host/key?to%6ben=sentinel",
		"https://user%3Asecret@host/key",
		"https://host/key#X-Amz-%53ignature=sentinel",
	}
	for _, value := range values {
		t.Run(value, func(t *testing.T) {
			root := t.TempDir()
			inv := IndexBuildInvocation{
				SchemaVersion: IndexBuildInvocationVersion, RequestedFormat: "durable", EffectiveFormat: "durable",
				StorageProvider: value, ScopeWarnPrefixes: DefaultScopeWarnPrefixes, ScopeMaxPrefixes: DefaultScopeMaxPrefixes,
			}
			_, err := NewExecutor(root).StartIndexBuildBackground(manifestPath, "", BackgroundOptions{Invocation: &inv})
			require.Error(t, err)
			entries, readErr := os.ReadDir(root)
			require.NoError(t, readErr)
			require.Empty(t, entries)
		})
	}
}

func TestStartIndexBuildBackgroundRecoversExpiredQueuedAfterCrash(t *testing.T) {
	root := t.TempDir()
	manifestPath := filepath.Join(t.TempDir(), "index.yaml")
	require.NoError(t, os.WriteFile(manifestPath, []byte("version: 1\n"), 0o600))
	e := NewExecutor(root)
	e.afterQueued = func(*JobRecord) error { return errors.New("simulated parent crash") }
	_, err := e.StartIndexBuildBackground(manifestPath, "same", BackgroundOptions{Dedupe: true})
	require.ErrorContains(t, err, "simulated parent crash")
	jobs, err := e.Store().List()
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	expired := time.Now().UTC().Add(-time.Second)
	jobs[0].EnqueueExpiresAt = &expired
	require.NoError(t, e.Store().Write(&jobs[0]))

	e.afterQueued = nil
	e.newCommand = helperCommand(t, root, filepath.Join(t.TempDir(), "args.json"), 0)
	rec, err := e.StartIndexBuildBackground(manifestPath, "same", BackgroundOptions{Dedupe: true})
	require.NoError(t, err)
	require.NotEqual(t, jobs[0].JobID, rec.JobID)
	stale, err := e.Store().Get(jobs[0].JobID)
	require.NoError(t, err)
	require.Equal(t, JobStateFailed, stale.State)
	require.NotNil(t, stale.EndedAt)
	waitHelperCompletion(t, e.Store(), rec)
}

func helperCommand(t *testing.T, root, capture string, delay time.Duration) func(string, ...string) *exec.Cmd {
	t.Helper()
	return func(_ string, args ...string) *exec.Cmd {
		cmdArgs := []string{"-test.run=TestManagedChildProcessHelper", "--"}
		cmdArgs = append(cmdArgs, args...)
		cmd := exec.Command(os.Args[0], cmdArgs...)
		cmd.Env = append(os.Environ(),
			"GO_WANT_JOB_HELPER=1",
			"JOB_HELPER_ROOT="+root,
			"JOB_HELPER_ARGS="+capture,
			"JOB_HELPER_DELAY="+delay.String(),
		)
		return cmd
	}
}

func waitHelperArgs(t *testing.T, path string) []string {
	t.Helper()
	var args []string
	require.Eventually(t, func() bool {
		b, err := os.ReadFile(path)
		return err == nil && json.Unmarshal(b, &args) == nil
	}, 2*time.Second, 10*time.Millisecond)
	return args
}

func waitHelperCompletion(t *testing.T, store *Store, rec *JobRecord) {
	t.Helper()
	require.Eventually(t, func() bool {
		stored, err := store.Get(rec.JobID)
		return err == nil && stored.State == JobStateSuccess && !isProcessAlive(rec.PID)
	}, 3*time.Second, 10*time.Millisecond)
}

func waitForAllHelperJobs(t *testing.T, store *Store) {
	t.Helper()
	require.Eventually(t, func() bool {
		jobs, err := store.List()
		if err != nil || len(jobs) == 0 {
			return false
		}
		for i := range jobs {
			if jobs[i].State != JobStateSuccess || isProcessAlive(jobs[i].PID) {
				return false
			}
		}
		return true
	}, 3*time.Second, 10*time.Millisecond)
}

func requireFlagValue(t *testing.T, args []string, flag, want string) {
	t.Helper()
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag {
			require.Equal(t, want, args[i+1])
			return
		}
	}
	t.Fatalf("flag %s not found in %v", flag, args)
}
