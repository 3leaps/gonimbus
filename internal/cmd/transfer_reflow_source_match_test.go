package cmd

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/provider"
	providerfile "github.com/3leaps/gonimbus/pkg/provider/file"
	"github.com/3leaps/gonimbus/pkg/provider/gcs"
	"github.com/3leaps/gonimbus/pkg/provider/s3"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/fulmenhq/gofulmen/foundry"
	"github.com/stretchr/testify/require"
)

type cmdNonRevisionProvider struct {
	inner    *reflowMemoryProvider
	getCalls atomic.Int32
}

func (p *cmdNonRevisionProvider) List(ctx context.Context, opts provider.ListOptions) (*provider.ListResult, error) {
	return p.inner.List(ctx, opts)
}
func (p *cmdNonRevisionProvider) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	return p.inner.Head(ctx, key)
}
func (p *cmdNonRevisionProvider) GetObject(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	p.getCalls.Add(1)
	return p.inner.GetObject(ctx, key)
}
func (p *cmdNonRevisionProvider) Close() error { return p.inner.Close() }

var (
	_ provider.Provider     = (*cmdNonRevisionProvider)(nil)
	_ provider.ObjectGetter = (*cmdNonRevisionProvider)(nil)
)

func runSourceMatchReflowCmd(t *testing.T, in *strings.Reader, args ...string) (string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	cmd := newTransferReflowTestCommand()
	cmd.SetIn(in)
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs(args)
	execErr := cmd.ExecuteContext(context.Background())
	return stdout.String(), execErr
}

func parseTypedReflowRecords(t *testing.T, stdout string) []reflowpkg.Record {
	t.Helper()
	var out []reflowpkg.Record
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var env struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal([]byte(line), &env); err != nil {
			continue
		}
		if env.Type != reflowpkg.RecordType {
			continue
		}
		var rec reflowpkg.Record
		require.NoError(t, json.Unmarshal(env.Data, &rec))
		out = append(out, rec)
	}
	return out
}

type fakeS3Entry struct {
	body      []byte
	etag      string
	versionID string
	modTime   time.Time
}

type fakeS3HTTPServer struct {
	mu                sync.Mutex
	objects           map[string]fakeS3Entry
	versions          map[string]map[string]fakeS3Entry
	unconditionalGets int
	conditionalGets   int
	versionedGets     int
	headCalls         int
	onHead            func(key string, call int)
	server            *httptest.Server
}

func newFakeS3HTTPServer(t *testing.T) *fakeS3HTTPServer {
	t.Helper()
	fs := &fakeS3HTTPServer{
		objects:  map[string]fakeS3Entry{},
		versions: map[string]map[string]fakeS3Entry{},
	}
	fs.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		trimmed := strings.TrimPrefix(r.URL.Path, "/")
		parts := strings.SplitN(trimmed, "/", 2)
		if len(parts) != 2 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		key := parts[1]

		switch r.Method {
		case http.MethodHead:
			versionID := r.URL.Query().Get("versionId")
			fs.mu.Lock()
			fs.headCalls++
			call := fs.headCalls
			hook := fs.onHead
			var entry fakeS3Entry
			var ok bool
			if versionID != "" {
				entry, ok = fs.versions[key][versionID]
			} else {
				entry, ok = fs.objects[key]
			}
			fs.mu.Unlock()
			if hook != nil {
				hook(key, call)
			}
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("ETag", `"`+entry.etag+`"`)
			if entry.versionID != "" {
				w.Header().Set("x-amz-version-id", entry.versionID)
				w.Header().Set("x-amz-meta-version", entry.versionID)
			}
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(entry.body)))
			w.Header().Set("Last-Modified", entry.modTime.UTC().Format(http.TimeFormat))
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("x-amz-meta-owner", "team")
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			ifMatch := r.Header.Get("If-Match")
			versionID := r.URL.Query().Get("versionId")
			fs.mu.Lock()
			var entry fakeS3Entry
			var ok bool
			if versionID != "" {
				fs.versionedGets++
				entry, ok = fs.versions[key][versionID]
			} else {
				if ifMatch == "" {
					fs.unconditionalGets++
				} else {
					fs.conditionalGets++
				}
				entry, ok = fs.objects[key]
			}
			fs.mu.Unlock()
			if !ok {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusNotFound)
				if versionID != "" {
					_, _ = w.Write([]byte(`<Error><Code>NoSuchVersion</Code><Message>The specified version does not exist.</Message></Error>`))
				} else {
					_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`))
				}
				return
			}
			if ifMatch != "" && strings.Trim(ifMatch, `"`) != entry.etag {
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusPreconditionFailed)
				_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code><Message>At least one of the pre-conditions you specified did not hold</Message></Error>`))
				return
			}
			w.Header().Set("ETag", `"`+entry.etag+`"`)
			if entry.versionID != "" {
				w.Header().Set("x-amz-version-id", entry.versionID)
			}
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(entry.body)))
			w.Header().Set("Last-Modified", entry.modTime.UTC().Format(http.TimeFormat))
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(entry.body)

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(fs.server.Close)
	return fs
}

func (fs *fakeS3HTTPServer) put(key, body, etag, versionID string, modTime time.Time) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	entry := fakeS3Entry{body: []byte(body), etag: etag, versionID: versionID, modTime: modTime}
	fs.objects[key] = entry
	if versionID != "" {
		if fs.versions[key] == nil {
			fs.versions[key] = map[string]fakeS3Entry{}
		}
		fs.versions[key][versionID] = entry
	}
}

func (fs *fakeS3HTTPServer) del(key string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	delete(fs.objects, key)
}

func (fs *fakeS3HTTPServer) counts() (condGets, uncondGets, heads int) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.conditionalGets, fs.unconditionalGets, fs.headCalls
}

func TestTransferReflowRequireSourceMatchS3HTTPTESTDualPath(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	paths := []struct {
		name     string
		extra    []string
		wantPath string
	}{
		{name: "engine", wantPath: reflowpkg.ExecutionPathEngine},
		{name: "cli-pool", extra: poolRoute, wantPath: reflowpkg.ExecutionPathCLIPool},
	}

	for _, tc := range paths {
		t.Run(tc.name, func(t *testing.T) {
			withTransferReflowTestState(t)
			s3Srv := newFakeS3HTTPServer(t)
			s3Srv.put("a/unchanged.bin", "AAAAA", "d41d8cd98f00b204e9800998ecf8427e-3", "ver-1", now)
			s3Srv.put("a/replaced.bin", "BBBBB", "opaque-kms-etag-v2", "ver-2", now)
			s3Srv.put("a/missing-token.bin", "CCCCC", "etag-c", "", now)

			srcProv, err := s3.New(context.Background(), s3.Config{
				Bucket:         "source-bucket",
				Region:         "us-east-1",
				Endpoint:       s3Srv.server.URL,
				ForcePathStyle: true,
				Anonymous:      true,
			})
			require.NoError(t, err)
			dstProv := newReflowMemoryProvider()
			checkpointPath := filepath.Join(t.TempDir(), "state.db")

			useTransferReflowProviderFactories(t, providerdispatch.Factories{
				S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
					switch cfg.Bucket {
					case "source-bucket":
						return srcProv, nil
					case "dest-bucket":
						return dstProv, nil
					default:
						return nil, fmt.Errorf("unexpected bucket %q", cfg.Bucket)
					}
				},
			})

			lines := strings.Join([]string{
				reflowInputLine("a/unchanged.bin", "d41d8cd98f00b204e9800998ecf8427e-3", 5, "", ""),
				reflowInputLine("a/replaced.bin", "etag-v1", 5, "", ""),
				reflowInputLine("a/deleted.bin", "etag-gone", 5, "", ""),
				reflowInputLine("a/missing-token.bin", "", 5, "", ""),
			}, "\n") + "\n"

			args := []string{
				"--stdin",
				"--dest", "s3://dest-bucket/data/",
				"--parallel", "1",
				"--checkpoint", checkpointPath,
				"--require-source-match",
			}
			args = append(args, tc.extra...)
			stdout, err := runSourceMatchReflowCmd(t, strings.NewReader(lines), args...)
			require.Error(t, err, "run with failed items must return error")

			runRec := requireRecord(t, stdout, reflowpkg.RunRecordType, "")
			require.Contains(t, string(runRec.Data), fmt.Sprintf(`"execution_path":%q`, tc.wantPath))

			records := parseTypedReflowRecords(t, stdout)
			byKey := map[string]reflowpkg.Record{}
			for _, rec := range records {
				if rec.Status != "in_progress" {
					byKey[rec.SourceKey] = rec
				}
			}

			// 1. Unchanged multipart ETag copy succeeds with read-bound source_etag & source_match.
			okRec := byKey["a/unchanged.bin"]
			require.Equal(t, "complete", okRec.Status)
			require.Equal(t, "d41d8cd98f00b204e9800998ecf8427e-3", okRec.SourceETag)
			require.Equal(t, "ver-1", okRec.SourceVersionID)
			require.NotNil(t, okRec.SourceMatch)
			require.Equal(t, "d41d8cd98f00b204e9800998ecf8427e-3", okRec.SourceMatch.ExpectedETag)
			require.Equal(t, "d41d8cd98f00b204e9800998ecf8427e-3", okRec.SourceMatch.ObservedETag)
			require.Equal(t, "ver-1", okRec.SourceMatch.ObservedVersionID)
			require.Equal(t, "AAAAA", string(dstProv.mustObject("data/a/unchanged.bin")))

			// 2. Same-size replacement returns failed/source_changed with observed_etag via follow-up HEAD.
			repRec := byKey["a/replaced.bin"]
			require.Equal(t, "failed", repRec.Status)
			require.Equal(t, "source_changed", repRec.Reason)
			require.NotNil(t, repRec.SourceMatch)
			require.Equal(t, "etag-v1", repRec.SourceMatch.ExpectedETag)
			require.Equal(t, "opaque-kms-etag-v2", repRec.SourceMatch.ObservedETag)
			require.Equal(t, "ver-2", repRec.SourceMatch.ObservedVersionID)
			require.False(t, dstProv.hasObject("data/a/replaced.bin"), "changed source must never land at destination")

			// 3. Deleted source returns failed/not_found (NOT source_changed).
			delRec := byKey["a/deleted.bin"]
			require.Equal(t, "failed", delRec.Status)
			require.Equal(t, "not_found", delRec.Reason)
			require.Nil(t, delRec.SourceMatch)
			require.False(t, dstProv.hasObject("data/a/deleted.bin"))

			// 4. Missing source_etag returns failed/source_token_missing.
			missRec := byKey["a/missing-token.bin"]
			require.Equal(t, "failed", missRec.Status)
			require.Equal(t, "source_token_missing", missRec.Reason)
			require.False(t, dstProv.hasObject("data/a/missing-token.bin"))

			// Zero unconditional GET fallback calls across the entire run.
			condGets, uncondGets, _ := s3Srv.counts()
			require.GreaterOrEqual(t, condGets, 3)
			require.Equal(t, 0, uncondGets, "pinned run must never issue an unconditional GetObject")
		})
	}
}

func TestTransferReflowRequireSourceMatchHeadPathDualPath(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		extra    []string
		wantPath string
	}{
		{name: "engine", wantPath: reflowpkg.ExecutionPathEngine},
		{name: "cli-pool", extra: poolRoute, wantPath: reflowpkg.ExecutionPathCLIPool},
	} {
		t.Run(tc.name+"/race-between-head-and-get", func(t *testing.T) {
			withTransferReflowTestState(t)
			s3Srv := newFakeS3HTTPServer(t)
			s3Srv.put("a/race.bin", "AAAAA", "etag-v1", "ver-1", now)
			s3Srv.onHead = func(key string, call int) {
				if call == 1 {
					s3Srv.put(key, "BBBBB", "etag-v2", "ver-2", now.Add(time.Minute))
				}
			}

			srcProv, err := s3.New(context.Background(), s3.Config{
				Bucket:         "source-bucket",
				Region:         "us-east-1",
				Endpoint:       s3Srv.server.URL,
				ForcePathStyle: true,
				Anonymous:      true,
			})
			require.NoError(t, err)
			dstProv := newReflowMemoryProvider()
			checkpointPath := filepath.Join(t.TempDir(), "state.db")

			useTransferReflowProviderFactories(t, providerdispatch.Factories{
				S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
					if cfg.Bucket == "source-bucket" {
						return srcProv, nil
					}
					return dstProv, nil
				},
			})

			line := reflowInputLine("a/race.bin", "etag-v1", 5, "", "") + "\n"
			args := []string{
				"--stdin",
				"--dest", "s3://dest-bucket/data/",
				"--parallel", "1",
				"--checkpoint", checkpointPath,
				"--metadata-policy", "preserve",
				"--require-source-match",
			}
			args = append(args, tc.extra...)
			stdout, err := runSourceMatchReflowCmd(t, strings.NewReader(line), args...)
			require.Error(t, err)

			runRec := requireRecord(t, stdout, reflowpkg.RunRecordType, "")
			require.Contains(t, string(runRec.Data), fmt.Sprintf(`"execution_path":%q`, tc.wantPath))

			records := parseTypedReflowRecords(t, stdout)
			var terminal reflowpkg.Record
			for _, rec := range records {
				if rec.Status != "in_progress" {
					terminal = rec
				}
			}
			require.Equal(t, "failed", terminal.Status)
			require.Equal(t, "source_changed", terminal.Reason)
			require.NotNil(t, terminal.SourceMatch)
			require.Equal(t, "etag-v1", terminal.SourceMatch.ExpectedETag)
			require.Equal(t, "etag-v2", terminal.SourceMatch.ObservedETag)
			require.False(t, dstProv.hasObject("data/a/race.bin"))
			_, uncondGets, _ := s3Srv.counts()
			require.Equal(t, 0, uncondGets)
		})
	}
}

func TestTransferReflowRequireSourceMatchPreflightNonRevisionGetterDualPath(t *testing.T) {
	for _, tc := range []struct {
		name  string
		extra []string
	}{
		{name: "engine"},
		{name: "cli-pool", extra: poolRoute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withTransferReflowTestState(t)
			inner := newReflowMemoryProvider()
			inner.putFixture("source/file.xml", "payload", "src-etag", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
			src := &cmdNonRevisionProvider{inner: inner}
			dst := newReflowMemoryProvider()
			checkpointPath := filepath.Join(t.TempDir(), "state.db")

			useTransferReflowProviderFactories(t, providerdispatch.Factories{
				S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
					if cfg.Bucket == "source-bucket" {
						return src, nil
					}
					return dst, nil
				},
			})

			line := reflowInputLine("source/file.xml", "src-etag", int64(len("payload")), "", "") + "\n"
			args := []string{
				"--stdin",
				"--dest", "s3://dest-bucket/data/",
				"--parallel", "1",
				"--checkpoint", checkpointPath,
				"--require-source-match",
			}
			args = append(args, tc.extra...)
			stdout, err := runSourceMatchReflowCmd(t, strings.NewReader(line), args...)
			require.Error(t, err)
			require.Contains(t, err.Error(), fmt.Sprintf("(exit code %d)", foundry.ExitInvalidArgument))
			require.Contains(t, stdout, `"reason":"replay_unverifiable"`)
			require.Contains(t, stdout, `"missing_capability":"RevisionGetter"`)
			require.Zero(t, src.getCalls.Load(), "preflight must refuse before any source read")
			require.False(t, dst.hasObject("data/source/file.xml"), "preflight must refuse before any destination write")
			require.Empty(t, dst.conditionalPutCallsSnapshot(), "preflight must refuse before any destination IfAbsent write-probe mutates the bucket")
		})
	}
}

func TestTransferReflowRequireSourceMatchSanitizesUnsafeSourceETagDualPath(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	unsafeETags := []string{
		"Bearer top-secret-etag-token",
		`"Bearer quoted-secret-etag-token"`,
		`W/"Bearer weak-secret-etag-token"`,
		`W/"Bearer unmatched-weak-secret-etag-token`,
		`w/"Bearer lower-unmatched-secret-etag-token`,
	}

	for _, unsafeETag := range unsafeETags {
		for _, tc := range []struct {
			name     string
			extra    []string
			wantPath string
		}{
			{name: "engine", wantPath: reflowpkg.ExecutionPathEngine},
			{name: "cli-pool", extra: poolRoute, wantPath: reflowpkg.ExecutionPathCLIPool},
		} {
			t.Run(tc.name+"/"+unsafeETag, func(t *testing.T) {
				withTransferReflowTestState(t)
				s3Srv := newFakeS3HTTPServer(t)
				s3Srv.put("a/item.bin", "BBBBB", "etag-v2", "", now)
				srcProv, err := s3.New(context.Background(), s3.Config{
					Bucket:         "source-bucket",
					Region:         "us-east-1",
					Endpoint:       s3Srv.server.URL,
					ForcePathStyle: true,
					Anonymous:      true,
				})
				require.NoError(t, err)
				dstProv := newReflowMemoryProvider()
				checkpointPath := filepath.Join(t.TempDir(), "state.db")

				useTransferReflowProviderFactories(t, providerdispatch.Factories{
					S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
						if cfg.Bucket == "source-bucket" {
							return srcProv, nil
						}
						return dstProv, nil
					},
				})

				line := reflowInputLine("a/item.bin", unsafeETag, 5, "", "") + "\n"
				args := []string{
					"--stdin",
					"--dest", "s3://dest-bucket/data/",
					"--parallel", "1",
					"--checkpoint", checkpointPath,
					"--require-source-match",
				}
				args = append(args, tc.extra...)
				stdout, err := runSourceMatchReflowCmd(t, strings.NewReader(line), args...)
				require.Error(t, err)
				require.NotContains(t, stdout, "secret-etag-token", "unsafe source_etag must never appear in stdout JSONL")

				db, dbErr := sql.Open("sqlite", checkpointPath)
				require.NoError(t, dbErr)
				defer func() { _ = db.Close() }()
				var storedETag string
				require.NoError(t, db.QueryRow(`SELECT COALESCE(source_etag, '') FROM reflow_items WHERE source_key = ?`, "a/item.bin").Scan(&storedETag))
				require.Empty(t, storedETag, "unsafe source_etag must never persist into checkpoint DB")

				coll := newCollisionInfo(collisionConflict, &provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{ETag: unsafeETag, Size: 5}}, decisionIfAbsentHead)
				require.Empty(t, coll.DestETagObserved, "CLI newCollisionInfo must sanitize dest_etag_observed")
			})
		}
	}

	// End-to-end S3 HTTP regression: an S3 server returning a weak-tag bearer
	// ETag (W/"Bearer s3-http-secret") is normalized by S3 cleanETag into the
	// unmatched-quote form W/"Bearer s3-http-secret. Verify it never leaks into
	// JSONL records, observed_etag, collision metadata, checkpoints, or sidecars.
	for _, tc := range []struct {
		name  string
		extra []string
	}{
		{name: "engine-s3-http-weak-tag"},
		{name: "cli-pool-s3-http-weak-tag", extra: poolRoute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withTransferReflowTestState(t)
			weakServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead {
					w.Header().Set("ETag", `W/"Bearer s3-http-secret"`)
					w.Header().Set("Content-Length", "5")
					w.Header().Set("Last-Modified", now.UTC().Format(http.TimeFormat))
					w.WriteHeader(http.StatusOK)
					return
				}
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusPreconditionFailed)
				_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code><Message>mismatch</Message></Error>`))
			}))
			defer weakServer.Close()

			srcProv, err := s3.New(context.Background(), s3.Config{
				Bucket:         "source-bucket",
				Region:         "us-east-1",
				Endpoint:       weakServer.URL,
				ForcePathStyle: true,
				Anonymous:      true,
			})
			require.NoError(t, err)
			dstProv := newReflowMemoryProvider()
			checkpointPath := filepath.Join(t.TempDir(), "state.db")

			useTransferReflowProviderFactories(t, providerdispatch.Factories{
				S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
					if cfg.Bucket == "source-bucket" {
						return srcProv, nil
					}
					return dstProv, nil
				},
			})

			line := reflowInputLine("a/item.bin", "etag-v1", 5, "", "") + "\n"
			args := []string{
				"--stdin",
				"--dest", "s3://dest-bucket/data/",
				"--parallel", "1",
				"--checkpoint", checkpointPath,
				"--require-source-match",
			}
			args = append(args, tc.extra...)
			stdout, err := runSourceMatchReflowCmd(t, strings.NewReader(line), args...)
			require.Error(t, err)
			require.NotContains(t, stdout, "s3-http-secret", "S3 weak-tag bearer ETag must never leak into JSONL stdout")

			db, dbErr := sql.Open("sqlite", checkpointPath)
			require.NoError(t, dbErr)
			defer func() { _ = db.Close() }()
			var storedETag string
			require.NoError(t, db.QueryRow(`SELECT COALESCE(source_etag, '') FROM reflow_items WHERE source_key = ?`, "a/item.bin").Scan(&storedETag))
			require.NotContains(t, storedETag, "s3-http-secret")
		})
	}
}

func TestTransferReflowRequireSourceMatchFileProviderEndToEnd(t *testing.T) {
	ctx := context.Background()
	srcDir, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	dstDir, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)

	unchangedPath := filepath.Join(srcDir, "unchanged.txt")
	replacedPath := filepath.Join(srcDir, "replaced.txt")
	deletedPath := filepath.Join(srcDir, "deleted.txt")
	require.NoError(t, os.WriteFile(unchangedPath, []byte("alpha"), 0o600))
	require.NoError(t, os.WriteFile(replacedPath, []byte("bravo"), 0o600))
	require.NoError(t, os.WriteFile(deletedPath, []byte("delta"), 0o600))

	srcProv, err := providerfile.New(providerfile.Config{BaseDir: srcDir})
	require.NoError(t, err)

	headUnchanged, err := srcProv.Head(ctx, "unchanged.txt")
	require.NoError(t, err)
	headReplaced, err := srcProv.Head(ctx, "replaced.txt")
	require.NoError(t, err)
	headDeleted, err := srcProv.Head(ctx, "deleted.txt")
	require.NoError(t, err)

	// Mutate replaced.txt with same size + newer mtime, and delete deleted.txt.
	require.NoError(t, os.WriteFile(replacedPath, []byte("BRAVO"), 0o600))
	newer := time.Now().Add(2 * time.Second)
	require.NoError(t, os.Chtimes(replacedPath, newer, newer))
	require.NoError(t, os.Remove(deletedPath))

	headReplacedAfter, err := srcProv.Head(ctx, "replaced.txt")
	require.NoError(t, err)
	require.NotEqual(t, headReplaced.ETag, headReplacedAfter.ETag)

	withTransferReflowTestState(t)
	checkpointPath := filepath.Join(t.TempDir(), "state.db")

	makeFileInputLine := func(key, etag string, size int64) string {
		rec := map[string]any{
			"type": "gonimbus.reflow.input.v1",
			"data": map[string]any{
				"source_uri":        "file://" + filepath.Join(srcDir, key),
				"source_key":        key,
				"source_etag":       etag,
				"source_size_bytes": size,
				"dest_rel_key":      key,
			},
		}
		raw, marshalErr := json.Marshal(rec)
		require.NoError(t, marshalErr)
		return string(raw)
	}

	input := strings.Join([]string{
		makeFileInputLine("unchanged.txt", headUnchanged.ETag, headUnchanged.Size),
		makeFileInputLine("replaced.txt", headReplaced.ETag, headReplaced.Size),
		makeFileInputLine("deleted.txt", headDeleted.ETag, headDeleted.Size),
	}, "\n") + "\n"

	stdout, err := runSourceMatchReflowCmd(t, strings.NewReader(input),
		"--stdin",
		"--dest", "file://"+dstDir,
		"--parallel", "1",
		"--checkpoint", checkpointPath,
		"--require-source-match",
	)
	require.Error(t, err)

	records := parseTypedReflowRecords(t, stdout)
	byKey := map[string]reflowpkg.Record{}
	for _, rec := range records {
		if rec.Status != "in_progress" {
			byKey[rec.SourceKey] = rec
		}
	}

	// Unchanged file succeeds with read-bound source_etag & source_match.
	require.Equal(t, "complete", byKey["unchanged.txt"].Status)
	require.Equal(t, headUnchanged.ETag, byKey["unchanged.txt"].SourceETag)
	require.NotNil(t, byKey["unchanged.txt"].SourceMatch)
	require.Equal(t, headUnchanged.ETag, byKey["unchanged.txt"].SourceMatch.ExpectedETag)
	require.Equal(t, headUnchanged.ETag, byKey["unchanged.txt"].SourceMatch.ObservedETag)
	gotBody, err := os.ReadFile(filepath.Join(dstDir, "unchanged.txt"))
	require.NoError(t, err)
	require.Equal(t, "alpha", string(gotBody))

	// Same-size replaced file fails with source_changed and observed_etag.
	require.Equal(t, "failed", byKey["replaced.txt"].Status)
	require.Equal(t, "source_changed", byKey["replaced.txt"].Reason)
	require.NotNil(t, byKey["replaced.txt"].SourceMatch)
	require.Equal(t, headReplaced.ETag, byKey["replaced.txt"].SourceMatch.ExpectedETag)
	require.Equal(t, headReplacedAfter.ETag, byKey["replaced.txt"].SourceMatch.ObservedETag)
	_, statErr := os.Stat(filepath.Join(dstDir, "replaced.txt"))
	require.True(t, os.IsNotExist(statErr), "changed file must not land at destination")

	// Deleted file fails with not_found (NOT source_changed).
	require.Equal(t, "failed", byKey["deleted.txt"].Status)
	require.Equal(t, "not_found", byKey["deleted.txt"].Reason)
	require.Nil(t, byKey["deleted.txt"].SourceMatch)
	_, statErr = os.Stat(filepath.Join(dstDir, "deleted.txt"))
	require.True(t, os.IsNotExist(statErr))
}

func TestTransferReflowRequireSourceMatchGCSFakeServerLibrary(t *testing.T) {
	ctx := context.Background()
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		Scheme: "http",
		InitialObjects: []fakestorage.Object{
			{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "src-bucket", Name: "ok.txt"}, Content: []byte("alpha")},
			{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "src-bucket", Name: "same-size.txt"}, Content: []byte("bravo")},
			{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "src-bucket", Name: "deleted.txt"}, Content: []byte("delta")},
		},
	})
	require.NoError(t, err)
	t.Cleanup(server.Stop)

	t.Setenv("STORAGE_EMULATOR_HOST", strings.TrimPrefix(server.URL(), "http://"))
	srcProv, err := gcs.New(ctx, gcs.Config{
		Bucket: "src-bucket",
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = srcProv.Close() })

	headOK, err := srcProv.Head(ctx, "ok.txt")
	require.NoError(t, err)
	headSameSize, err := srcProv.Head(ctx, "same-size.txt")
	require.NoError(t, err)
	headDeleted, err := srcProv.Head(ctx, "deleted.txt")
	require.NoError(t, err)

	// Replace same-size.txt and delete deleted.txt after admission.
	require.NoError(t, srcProv.PutObject(ctx, "same-size.txt", strings.NewReader("BRAVO"), 5))
	headSameSizeAfter, err := srcProv.Head(ctx, "same-size.txt")
	require.NoError(t, err)
	require.NotEqual(t, headSameSize.ETag, headSameSizeAfter.ETag)
	require.NoError(t, srcProv.DeleteObject(ctx, "deleted.txt"))

	dstProv := newReflowMemoryProvider()
	sink := &testJSONLSink{}
	runner, err := reflowpkg.NewRunner(reflowpkg.Config{
		Destination:        reflowpkg.Destination{Provider: dstProv, ProviderID: "s3", BaseURI: "s3://dest-bucket/data/"},
		Rewrite:            reflowpkg.RewriteConfig{From: "{key}", To: "{key}"},
		Collision:          reflowpkg.CollisionPolicy{Mode: reflowpkg.CollisionSkipIfDuplicate},
		Concurrency:        reflowpkg.ResolveConcurrency(1, true, reflowpkg.DefaultResourceProbe()),
		RequireSourceMatch: true,
		Events:             sink,
	})
	require.NoError(t, err)

	// Inject the live gcs.Provider backed by fake-gcs-server into reflow.Runner
	// (PrefixSource URI parsing currently constrains the scheme label to s3://
	// while delegating all reads/HEADs directly to the injected provider).
	_, err = runner.Run(ctx, reflowpkg.PrefixSource{
		Provider: &gcsSnapshotListProvider{
			Provider: srcProv,
			snapshot: []provider.ObjectSummary{
				{Key: "ok.txt", Size: headOK.Size, ETag: headOK.ETag},
				{Key: "same-size.txt", Size: headSameSize.Size, ETag: headSameSize.ETag},
				{Key: "deleted.txt", Size: headDeleted.Size, ETag: headDeleted.ETag},
			},
		},
		URI: "s3://src-bucket/",
	})
	require.Error(t, err)

	byKey := map[string]reflowpkg.Record{}
	for _, rec := range sink.records {
		if rec.Status != "in_progress" {
			byKey[rec.SourceKey] = rec
		}
	}

	require.Equal(t, "complete", byKey["ok.txt"].Status)
	require.Equal(t, headOK.ETag, byKey["ok.txt"].SourceETag)
	require.Equal(t, headOK.Version, byKey["ok.txt"].SourceVersionID)
	require.NotNil(t, byKey["ok.txt"].SourceMatch)
	require.Equal(t, headOK.ETag, byKey["ok.txt"].SourceMatch.ExpectedETag)
	require.Equal(t, headOK.ETag, byKey["ok.txt"].SourceMatch.ObservedETag)
	require.Equal(t, headOK.Version, byKey["ok.txt"].SourceMatch.ObservedVersionID)
	require.Equal(t, "alpha", string(dstProv.mustObject("data/ok.txt")))

	require.Equal(t, "failed", byKey["same-size.txt"].Status)
	require.Equal(t, "source_changed", byKey["same-size.txt"].Reason)
	require.NotNil(t, byKey["same-size.txt"].SourceMatch)
	require.Equal(t, headSameSize.ETag, byKey["same-size.txt"].SourceMatch.ExpectedETag)
	require.Equal(t, headSameSizeAfter.ETag, byKey["same-size.txt"].SourceMatch.ObservedETag)
	require.False(t, dstProv.hasObject("data/same-size.txt"))

	require.Equal(t, "failed", byKey["deleted.txt"].Status)
	require.Equal(t, "not_found", byKey["deleted.txt"].Reason)
	require.Nil(t, byKey["deleted.txt"].SourceMatch)
	require.False(t, dstProv.hasObject("data/deleted.txt"))
}

func TestTransferReflowRequireSourceMatchOffByteIdenticalDualPath(t *testing.T) {
	const wantExactV050 = `{"source_uri":"s3://source-bucket/source/file.xml","source_bucket":"source-bucket","source_key":"source/file.xml","source_etag":"src-etag","source_size_bytes":7,"source_last_modified":"2026-01-15T20:53:44Z","dest_sha256":"239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5","dest_etag":"dest-data/source/file.xml","dest_uri":"s3://dest-bucket/data/source/file.xml","dest_key":"data/source/file.xml","bytes":7,"status":"complete"}`
	for _, tc := range []struct {
		name     string
		extra    []string
		wantPath string
	}{
		{name: "engine", wantPath: reflowpkg.ExecutionPathEngine},
		{name: "cli-pool", extra: poolRoute, wantPath: reflowpkg.ExecutionPathCLIPool},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newFlagProbeEnv(t)
			stdout, err := env.run(t, tc.extra...)
			requireProbeComplete(t, stdout, err, tc.wantPath)
			rec := requireRecord(t, stdout, reflowpkg.RecordType, "complete")
			require.Equal(t, wantExactV050, string(rec.Data), "flag-off complete receipt must remain byte-identical to v0.5.0")
		})
	}
}

type cliFailingReader struct {
	chunks [][]byte
	err    error
	idx    int
}

func (r *cliFailingReader) Read(p []byte) (int, error) {
	if r.idx < len(r.chunks) {
		n := copy(p, r.chunks[r.idx])
		r.idx++
		if r.idx == len(r.chunks) {
			return n, r.err
		}
		return n, nil
	}
	return 0, io.EOF
}

func TestTransferReflowRequireSourceMatchCLIPoolPreflightHardening(t *testing.T) {
	advisoryLine := `{"type":"gonimbus.custom.heartbeat.v1","advisory":true,"data":{}}` + "\n"
	validLine := reflowInputLine("source/file.xml", "src-etag", 7, "", "")

	t.Run("failing reader after advisory prefix propagates error without copying or probing", func(t *testing.T) {
		withTransferReflowTestState(t)
		src := newReflowMemoryProvider()
		src.putFixture("source/file.xml", "payload", "src-etag", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
		dst := newReflowMemoryProvider()
		checkpointPath := filepath.Join(t.TempDir(), "state.db")

		useTransferReflowProviderFactories(t, providerdispatch.Factories{
			S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
				if cfg.Bucket == "source-bucket" {
					return src, nil
				}
				return dst, nil
			},
		})

		in := &cliFailingReader{
			chunks: [][]byte{[]byte(advisoryLine), []byte(validLine)},
			err:    io.ErrUnexpectedEOF,
		}
		var stdout, stderr bytes.Buffer
		cmd := newTransferReflowTestCommand()
		cmd.SetIn(in)
		cmd.SetOut(&stdout)
		cmd.SetErr(&stderr)
		cmd.SetArgs(append([]string{
			"--stdin",
			"--dest", "s3://dest-bucket/data/",
			"--parallel", "1",
			"--checkpoint", checkpointPath,
			"--require-source-match",
		}, poolRoute...))
		err := cmd.ExecuteContext(context.Background())
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.Empty(t, dst.conditionalPutCallsSnapshot(), "failing stream must refuse before destination probe")
		require.False(t, dst.hasObject("data/source/file.xml"))
	})

	t.Run("blank preamble after advisory prefix exceeding sniff cap refuses before destination probe", func(t *testing.T) {
		withTransferReflowTestState(t)
		inner := newReflowMemoryProvider()
		inner.putFixture("source/file.xml", "payload", "src-etag", time.Date(2026, 1, 15, 20, 53, 44, 0, time.UTC))
		src := &cmdNonRevisionProvider{inner: inner}
		dst := newReflowMemoryProvider()
		checkpointPath := filepath.Join(t.TempDir(), "state.db")

		useTransferReflowProviderFactories(t, providerdispatch.Factories{
			S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
				if cfg.Bucket == "source-bucket" {
					return src, nil
				}
				return dst, nil
			},
		})

		stream := advisoryLine + strings.Repeat("\n", (1<<20)+64) + validLine + "\n"
		args := append([]string{
			"--stdin",
			"--dest", "s3://dest-bucket/data/",
			"--parallel", "1",
			"--checkpoint", checkpointPath,
			"--require-source-match",
		}, poolRoute...)
		stdout, err := runSourceMatchReflowCmd(t, strings.NewReader(stream), args...)
		require.Error(t, err)
		require.Contains(t, err.Error(), fmt.Sprintf("(exit code %d)", foundry.ExitInvalidArgument))
		require.Contains(t, stdout, `"reason":"replay_unverifiable"`)
		require.Empty(t, dst.conditionalPutCallsSnapshot(), "budget exhaustion must refuse before destination probe")
	})
}

type testJSONLSink struct {
	mu      sync.Mutex
	records []reflowpkg.Record
}

func (s *testJSONLSink) OnRun(context.Context, reflowpkg.RunRecord) error          { return nil }
func (s *testJSONLSink) OnSource(context.Context, reflowpkg.SourceRunRecord) error { return nil }
func (s *testJSONLSink) OnWarning(context.Context, reflowpkg.Warning) error        { return nil }
func (s *testJSONLSink) OnError(context.Context, reflowpkg.ErrorEvent) error       { return nil }
func (s *testJSONLSink) OnSummary(context.Context, reflowpkg.SummaryRecord) error  { return nil }
func (s *testJSONLSink) OnRecord(_ context.Context, rec reflowpkg.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, rec)
	return nil
}

type gcsSnapshotListProvider struct {
	*gcs.Provider
	snapshot []provider.ObjectSummary
}

func (p *gcsSnapshotListProvider) List(_ context.Context, _ provider.ListOptions) (*provider.ListResult, error) {
	return &provider.ListResult{Objects: append([]provider.ObjectSummary(nil), p.snapshot...)}, nil
}

type noIfAbsentDestProvider struct {
	*reflowMemoryProvider
	onHead func(key string)
}

func (p *noIfAbsentDestProvider) ConditionalWriteCapabilities() provider.ConditionalWriteCapabilities {
	return provider.ConditionalWriteCapabilities{}
}

func (p *noIfAbsentDestProvider) Head(ctx context.Context, key string) (*provider.ObjectMeta, error) {
	if p.onHead != nil {
		p.onHead(key)
	}
	return p.reflowMemoryProvider.Head(ctx, key)
}

func readCheckpointItemStatusReason(t *testing.T, checkpointPath, sourceKey string) (status, reason string) {
	t.Helper()
	db, err := sql.Open("sqlite", checkpointPath)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	err = db.QueryRow(`SELECT status, COALESCE(reason, '') FROM reflow_items WHERE source_key = ?`, sourceKey).Scan(&status, &reason)
	require.NoError(t, err, "expected checkpoint row for source_key=%s", sourceKey)
	return status, reason
}

func TestTransferReflowRequireSourceMatchCollisionBodyCompareDualPath(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		extra    []string
		wantPath string
	}{
		{name: "engine", wantPath: reflowpkg.ExecutionPathEngine},
		{name: "cli-pool", extra: poolRoute, wantPath: reflowpkg.ExecutionPathCLIPool},
	} {
		t.Run(tc.name+"/source-mutated-to-match-dest", func(t *testing.T) {
			withTransferReflowTestState(t)
			s3Srv := newFakeS3HTTPServer(t)
			// Source mutated in-place to match the existing destination bytes ("NEW!!").
			s3Srv.put("a/item.bin", "NEW!!", "etag-v2", "ver-2", now)

			srcProv, err := s3.New(context.Background(), s3.Config{
				Bucket:         "source-bucket",
				Region:         "us-east-1",
				Endpoint:       s3Srv.server.URL,
				ForcePathStyle: true,
				Anonymous:      true,
			})
			require.NoError(t, err)
			dstInner := newReflowMemoryProvider()
			// Existing destination with non-matching multipart ETag forces objectBodiesEqual.
			dstInner.putFixture("data/a/item.bin", "NEW!!", "deadbeef-2", now)
			dstProv := &noIfAbsentDestProvider{reflowMemoryProvider: dstInner}
			checkpointPath := filepath.Join(t.TempDir(), "state.db")

			useTransferReflowProviderFactories(t, providerdispatch.Factories{
				S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
					if cfg.Bucket == "source-bucket" {
						return srcProv, nil
					}
					return dstProv, nil
				},
			})

			line := reflowInputLine("a/item.bin", "etag-v1", 5, "", "") + "\n"
			args := []string{
				"--stdin",
				"--dest", "s3://dest-bucket/data/",
				"--parallel", "1",
				"--checkpoint", checkpointPath,
				"--on-collision", "skip-if-duplicate",
				"--require-source-match",
			}
			args = append(args, tc.extra...)
			stdout, err := runSourceMatchReflowCmd(t, strings.NewReader(line), args...)
			require.Error(t, err)

			runRec := requireRecord(t, stdout, reflowpkg.RunRecordType, "")
			require.Contains(t, string(runRec.Data), fmt.Sprintf(`"execution_path":%q`, tc.wantPath))

			records := parseTypedReflowRecords(t, stdout)
			var terminal reflowpkg.Record
			for _, rec := range records {
				if rec.Status != "in_progress" {
					terminal = rec
				}
			}
			require.Equal(t, "failed", terminal.Status, "mutated source matching dest must fail as source_changed, not skipped/duplicate")
			require.Equal(t, "source_changed", terminal.Reason)
			require.NotNil(t, terminal.SourceMatch)
			require.Equal(t, "etag-v1", terminal.SourceMatch.ExpectedETag)
			require.Equal(t, "etag-v2", terminal.SourceMatch.ObservedETag)

			st, rsn := readCheckpointItemStatusReason(t, checkpointPath, "a/item.bin")
			require.Equal(t, "failed", st)
			require.Equal(t, "source_changed", rsn)

			_, uncondGets, _ := s3Srv.counts()
			require.Equal(t, 0, uncondGets, "collision body compare must never call unconditional GetObject")
		})

		t.Run(tc.name+"/source-deleted-before-body-compare", func(t *testing.T) {
			withTransferReflowTestState(t)
			s3Srv := newFakeS3HTTPServer(t)
			// Source object intentionally absent (deleted).

			srcProv, err := s3.New(context.Background(), s3.Config{
				Bucket:         "source-bucket",
				Region:         "us-east-1",
				Endpoint:       s3Srv.server.URL,
				ForcePathStyle: true,
				Anonymous:      true,
			})
			require.NoError(t, err)
			dstInner := newReflowMemoryProvider()
			dstInner.putFixture("data/a/item.bin", "OLD!!", "deadbeef-2", now)
			dstProv := &noIfAbsentDestProvider{reflowMemoryProvider: dstInner}
			checkpointPath := filepath.Join(t.TempDir(), "state.db")

			useTransferReflowProviderFactories(t, providerdispatch.Factories{
				S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
					if cfg.Bucket == "source-bucket" {
						return srcProv, nil
					}
					return dstProv, nil
				},
			})

			line := reflowInputLine("a/item.bin", "etag-v1", 5, "", "") + "\n"
			args := []string{
				"--stdin",
				"--dest", "s3://dest-bucket/data/",
				"--parallel", "1",
				"--checkpoint", checkpointPath,
				"--on-collision", "skip-if-duplicate",
				"--require-source-match",
			}
			args = append(args, tc.extra...)
			stdout, err := runSourceMatchReflowCmd(t, strings.NewReader(line), args...)
			require.Error(t, err)

			records := parseTypedReflowRecords(t, stdout)
			var terminal reflowpkg.Record
			for _, rec := range records {
				if rec.Status != "in_progress" {
					terminal = rec
				}
			}
			require.Equal(t, "failed", terminal.Status)
			require.Equal(t, "not_found", terminal.Reason)
			require.Nil(t, terminal.SourceMatch)

			st, rsn := readCheckpointItemStatusReason(t, checkpointPath, "a/item.bin")
			require.Equal(t, "failed", st)
			require.Equal(t, "not_found", rsn)

			_, uncondGets, _ := s3Srv.counts()
			require.Equal(t, 0, uncondGets)
		})
	}
}

func TestTransferReflowRequireSourceMatchDeletedOnRequiredHeadDualPath(t *testing.T) {
	for _, tc := range []struct {
		name     string
		extra    []string
		wantPath string
	}{
		{name: "engine", wantPath: reflowpkg.ExecutionPathEngine},
		{name: "cli-pool", extra: poolRoute, wantPath: reflowpkg.ExecutionPathCLIPool},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withTransferReflowTestState(t)
			s3Srv := newFakeS3HTTPServer(t) // source intentionally absent (404 on required HEAD)
			srcProv, err := s3.New(context.Background(), s3.Config{
				Bucket:         "source-bucket",
				Region:         "us-east-1",
				Endpoint:       s3Srv.server.URL,
				ForcePathStyle: true,
				Anonymous:      true,
			})
			require.NoError(t, err)
			dstProv := newReflowMemoryProvider()
			checkpointPath := filepath.Join(t.TempDir(), "state.db")

			useTransferReflowProviderFactories(t, providerdispatch.Factories{
				S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
					if cfg.Bucket == "source-bucket" {
						return srcProv, nil
					}
					return dstProv, nil
				},
			})

			line := reflowInputLine("a/gone.bin", "etag-v1", 5, "", "") + "\n"
			args := []string{
				"--stdin",
				"--dest", "s3://dest-bucket/data/",
				"--parallel", "1",
				"--checkpoint", checkpointPath,
				"--metadata-policy", "preserve",
				"--require-source-match",
			}
			args = append(args, tc.extra...)
			stdout, err := runSourceMatchReflowCmd(t, strings.NewReader(line), args...)
			require.Error(t, err)

			runRec := requireRecord(t, stdout, reflowpkg.RunRecordType, "")
			require.Contains(t, string(runRec.Data), fmt.Sprintf(`"execution_path":%q`, tc.wantPath))

			records := parseTypedReflowRecords(t, stdout)
			var terminal reflowpkg.Record
			for _, rec := range records {
				if rec.Status != "in_progress" {
					terminal = rec
				}
			}
			require.Equal(t, "failed", terminal.Status, "required HEAD 404 must emit a failed terminal rather than stopping at in_progress")
			require.Equal(t, "not_found", terminal.Reason)

			st, rsn := readCheckpointItemStatusReason(t, checkpointPath, "a/gone.bin")
			require.Equal(t, "failed", st)
			require.Equal(t, "not_found", rsn)
		})
	}
}

func TestTransferReflowRequireSourceMatchQuarantineDeletedPreservesNotFound(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

	t.Run("collision-quarantine", func(t *testing.T) {
		withTransferReflowTestState(t)
		s3Srv := newFakeS3HTTPServer(t)
		s3Srv.put("a/item.bin", "AAAAA", "etag-v1", "ver-1", now)
		srcProv, err := s3.New(context.Background(), s3.Config{
			Bucket:         "source-bucket",
			Region:         "us-east-1",
			Endpoint:       s3Srv.server.URL,
			ForcePathStyle: true,
			Anonymous:      true,
		})
		require.NoError(t, err)

		dstInner := newReflowMemoryProvider()
		dstInner.putFixture("data/a/item.bin", "CONFLICT-DIFFERENT-SIZE", "dst-etag", now)
		dstProv := &noIfAbsentDestProvider{
			reflowMemoryProvider: dstInner,
			onHead: func(key string) {
				if key == "data/a/item.bin" {
					// Delete source right before the collision quarantine copy executes.
					s3Srv.del("a/item.bin")
				}
			},
		}
		checkpointPath := filepath.Join(t.TempDir(), "state.db")

		useTransferReflowProviderFactories(t, providerdispatch.Factories{
			S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
				if cfg.Bucket == "source-bucket" {
					return srcProv, nil
				}
				return dstProv, nil
			},
		})

		line := reflowInputLine("a/item.bin", "etag-v1", 5, "", "") + "\n"
		stdout, err := runSourceMatchReflowCmd(t, strings.NewReader(line),
			"--stdin",
			"--dest", "s3://dest-bucket/data/",
			"--parallel", "1",
			"--checkpoint", checkpointPath,
			"--on-collision", "quarantine",
			"--collision-quarantine-prefix", "_quar",
			"--require-source-match",
		)
		require.Error(t, err)

		records := parseTypedReflowRecords(t, stdout)
		var terminal reflowpkg.Record
		for _, rec := range records {
			if rec.Status != "in_progress" {
				terminal = rec
			}
		}
		require.Equal(t, "failed", terminal.Status)
		require.Equal(t, "not_found", terminal.Reason)
		require.Equal(t, "quarantine", terminal.RoutingClass)

		st, rsn := readCheckpointItemStatusReason(t, checkpointPath, "a/item.bin")
		require.Equal(t, "failed", st)
		require.Equal(t, "not_found", rsn)
	})

	t.Run("metadata-quarantine", func(t *testing.T) {
		withTransferReflowTestState(t)
		s3Srv := newFakeS3HTTPServer(t)
		s3Srv.put("a/item.bin", "AAAAA", "etag-v1", "ver-1", now)
		s3Srv.onHead = func(key string, call int) {
			if call == 1 {
				// HEAD succeeds so metadata derivation runs and routes to quarantine,
				// then source is deleted immediately before the quarantine copy GET.
				s3Srv.del(key)
			}
		}
		srcProv, err := s3.New(context.Background(), s3.Config{
			Bucket:         "source-bucket",
			Region:         "us-east-1",
			Endpoint:       s3Srv.server.URL,
			ForcePathStyle: true,
			Anonymous:      true,
		})
		require.NoError(t, err)
		dstProv := newReflowMemoryProvider()
		checkpointPath := filepath.Join(t.TempDir(), "state.db")

		useTransferReflowProviderFactories(t, providerdispatch.Factories{
			S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
				if cfg.Bucket == "source-bucket" {
					return srcProv, nil
				}
				return dstProv, nil
			},
		})

		line := reflowInputLine("a/item.bin", "etag-v1", 5, "", "") + "\n"
		stdout, err := runSourceMatchReflowCmd(t, strings.NewReader(line),
			"--stdin",
			"--dest", "s3://dest-bucket/data/",
			"--parallel", "1",
			"--checkpoint", checkpointPath,
			"--metadata-set-from-source-derived", "missing=meta.absent.token",
			"--metadata-on-missing-source", "fail",
			"--on-collision", "quarantine",
			"--collision-quarantine-prefix", "_quar",
			"--require-source-match",
		)
		require.Error(t, err)

		records := parseTypedReflowRecords(t, stdout)
		var terminal reflowpkg.Record
		for _, rec := range records {
			if rec.Status != "in_progress" {
				terminal = rec
			}
		}
		require.Equal(t, "failed", terminal.Status)
		require.Equal(t, "not_found", terminal.Reason)
		require.Equal(t, "quarantine", terminal.RoutingClass)

		st, rsn := readCheckpointItemStatusReason(t, checkpointPath, "a/item.bin")
		require.Equal(t, "failed", st)
		require.Equal(t, "not_found", rsn)
	})
}

func TestTransferReflowSourceVersionIDS3DualPath(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	paths := []struct {
		name     string
		extra    []string
		wantPath string
	}{
		{name: "engine", wantPath: reflowpkg.ExecutionPathEngine},
		{name: "cli-pool", extra: poolRoute, wantPath: reflowpkg.ExecutionPathCLIPool},
	}

	for _, tc := range paths {
		for _, requireMatch := range []bool{true, false} {
			modeLabel := "flag-off"
			if requireMatch {
				modeLabel = "flag-on"
			}
			t.Run(tc.name+"/"+modeLabel, func(t *testing.T) {
				withTransferReflowTestState(t)
				s3Srv := newFakeS3HTTPServer(t)
				// Overwritten key: reviewed v1 still exists historically while latest is v2.
				s3Srv.put("a/reviewed.xml", "reviewed-v1", "etag-v1", "ver-1", now)
				s3Srv.put("a/reviewed.xml", "mutated-v2-different-size", "etag-v2", "ver-2", now.Add(time.Minute))
				// Existing key with another version for NoSuchVersion -> source_changed.
				s3Srv.put("a/wrongver.xml", "live-v2", "etag-live", "ver-live", now)
				// Deleted key for NoSuchVersion -> not_found (never put).

				srcProv, err := s3.New(context.Background(), s3.Config{
					Bucket:         "source-bucket",
					Region:         "us-east-1",
					Endpoint:       s3Srv.server.URL,
					ForcePathStyle: true,
					Anonymous:      true,
				})
				require.NoError(t, err)
				dstProv := newReflowMemoryProvider()
				checkpointPath := filepath.Join(t.TempDir(), "state.db")

				useTransferReflowProviderFactories(t, providerdispatch.Factories{
					S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
						if cfg.Bucket == "source-bucket" {
							return srcProv, nil
						}
						return dstProv, nil
					},
				})

				input := strings.Join([]string{
					`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/reviewed.xml","source_key":"a/reviewed.xml","source_etag":"etag-v1","source_version_id":"ver-1","source_size_bytes":11,"dest_rel_key":"a/reviewed.xml"}}`,
					`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/reviewed.xml","source_key":"a/reviewed.xml","source_etag":"wrong-etag","source_version_id":"ver-1","source_size_bytes":11,"dest_rel_key":"a/mismatch.xml"}}`,
					`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/wrongver.xml","source_key":"a/wrongver.xml","source_etag":"etag-old","source_version_id":"ver-deleted","source_size_bytes":7,"dest_rel_key":"a/wrongver.xml"}}`,
					`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/missing.xml","source_key":"a/missing.xml","source_etag":"etag-gone","source_version_id":"ver-gone","source_size_bytes":5,"dest_rel_key":"a/missing.xml"}}`,
				}, "\n") + "\n"

				args := []string{
					"--stdin",
					"--dest", "s3://dest-bucket/data/",
					"--parallel", "1",
					"--checkpoint", checkpointPath,
					"--metadata-policy", "preserve",
				}
				if requireMatch {
					args = append(args, "--require-source-match")
				}
				args = append(args, tc.extra...)

				stdout, err := runSourceMatchReflowCmd(t, strings.NewReader(input), args...)
				require.Error(t, err)

				sum := requireRecord(t, stdout, reflowpkg.SummaryRecordType, "")
				require.Contains(t, string(sum.Data), fmt.Sprintf(`"execution_path":"%s"`, tc.wantPath))

				byDest := map[string]reflowpkg.Record{}
				for _, rec := range parseTypedReflowRecords(t, stdout) {
					if rec.Status != "in_progress" {
						byDest[rec.DestKey] = rec
					}
				}

				// 1. Historical version v1 copied even after v2 overwrite and metadata HEAD.
				reviewed := byDest["data/a/reviewed.xml"]
				require.Equal(t, "complete", reviewed.Status)
				require.Equal(t, "reviewed-v1", string(dstProv.mustObject("data/a/reviewed.xml")))
				require.Equal(t, "etag-v1", reviewed.SourceETag)
				require.Equal(t, "ver-1", reviewed.SourceVersionID)
				require.NotNil(t, reviewed.SourceMatch)
				require.Equal(t, "etag-v1", reviewed.SourceMatch.ExpectedETag)
				require.Equal(t, "ver-1", reviewed.SourceMatch.ExpectedVersionID)
				require.Equal(t, "etag-v1", reviewed.SourceMatch.ObservedETag)
				require.Equal(t, "ver-1", reviewed.SourceMatch.ObservedVersionID)

				// 2. Dual-token ETag mismatch fails closed with zero dest bytes and pinned version ETag.
				mismatch := byDest["data/a/mismatch.xml"]
				require.Equal(t, "failed", mismatch.Status)
				require.Equal(t, "source_changed", mismatch.Reason)
				require.False(t, dstProv.hasObject("data/a/mismatch.xml"))
				require.NotNil(t, mismatch.SourceMatch)
				require.Equal(t, "wrong-etag", mismatch.SourceMatch.ExpectedETag)
				require.Equal(t, "ver-1", mismatch.SourceMatch.ExpectedVersionID)
				require.Equal(t, "etag-v1", mismatch.SourceMatch.ObservedETag)
				require.Equal(t, "ver-1", mismatch.SourceMatch.ObservedVersionID)

				// 3. NoSuchVersion on existing key reports source_changed with live observed version.
				wrongver := byDest["data/a/wrongver.xml"]
				require.Equal(t, "failed", wrongver.Status)
				require.Equal(t, "source_changed", wrongver.Reason)
				require.False(t, dstProv.hasObject("data/a/wrongver.xml"))
				require.NotNil(t, wrongver.SourceMatch)
				require.Equal(t, "ver-deleted", wrongver.SourceMatch.ExpectedVersionID)
				require.Equal(t, "ver-live", wrongver.SourceMatch.ObservedVersionID)

				// 4. NoSuchVersion on deleted key reports not_found.
				missing := byDest["data/a/missing.xml"]
				require.Equal(t, "failed", missing.Status)
				require.Equal(t, "not_found", missing.Reason)
				require.False(t, dstProv.hasObject("data/a/missing.xml"))
				require.Nil(t, missing.SourceMatch)

				_, uncondGets, _ := s3Srv.counts()
				require.Zero(t, uncondGets, "version-pinned reflow must never issue unpinned GetObject")
			})
		}
	}
}

func TestTransferReflowSourceVersionIDFileRefusal(t *testing.T) {
	srcRoot, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(srcRoot, "doc.txt"), []byte("hello"), 0o600))
	srcURI := fileURI(filepath.Join(srcRoot, "doc.txt"))

	line := fmt.Sprintf(`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":%q,"source_key":"doc.txt","source_etag":"any-etag","source_version_id":"v1","source_size_bytes":5,"dest_rel_key":"doc.txt"}}`+"\n", srcURI)

	t.Run("flag-on refuses at preflight with exit 40", func(t *testing.T) {
		withTransferReflowTestState(t)
		dstProv := newReflowMemoryProvider()
		useTransferReflowProviderFactories(t, providerdispatch.Factories{
			S3: func(_ context.Context, _ s3.Config) (provider.Provider, error) { return dstProv, nil },
		})

		stdout, execErr := runSourceMatchReflowCmd(t, strings.NewReader(line),
			"--stdin",
			"--dest", "s3://dest-bucket/data/",
			"--parallel", "1",
			"--checkpoint", filepath.Join(t.TempDir(), "state.db"),
			"--require-source-match",
		)
		require.Error(t, execErr)
		require.Contains(t, execErr.Error(), fmt.Sprintf("(exit code %d)", foundry.ExitInvalidArgument))
		require.Contains(t, stdout, `"reason":"replay_unverifiable"`)
		require.Empty(t, dstProv.conditionalPutCallsSnapshot(), "preflight refusal must precede destination probes")
		require.False(t, dstProv.hasObject("data/doc.txt"))
	})

	t.Run("flag-off refuses per-line with replay_unverifiable", func(t *testing.T) {
		withTransferReflowTestState(t)
		dstProv := newReflowMemoryProvider()
		useTransferReflowProviderFactories(t, providerdispatch.Factories{
			S3: func(_ context.Context, _ s3.Config) (provider.Provider, error) { return dstProv, nil },
		})

		stdout, execErr := runSourceMatchReflowCmd(t, strings.NewReader(line),
			"--stdin",
			"--dest", "s3://dest-bucket/data/",
			"--parallel", "1",
			"--checkpoint", filepath.Join(t.TempDir(), "state.db"),
		)
		require.Error(t, execErr)
		require.False(t, dstProv.hasObject("data/doc.txt"))
		records := parseTypedReflowRecords(t, stdout)
		var terminal reflowpkg.Record
		for _, rec := range records {
			if rec.Status != "in_progress" {
				terminal = rec
			}
		}
		require.Equal(t, "failed", terminal.Status)
		require.Equal(t, "replay_unverifiable", terminal.Reason)
	})

	t.Run("multi-record stream refuses later file source_version_id per-line", func(t *testing.T) {
		withTransferReflowTestState(t)
		okPath := filepath.Join(srcRoot, "ok.txt")
		require.NoError(t, os.WriteFile(okPath, []byte("alpha"), 0o600))
		fileProv, err := providerfile.New(providerfile.Config{BaseDir: srcRoot})
		require.NoError(t, err)
		defer func() { _ = fileProv.Close() }()
		okMeta, err := fileProv.Head(context.Background(), "ok.txt")
		require.NoError(t, err)

		dstProv := newReflowMemoryProvider()
		useTransferReflowProviderFactories(t, providerdispatch.Factories{
			S3: func(_ context.Context, _ s3.Config) (provider.Provider, error) { return dstProv, nil },
		})

		stream := strings.Join([]string{
			fmt.Sprintf(`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":%q,"source_key":"ok.txt","source_etag":%q,"source_size_bytes":5,"dest_rel_key":"ok.txt"}}`, fileURI(okPath), okMeta.ETag),
			strings.TrimSpace(line),
		}, "\n") + "\n"
		stdout, execErr := runSourceMatchReflowCmd(t, strings.NewReader(stream),
			"--stdin",
			"--dest", "s3://dest-bucket/data/",
			"--parallel", "1",
			"--checkpoint", filepath.Join(t.TempDir(), "state.db"),
			"--require-source-match",
		)
		require.Error(t, execErr)
		require.Equal(t, "alpha", string(dstProv.mustObject("data/ok.txt")))
		require.False(t, dstProv.hasObject("data/doc.txt"))

		byDest := map[string]reflowpkg.Record{}
		for _, rec := range parseTypedReflowRecords(t, stdout) {
			if rec.Status != "in_progress" {
				byDest[rec.DestKey] = rec
			}
		}
		require.Equal(t, "complete", byDest["data/ok.txt"].Status)
		require.Equal(t, "failed", byDest["data/doc.txt"].Status)
		require.Equal(t, "replay_unverifiable", byDest["data/doc.txt"].Reason)
	})
}

func TestTransferReflowSourceVersionIDHardeningDualPath(t *testing.T) {
	t1 := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Hour)
	t3 := t2.Add(time.Hour)

	paths := []struct {
		name     string
		extra    []string
		wantPath string
	}{
		{name: "engine", wantPath: reflowpkg.ExecutionPathEngine},
		{name: "cli-pool", extra: poolRoute, wantPath: reflowpkg.ExecutionPathCLIPool},
	}

	for _, tc := range paths {
		t.Run(tc.name+"/whitespace-source-version-id-refused", func(t *testing.T) {
			for _, requireMatch := range []bool{true, false} {
				withTransferReflowTestState(t)
				s3Srv := newFakeS3HTTPServer(t)
				s3Srv.put("a/doc.xml", "live-body", "etag-live", "ver-live", t1)
				srcProv, err := s3.New(context.Background(), s3.Config{
					Bucket: "source-bucket", Region: "us-east-1", Endpoint: s3Srv.server.URL, ForcePathStyle: true, Anonymous: true,
				})
				require.NoError(t, err)
				dstProv := newReflowMemoryProvider()
				useTransferReflowProviderFactories(t, providerdispatch.Factories{
					S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
						if cfg.Bucket == "source-bucket" {
							return srcProv, nil
						}
						return dstProv, nil
					},
				})

				line := `{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/doc.xml","source_key":"a/doc.xml","source_etag":"etag-live","source_version_id":"   ","source_size_bytes":9,"dest_rel_key":"a/doc.xml"}}` + "\n"
				args := []string{"--stdin", "--dest", "s3://dest-bucket/data/", "--parallel", "1", "--checkpoint", filepath.Join(t.TempDir(), "state.db")}
				if requireMatch {
					args = append(args, "--require-source-match")
				}
				args = append(args, tc.extra...)
				stdout, execErr := runSourceMatchReflowCmd(t, strings.NewReader(line), args...)
				require.Error(t, execErr)
				require.False(t, dstProv.hasObject("data/a/doc.xml"))
				if requireMatch {
					require.Contains(t, execErr.Error(), fmt.Sprintf("(exit code %d)", foundry.ExitInvalidArgument))
					require.Contains(t, stdout, `"reason":"replay_unverifiable"`)
					require.Empty(t, dstProv.conditionalPutCallsSnapshot())
				} else {
					recs := parseTypedReflowRecords(t, stdout)
					var terminal reflowpkg.Record
					for _, rec := range recs {
						if rec.Status != "in_progress" {
							terminal = rec
						}
					}
					require.Equal(t, "failed", terminal.Status)
					require.Equal(t, "replay_unverifiable", terminal.Reason)
				}
			}
		})

		t.Run(tc.name+"/head-revision-prevents-rollback-and-preserves-v1-metadata", func(t *testing.T) {
			withTransferReflowTestState(t)
			s3Srv := newFakeS3HTTPServer(t)
			// v1 at t1, overwritten by v2 at t3; dest already holds newer object at t2.
			s3Srv.put("a/rollback.xml", "older-v1", "etag-v1", "ver-1", t1)
			s3Srv.put("a/rollback.xml", "newer-v2", "etag-v2", "ver-2", t3)
			// Key whose latest version is deleted (delete marker) while historical v1 exists.
			s3Srv.put("a/delmarker.xml", "hist-v1", "etag-h1", "ver-h1", t1)
			s3Srv.del("a/delmarker.xml")

			srcProv, err := s3.New(context.Background(), s3.Config{
				Bucket: "source-bucket", Region: "us-east-1", Endpoint: s3Srv.server.URL, ForcePathStyle: true, Anonymous: true,
			})
			require.NoError(t, err)
			dstProv := newReflowMemoryProvider()
			dstProv.putFixture("data/a/rollback.xml", "dest-t2", "dest-etag", t2)
			useTransferReflowProviderFactories(t, providerdispatch.Factories{
				S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
					if cfg.Bucket == "source-bucket" {
						return srcProv, nil
					}
					return dstProv, nil
				},
			})

			input := strings.Join([]string{
				`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/rollback.xml","source_key":"a/rollback.xml","source_etag":"etag-v1","source_version_id":"ver-1","source_size_bytes":8,"dest_rel_key":"a/rollback.xml"}}`,
				`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/delmarker.xml","source_key":"a/delmarker.xml","source_etag":"etag-h1","source_version_id":"ver-h1","source_size_bytes":7,"dest_rel_key":"a/delmarker.xml"}}`,
			}, "\n") + "\n"
			args := append([]string{
				"--stdin", "--dest", "s3://dest-bucket/data/", "--parallel", "1",
				"--checkpoint", filepath.Join(t.TempDir(), "state.db"),
				"--on-collision", "overwrite-if-source-newer",
				"--metadata-policy", "preserve",
				"--require-source-match",
			}, tc.extra...)
			stdout, execErr := runSourceMatchReflowCmd(t, strings.NewReader(input), args...)
			require.NoError(t, execErr)

			byDest := map[string]reflowpkg.Record{}
			for _, rec := range parseTypedReflowRecords(t, stdout) {
				if rec.Status != "in_progress" {
					byDest[rec.DestKey] = rec
				}
			}
			// Rollback prevented: v1@t1 < dest@t2 -> skipped_src_older.
			require.Equal(t, "skipped", byDest["data/a/rollback.xml"].Status)
			require.Equal(t, "collision.skipped_src_older", byDest["data/a/rollback.xml"].Reason)
			require.Equal(t, "dest-t2", string(dstProv.mustObject("data/a/rollback.xml")))

			// Delete-marker latest still copies historical v1 and preserves v1's metadata.
			require.Equal(t, "complete", byDest["data/a/delmarker.xml"].Status)
			require.Equal(t, "hist-v1", string(dstProv.mustObject("data/a/delmarker.xml")))
			dstMeta, headErr := dstProv.Head(context.Background(), "data/a/delmarker.xml")
			require.NoError(t, headErr)
			require.Equal(t, "ver-h1", dstMeta.Metadata["version"])
		})

		t.Run(tc.name+"/equal-timestamp-unknown-size-refuses-tie-breaker", func(t *testing.T) {
			withTransferReflowTestState(t)
			s3Srv := newFakeS3HTTPServer(t)
			s3Srv.put("a/tie.xml", "AAAAA", "etag-v1", "ver-1", t1)
			srcProv, err := s3.New(context.Background(), s3.Config{
				Bucket: "source-bucket", Region: "us-east-1", Endpoint: s3Srv.server.URL, ForcePathStyle: true, Anonymous: true,
			})
			require.NoError(t, err)
			dstProv := newReflowMemoryProvider()
			dstProv.putFixture("data/a/tie.xml", "BBBBB", "dest-etag", t1) // same timestamp t1, same size 5
			useTransferReflowProviderFactories(t, providerdispatch.Factories{
				S3: func(_ context.Context, cfg s3.Config) (provider.Provider, error) {
					if cfg.Bucket == "source-bucket" {
						return srcProv, nil
					}
					return dstProv, nil
				},
			})

			// Omit source_size_bytes while providing source_last_modified == t1:
			// optional HEAD is skipped for version-pinned input, so size is unknown.
			// Equal-timestamp conflict must fail closed (collision.source_size_unavailable)
			// instead of treating unknown 0 != 5 as a size difference.
			line := fmt.Sprintf(`{"type":"gonimbus.reflow.input.v1","data":{"source_uri":"s3://source-bucket/a/tie.xml","source_key":"a/tie.xml","source_etag":"etag-v1","source_version_id":"ver-1","source_last_modified":%q,"dest_rel_key":"a/tie.xml"}}`+"\n", t1.Format(time.RFC3339))
			args := append([]string{
				"--stdin", "--dest", "s3://dest-bucket/data/", "--parallel", "1",
				"--checkpoint", filepath.Join(t.TempDir(), "state.db"),
				"--on-collision", "overwrite-if-source-newer",
				"--require-source-match",
			}, tc.extra...)
			stdout, execErr := runSourceMatchReflowCmd(t, strings.NewReader(line), args...)
			require.Error(t, execErr)
			require.Equal(t, "BBBBB", string(dstProv.mustObject("data/a/tie.xml")), "unknown size must never authorize destructive overwrite")
			recs := parseTypedReflowRecords(t, stdout)
			var terminal reflowpkg.Record
			for _, rec := range recs {
				if rec.Status != "in_progress" {
					terminal = rec
				}
			}
			require.Equal(t, "failed", terminal.Status)
			require.Equal(t, "collision.source_size_unavailable", terminal.Reason)
		})
	}
}
