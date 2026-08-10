package file

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
)

func TestPutObjectConditionalIfAbsent(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	p, err := New(Config{BaseDir: baseDir})
	require.NoError(t, err)

	result, err := p.PutObjectConditional(ctx, "nested/object.txt", strings.NewReader("first"), int64(len("first")), provider.PutPrecondition{IfAbsent: true})
	require.NoError(t, err)
	require.NotEmpty(t, result.ETag)
	require.Empty(t, result.Version)

	_, err = p.PutObjectConditional(ctx, "nested/object.txt", strings.NewReader("second"), int64(len("second")), provider.PutPrecondition{IfAbsent: true})
	require.Error(t, err)
	require.True(t, provider.IsAlreadyExists(err), "got %v", err)

	got, err := os.ReadFile(filepath.Join(baseDir, "nested", "object.txt"))
	require.NoError(t, err)
	require.Equal(t, "first", string(got))
}

func TestPutObjectConditionalRejectsInvalidPrecondition(t *testing.T) {
	ctx := context.Background()
	p, err := New(Config{BaseDir: t.TempDir()})
	require.NoError(t, err)

	_, err = p.PutObjectConditional(ctx, "object.txt", strings.NewReader("payload"), 7, provider.PutPrecondition{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "exactly one put precondition")
}

func TestPutObjectWithOptionsWritesMetadataSidecar(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	p, err := New(Config{BaseDir: baseDir})
	require.NoError(t, err)

	err = p.PutObjectWithOptions(ctx, "nested/object.txt", strings.NewReader("payload"), int64(len("payload")), provider.PutOptions{
		UserMetadata: map[string]string{"owner": "team-a"},
		ContentType:  "text/plain",
		StorageClass: "STANDARD_IA",
	})
	require.NoError(t, err)

	meta, err := p.Head(ctx, "nested/object.txt")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"owner": "team-a"}, meta.Metadata)
	require.Equal(t, "text/plain", meta.ContentType)
	require.Equal(t, "STANDARD_IA", meta.StorageClass)

	raw, err := os.ReadFile(filepath.Join(baseDir, "nested", "object.txt"+DefaultMetadataSidecarSuffix))
	require.NoError(t, err)
	var sidecar map[string]any
	require.NoError(t, json.Unmarshal(raw, &sidecar))
	require.Equal(t, metadataSidecarSchema, sidecar["schema"])
}

func TestPutObjectConditionalWithOptionsWritesMetadataSidecar(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	p, err := New(Config{BaseDir: baseDir})
	require.NoError(t, err)

	_, err = p.PutObjectConditionalWithOptions(ctx, "object.txt", strings.NewReader("payload"), int64(len("payload")), provider.PutPrecondition{IfAbsent: true}, provider.PutOptions{
		UserMetadata: map[string]string{"owner": "team-a"},
		ContentType:  "text/plain",
	})
	require.NoError(t, err)

	meta, err := p.Head(ctx, "object.txt")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"owner": "team-a"}, meta.Metadata)
	require.Equal(t, "text/plain", meta.ContentType)
}

func TestPutObjectClearsExistingMetadataSidecar(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	p, err := New(Config{BaseDir: baseDir})
	require.NoError(t, err)
	require.NoError(t, p.PutObjectWithOptions(ctx, "object.txt", strings.NewReader("payload"), int64(len("payload")), provider.PutOptions{UserMetadata: map[string]string{"owner": "team-a"}}))

	require.NoError(t, p.PutObject(ctx, "object.txt", strings.NewReader("replacement"), int64(len("replacement"))))
	meta, err := p.Head(ctx, "object.txt")
	require.NoError(t, err)
	require.Nil(t, meta.Metadata)
	require.NoFileExists(t, filepath.Join(baseDir, "object.txt"+DefaultMetadataSidecarSuffix))
}

func TestGetObjectVersionedReturnsOpaqueLocalVersion(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	p, err := New(Config{BaseDir: baseDir})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(baseDir, "object.txt"), []byte("payload"), 0o600))

	body, meta, err := p.GetObjectVersioned(ctx, "object.txt")
	require.NoError(t, err)
	defer func() { _ = body.Close() }()

	require.Equal(t, int64(7), meta.Size)
	require.NotEmpty(t, meta.ETag)
}

func TestGetObjectRevisionRefusesChangedLocalFile(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	p, err := New(Config{BaseDir: baseDir})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(baseDir, "object.txt"), []byte("first"), 0o600))

	objects, err := p.List(ctx, provider.ListOptions{})
	require.NoError(t, err)
	require.Len(t, objects.Objects, 1)
	revision := provider.SourceRevision{Kind: provider.RevisionNative, Value: objects.Objects[0].Revision}

	require.NoError(t, os.WriteFile(filepath.Join(baseDir, "object.txt"), []byte("changed-size"), 0o600))
	_, _, err = p.GetObjectRevision(ctx, "object.txt", revision)
	require.ErrorIs(t, err, provider.ErrSourceChanged)
}

func TestListSkipsSymlinksByDefault(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	outsideDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(baseDir, "nested"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(baseDir, "nested", "keep.txt"), []byte("keep"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(outsideDir, "secret.txt"), []byte("secret"), 0o600))
	require.NoError(t, os.Symlink(filepath.Join(outsideDir, "secret.txt"), filepath.Join(baseDir, "nested", "link.txt")))

	p, err := New(Config{BaseDir: baseDir})
	require.NoError(t, err)

	res, err := p.List(ctx, provider.ListOptions{Prefix: ""})
	require.NoError(t, err)
	require.Len(t, res.Objects, 1)
	require.Equal(t, "nested/keep.txt", res.Objects[0].Key)
}

func TestReadMethodsRejectSymlinksByDefault(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	outsideDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outsideDir, "secret.txt"), []byte("secret"), 0o600))
	require.NoError(t, os.Symlink(filepath.Join(outsideDir, "secret.txt"), filepath.Join(baseDir, "link.txt")))
	require.NoError(t, os.Symlink(outsideDir, filepath.Join(baseDir, "alias")))

	p, err := New(Config{BaseDir: baseDir})
	require.NoError(t, err)

	_, err = p.Head(ctx, "link.txt")
	require.ErrorContains(t, err, "symlink")

	_, _, err = p.GetObject(ctx, "link.txt")
	require.ErrorContains(t, err, "symlink")

	_, _, err = p.GetObjectVersioned(ctx, "link.txt")
	require.ErrorContains(t, err, "symlink")

	_, _, err = p.GetRange(ctx, "link.txt", 0, 2)
	require.ErrorContains(t, err, "symlink")

	_, _, err = p.GetObject(ctx, "alias/secret.txt")
	require.ErrorContains(t, err, "symlink")
}

func TestReadMethodsRejectSymlinkedBaseByDefault(t *testing.T) {
	ctx := context.Background()
	parentDir := t.TempDir()
	realDir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(realDir, "sub"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(realDir, "sub", "secret.txt"), []byte("secret"), 0o600))
	require.NoError(t, os.Symlink(realDir, filepath.Join(parentDir, "alias")))

	p, err := New(Config{BaseDir: filepath.Join(parentDir, "alias", "sub")})
	require.NoError(t, err)

	_, err = p.Head(ctx, "secret.txt")
	require.ErrorContains(t, err, "symlink")

	_, _, err = p.GetObject(ctx, "secret.txt")
	require.ErrorContains(t, err, "symlink")

	_, _, err = p.GetObjectVersioned(ctx, "secret.txt")
	require.ErrorContains(t, err, "symlink")

	_, _, err = p.GetRange(ctx, "secret.txt", 0, 2)
	require.ErrorContains(t, err, "symlink")
}

func TestReadMethodsFollowOnlyConfinedSymlinksWhenEnabled(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	outsideDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(baseDir, "nested"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(baseDir, "nested", "target.txt"), []byte("inside"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(outsideDir, "secret.txt"), []byte("outside"), 0o600))
	require.NoError(t, os.Symlink(filepath.Join(baseDir, "nested", "target.txt"), filepath.Join(baseDir, "inside-link.txt")))
	require.NoError(t, os.Symlink(filepath.Join(outsideDir, "secret.txt"), filepath.Join(baseDir, "outside-link.txt")))

	p, err := New(Config{BaseDir: baseDir, SymlinkPolicy: SymlinkPolicyFollow})
	require.NoError(t, err)

	body, size, err := p.GetObject(ctx, "inside-link.txt")
	require.NoError(t, err)
	defer func() { _ = body.Close() }()
	raw, err := io.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, int64(len("inside")), size)
	require.Equal(t, "inside", string(raw))

	_, _, err = p.GetObject(ctx, "outside-link.txt")
	require.ErrorContains(t, err, "escapes base dir")
}

func TestReadMethodsFollowSymlinkedBaseWhenEnabled(t *testing.T) {
	ctx := context.Background()
	parentDir := t.TempDir()
	realDir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(realDir, "sub"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(realDir, "sub", "object.txt"), []byte("payload"), 0o600))
	require.NoError(t, os.Symlink(realDir, filepath.Join(parentDir, "alias")))

	p, err := New(Config{BaseDir: filepath.Join(parentDir, "alias", "sub"), SymlinkPolicy: SymlinkPolicyFollow})
	require.NoError(t, err)

	body, size, err := p.GetObject(ctx, "object.txt")
	require.NoError(t, err)
	defer func() { _ = body.Close() }()
	raw, err := io.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, int64(len("payload")), size)
	require.Equal(t, "payload", string(raw))
}

func TestReadMethodsRejectNonRegularFiles(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(baseDir, "dir"), 0o755))

	p, err := New(Config{BaseDir: baseDir})
	require.NoError(t, err)

	_, _, err = p.GetObject(ctx, "dir")
	require.ErrorContains(t, err, "not a regular file")
}

func TestHeadReturnsIfMatchVersionToken(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	p, err := New(Config{BaseDir: baseDir})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(baseDir, "object.txt"), []byte("first"), 0o600))

	meta, err := p.Head(ctx, "object.txt")
	require.NoError(t, err)
	require.NotEmpty(t, meta.ETag)

	result, err := p.PutObjectConditional(ctx, "object.txt", strings.NewReader("second"), int64(len("second")), provider.PutPrecondition{IfMatchETag: &meta.ETag})
	require.NoError(t, err)
	require.NotEmpty(t, result.ETag)
	require.NotEqual(t, meta.ETag, result.ETag)

	got, err := os.ReadFile(filepath.Join(baseDir, "object.txt"))
	require.NoError(t, err)
	require.Equal(t, "second", string(got))
}

func TestPutObjectConditionalIfMatch(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	p, err := New(Config{BaseDir: baseDir})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(baseDir, "object.txt"), []byte("first"), 0o600))

	body, meta, err := p.GetObjectVersioned(ctx, "object.txt")
	require.NoError(t, err)
	require.NoError(t, body.Close())

	result, err := p.PutObjectConditional(ctx, "object.txt", strings.NewReader("second"), int64(len("second")), provider.PutPrecondition{IfMatchETag: &meta.ETag})
	require.NoError(t, err)
	require.NotEmpty(t, result.ETag)
	require.NotEqual(t, meta.ETag, result.ETag)

	got, err := os.ReadFile(filepath.Join(baseDir, "object.txt"))
	require.NoError(t, err)
	require.Equal(t, "second", string(got))
}

func TestPutObjectConditionalIfMatchRejectsStaleToken(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	p, err := New(Config{BaseDir: baseDir})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(baseDir, "object.txt"), []byte("first"), 0o600))

	body, meta, err := p.GetObjectVersioned(ctx, "object.txt")
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.NoError(t, p.PutObject(ctx, "object.txt", strings.NewReader("other"), int64(len("other"))))

	_, err = p.PutObjectConditional(ctx, "object.txt", strings.NewReader("second"), int64(len("second")), provider.PutPrecondition{IfMatchETag: &meta.ETag})
	require.Error(t, err)
	require.True(t, provider.IsPreconditionFailed(err), "got %v", err)

	got, err := os.ReadFile(filepath.Join(baseDir, "object.txt"))
	require.NoError(t, err)
	require.Equal(t, "other", string(got))
}

func TestPutObjectConditionalIfAbsentConcurrentWriters(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	p, err := New(Config{BaseDir: baseDir})
	require.NoError(t, err)

	const writers = 24
	start := make(chan struct{})
	results := make(chan string, writers)
	errs := make(chan error, writers)
	var wg sync.WaitGroup

	for i := 0; i < writers; i++ {
		payload := strings.Repeat(string(rune('a'+i)), i+1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, putErr := p.PutObjectConditional(ctx, "same-key.txt", strings.NewReader(payload), int64(len(payload)), provider.PutPrecondition{IfAbsent: true})
			if putErr == nil {
				results <- payload
				return
			}
			if !provider.IsAlreadyExists(putErr) {
				errs <- putErr
			}
			results <- ""
		}()
	}

	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	successes := 0
	successPayload := ""
	for payload := range results {
		if payload == "" {
			continue
		}
		successes++
		successPayload = payload
	}
	require.Equal(t, 1, successes)

	got, err := os.ReadFile(filepath.Join(baseDir, "same-key.txt"))
	require.NoError(t, err)
	require.Equal(t, successPayload, string(got))
}

func TestPutObjectConditionalIfMatchConcurrentWriters(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	p, err := New(Config{BaseDir: baseDir})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(baseDir, "same-key.txt"), []byte("initial"), 0o600))

	body, meta, err := p.GetObjectVersioned(ctx, "same-key.txt")
	require.NoError(t, err)
	require.NoError(t, body.Close())

	const writers = 24
	start := make(chan struct{})
	results := make(chan string, writers)
	errs := make(chan error, writers)
	var wg sync.WaitGroup

	for i := 0; i < writers; i++ {
		payload := strings.Repeat(string(rune('a'+i)), i+1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, putErr := p.PutObjectConditional(ctx, "same-key.txt", strings.NewReader(payload), int64(len(payload)), provider.PutPrecondition{IfMatchETag: &meta.ETag})
			if putErr == nil {
				results <- payload
				return
			}
			if !provider.IsPreconditionFailed(putErr) {
				errs <- putErr
			}
			results <- ""
		}()
	}

	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	successes := 0
	successPayload := ""
	for payload := range results {
		if payload == "" {
			continue
		}
		successes++
		successPayload = payload
	}
	require.Equal(t, 1, successes)

	got, err := os.ReadFile(filepath.Join(baseDir, "same-key.txt"))
	require.NoError(t, err)
	require.Equal(t, successPayload, string(got))
}

// blockingReader delivers first chunk, then blocks until unblocked or ctx cancels.
// Used to prove IfAbsent does not expose a final path while the body is still streaming.
type blockingReader struct {
	first    []byte
	sent     bool
	unblock  <-chan struct{}
	progress chan<- struct{}
}

func (r *blockingReader) Read(p []byte) (int, error) {
	if !r.sent {
		r.sent = true
		n := copy(p, r.first)
		if r.progress != nil {
			select {
			case r.progress <- struct{}{}:
			default:
			}
		}
		return n, nil
	}
	<-r.unblock
	return 0, io.EOF
}

func TestPutObjectConditionalIfAbsentNoFinalWhileStreaming(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	p, err := New(Config{BaseDir: baseDir})
	require.NoError(t, err)

	key := "nested/object.txt"
	full := filepath.Join(baseDir, "nested", "object.txt")
	unblock := make(chan struct{})
	progress := make(chan struct{}, 1)
	body := &blockingReader{first: []byte("partial-chunk"), unblock: unblock, progress: progress}

	errCh := make(chan error, 1)
	go func() {
		_, putErr := p.PutObjectConditional(ctx, key, body, -1, provider.PutPrecondition{IfAbsent: true})
		errCh <- putErr
	}()

	select {
	case <-progress:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for mid-stream progress")
	}

	// While body is mid-stream, final must not exist; a reserved temp may exist.
	_, statErr := os.Stat(full)
	require.True(t, os.IsNotExist(statErr), "final path must stay absent mid IfAbsent stream, got %v", statErr)

	entries, err := os.ReadDir(filepath.Join(baseDir, "nested"))
	require.NoError(t, err)
	foundTemp := false
	for _, e := range entries {
		if isReservedIfAbsentTempName(e.Name()) {
			foundTemp = true
			break
		}
	}
	require.True(t, foundTemp, "expected reserved ifabsent temp during mid-stream write")

	// List must not surface the temp as an object key.
	res, err := p.List(ctx, provider.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, res.Objects)

	close(unblock)
	require.NoError(t, <-errCh)

	got, err := os.ReadFile(full)
	require.NoError(t, err)
	require.Equal(t, "partial-chunk", string(got))
}

func TestPutObjectConditionalIfAbsentNoReplacePreservesExisting(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	p, err := New(Config{BaseDir: baseDir})
	require.NoError(t, err)

	key := "object.txt"
	full := filepath.Join(baseDir, key)
	require.NoError(t, os.WriteFile(full, []byte("content-A"), 0o600))

	_, err = p.PutObjectConditional(ctx, key, strings.NewReader("content-B"), int64(len("content-B")), provider.PutPrecondition{IfAbsent: true})
	require.Error(t, err)
	require.True(t, provider.IsAlreadyExists(err), "got %v", err)

	got, err := os.ReadFile(full)
	require.NoError(t, err)
	require.Equal(t, "content-A", string(got))
}

func TestPutObjectConditionalIfAbsentForeignZeroByteStays(t *testing.T) {
	// T−b at provider layer: pre-seeded empty final is foreign; never overwritten.
	ctx := context.Background()
	baseDir := t.TempDir()
	p, err := New(Config{BaseDir: baseDir})
	require.NoError(t, err)

	key := "empty.txt"
	full := filepath.Join(baseDir, key)
	require.NoError(t, os.WriteFile(full, []byte{}, 0o600))

	_, err = p.PutObjectConditional(ctx, key, strings.NewReader("source-bytes"), int64(len("source-bytes")), provider.PutPrecondition{IfAbsent: true})
	require.Error(t, err)
	require.True(t, provider.IsAlreadyExists(err), "got %v", err)

	st, err := os.Stat(full)
	require.NoError(t, err)
	require.Equal(t, int64(0), st.Size())
	got, err := os.ReadFile(full)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestPutObjectConditionalIfAbsentWrongContentUnchanged(t *testing.T) {
	// T−a at provider layer.
	ctx := context.Background()
	baseDir := t.TempDir()
	p, err := New(Config{BaseDir: baseDir})
	require.NoError(t, err)

	key := "wrong.txt"
	full := filepath.Join(baseDir, key)
	require.NoError(t, os.WriteFile(full, []byte("foreign-nonempty"), 0o600))

	_, err = p.PutObjectConditional(ctx, key, strings.NewReader("source"), int64(len("source")), provider.PutPrecondition{IfAbsent: true})
	require.Error(t, err)
	require.True(t, provider.IsAlreadyExists(err), "got %v", err)

	got, err := os.ReadFile(full)
	require.NoError(t, err)
	require.Equal(t, "foreign-nonempty", string(got))
}

func TestListExcludesIfAbsentTempExactShapeOnly(t *testing.T) {
	// T-list-orphan + T-list-filter-exact (exact-shape List filter review).
	ctx := context.Background()
	baseDir := t.TempDir()
	p, err := New(Config{BaseDir: baseDir})
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(filepath.Join(baseDir, "nested"), 0o755))
	// Ordinary keys that must remain listed.
	require.NoError(t, os.WriteFile(filepath.Join(baseDir, "keep.txt"), []byte("k"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(baseDir, "gonimbus-ifabsent-report.csv"), []byte("csv"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(baseDir, "nested", "my-gonimbus-ifabsent-x.tmp"), []byte("near"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(baseDir, "gonimbus-ifabsent-.tmp"), []byte("empty-mid"), 0o600)) // empty mid → not reserved
	// Real reserved orphan temp shapes.
	require.NoError(t, os.WriteFile(filepath.Join(baseDir, "nested", "gonimbus-ifabsent-abc123.tmp"), []byte("orphan"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(baseDir, "gonimbus-ifabsent-9z.tmp"), []byte("orphan2"), 0o600))

	res, err := p.List(ctx, provider.ListOptions{})
	require.NoError(t, err)
	keys := make([]string, 0, len(res.Objects))
	for _, o := range res.Objects {
		keys = append(keys, o.Key)
	}
	require.ElementsMatch(t, []string{
		"keep.txt",
		"gonimbus-ifabsent-report.csv",
		"nested/my-gonimbus-ifabsent-x.tmp",
		"gonimbus-ifabsent-.tmp",
	}, keys)
}

func TestIsReservedIfAbsentTempName(t *testing.T) {
	require.True(t, isReservedIfAbsentTempName("gonimbus-ifabsent-123.tmp"))
	require.True(t, isReservedIfAbsentTempName("nested/gonimbus-ifabsent-xyz.tmp"))
	require.False(t, isReservedIfAbsentTempName("gonimbus-ifabsent-report.csv"))
	require.False(t, isReservedIfAbsentTempName("my-gonimbus-ifabsent-x.tmp"))
	require.False(t, isReservedIfAbsentTempName("gonimbus-ifabsent-.tmp"))
	require.False(t, isReservedIfAbsentTempName("gonimbus-put-123.tmp"))
	require.False(t, isReservedIfAbsentTempName("object.txt"))
}

// TestPutObjectConditionalIfAbsentHardKillNoFinal exercises crash-without-cleanup
// via a subprocess (SIGKILL) after temp progress. Parent asserts final NotFound.
func TestPutObjectConditionalIfAbsentHardKillNoFinal(t *testing.T) {
	if os.Getenv("GONIMBUS_IFABSENT_KILL_HELPER") == "1" {
		runIfAbsentKillHelper()
		return
	}

	baseDir := t.TempDir()
	readyPath := filepath.Join(baseDir, "ready.marker")
	key := "nested/target.bin"

	cmd := exec.Command(os.Args[0], "-test.run=^TestPutObjectConditionalIfAbsentHardKillNoFinal$", "-test.v") // #nosec G204 -- re-exec of current test binary only.
	cmd.Env = append(os.Environ(),
		"GONIMBUS_IFABSENT_KILL_HELPER=1",
		"GONIMBUS_IFABSENT_BASEDIR="+baseDir,
		"GONIMBUS_IFABSENT_KEY="+key,
		"GONIMBUS_IFABSENT_READY="+readyPath,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(readyPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			t.Fatal("timed out waiting for helper temp progress marker")
		}
		// Also accept presence of a reserved temp as progress.
		if temps := listReservedTemps(baseDir); len(temps) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Hard kill without graceful cleanup (no defer).
	require.NoError(t, cmd.Process.Kill())
	_, _ = cmd.Process.Wait()

	full := filepath.Join(baseDir, filepath.FromSlash(key))
	_, err := os.Stat(full)
	require.True(t, os.IsNotExist(err), "final must be absent after hard kill mid IfAbsent, got %v", err)

	p, err := New(Config{BaseDir: baseDir})
	require.NoError(t, err)
	res, err := p.List(context.Background(), provider.ListOptions{})
	require.NoError(t, err)
	for _, o := range res.Objects {
		require.False(t, isReservedIfAbsentTempName(o.Key), "list must not return reserved temp %q", o.Key)
		require.NotEqual(t, key, o.Key)
	}

	// T+ provider-level: same fixture (final absent) → IfAbsent succeeds with source bytes.
	payload := "recovered-source"
	_, err = p.PutObjectConditional(context.Background(), key, strings.NewReader(payload), int64(len(payload)), provider.PutPrecondition{IfAbsent: true})
	require.NoError(t, err)
	got, err := os.ReadFile(full)
	require.NoError(t, err)
	require.Equal(t, payload, string(got))
}

func runIfAbsentKillHelper() {
	baseDir := os.Getenv("GONIMBUS_IFABSENT_BASEDIR")
	key := os.Getenv("GONIMBUS_IFABSENT_KEY")
	readyPath := os.Getenv("GONIMBUS_IFABSENT_READY")
	p, err := New(Config{BaseDir: baseDir})
	if err != nil {
		os.Exit(2)
	}
	// Infinite body after first chunk so parent can kill while temp is open.
	unblock := make(chan struct{}) // never closed in helper
	progress := make(chan struct{}, 1)
	body := &blockingReader{first: []byte("helper-chunk"), unblock: unblock, progress: progress}
	go func() {
		<-progress
		_ = os.WriteFile(readyPath, []byte("1"), 0o600)
	}()
	_, _ = p.PutObjectConditional(context.Background(), key, body, -1, provider.PutPrecondition{IfAbsent: true})
	// If we ever unblock, hang instead of exiting cleanly.
	select {}
}

func listReservedTemps(baseDir string) []string {
	var out []string
	_ = filepath.WalkDir(baseDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(baseDir, path)
		if err != nil {
			return nil
		}
		if isReservedIfAbsentTempName(filepath.ToSlash(rel)) {
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	return out
}
