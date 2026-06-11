package file

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

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
