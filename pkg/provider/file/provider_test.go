package file

import (
	"context"
	"errors"
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
	require.Empty(t, result.ETag)
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

func TestPutObjectConditionalRejectsUnsupportedIfMatch(t *testing.T) {
	ctx := context.Background()
	p, err := New(Config{BaseDir: t.TempDir()})
	require.NoError(t, err)

	etag := "abc123"
	_, err = p.PutObjectConditional(ctx, "object.txt", strings.NewReader("payload"), 7, provider.PutPrecondition{IfMatchETag: &etag})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported put precondition")
	require.False(t, errors.Is(err, provider.ErrAlreadyExists))
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
