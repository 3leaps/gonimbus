package transfer

import (
	"bytes"
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/provider"
)

type stageMemProvider struct {
	mu      sync.Mutex
	objects map[string][]byte
	gets    atomic.Int64
	puts    atomic.Int64
}

func (p *stageMemProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return &provider.ListResult{}, nil
}

func (p *stageMemProvider) Head(_ context.Context, key string) (*provider.ObjectMeta, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	data, ok := p.objects[key]
	if !ok {
		return nil, provider.ErrNotFound
	}
	return &provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Key: key, Size: int64(len(data))}}, nil
}

func (p *stageMemProvider) Close() error { return nil }

func (p *stageMemProvider) GetObject(_ context.Context, key string) (io.ReadCloser, int64, error) {
	p.gets.Add(1)
	p.mu.Lock()
	data, ok := p.objects[key]
	if !ok {
		p.mu.Unlock()
		return nil, 0, provider.ErrNotFound
	}
	cp := append([]byte(nil), data...)
	p.mu.Unlock()
	// Small artificial delay so overlapping phases are observable under race.
	time.Sleep(2 * time.Millisecond)
	return io.NopCloser(bytes.NewReader(cp)), int64(len(cp)), nil
}

func (p *stageMemProvider) PutObject(_ context.Context, key string, r io.Reader, size int64) error {
	p.puts.Add(1)
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if size >= 0 && int64(len(data)) != size {
		return io.ErrUnexpectedEOF
	}
	time.Sleep(2 * time.Millisecond)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.objects == nil {
		p.objects = map[string][]byte{}
	}
	p.objects[key] = data
	return nil
}

type countingGate struct {
	source atomic.Int64
	dest   atomic.Int64
	// maxConcurrent is peak overlapping Do calls across stages.
	active    atomic.Int64
	maxActive atomic.Int64
}

func (g *countingGate) Do(ctx context.Context, stage string, fn func(context.Context) error) error {
	cur := g.active.Add(1)
	for {
		max := g.maxActive.Load()
		if cur <= max || g.maxActive.CompareAndSwap(max, cur) {
			break
		}
	}
	defer g.active.Add(-1)
	switch stage {
	case CopyStageSourceRead:
		g.source.Add(1)
	case CopyStageDestWrite:
		g.dest.Add(1)
	}
	return fn(ctx)
}

func TestCopyObjectWithGatePhaseSplit(t *testing.T) {
	src := &stageMemProvider{objects: map[string][]byte{"a": []byte("hello-a"), "b": []byte("hello-b")}}
	dst := &stageMemProvider{objects: map[string][]byte{}}
	gate := &countingGate{}

	n, err := CopyObjectWithGate(context.Background(), src, dst, "a", "out-a", 7, 1<<20, provider.PutOptions{}, gate)
	if err != nil {
		t.Fatalf("copy a: %v", err)
	}
	if n != 7 {
		t.Fatalf("bytes=%d", n)
	}
	if gate.source.Load() != 1 || gate.dest.Load() != 1 {
		t.Fatalf("stages source=%d dest=%d", gate.source.Load(), gate.dest.Load())
	}
	if string(dst.objects["out-a"]) != "hello-a" {
		t.Fatalf("dest payload=%q", dst.objects["out-a"])
	}
}

func TestCopyObjectWithGateAllowsPhaseOverlapAcrossCopies(t *testing.T) {
	// Two concurrent copies with a gate that does not serialize stages against
	// each other: peak active stages should reach 2 when source of one overlaps
	// dest of the other (callers that phase-split the real limiter get this).
	src := &stageMemProvider{objects: map[string][]byte{
		"a": bytes.Repeat([]byte("a"), 64),
		"b": bytes.Repeat([]byte("b"), 64),
	}}
	dst := &stageMemProvider{objects: map[string][]byte{}}
	gate := &countingGate{}

	errCh := make(chan error, 2)
	go func() {
		_, err := CopyObjectWithGate(context.Background(), src, dst, "a", "out-a", 64, 1<<20, provider.PutOptions{}, gate)
		errCh <- err
	}()
	go func() {
		_, err := CopyObjectWithGate(context.Background(), src, dst, "b", "out-b", 64, 1<<20, provider.PutOptions{}, gate)
		errCh <- err
	}()
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("copy: %v", err)
		}
	}
	if gate.source.Load() != 2 || gate.dest.Load() != 2 {
		t.Fatalf("stages source=%d dest=%d", gate.source.Load(), gate.dest.Load())
	}
	if gate.maxActive.Load() < 2 {
		t.Fatalf("expected overlapping stages, maxActive=%d", gate.maxActive.Load())
	}
}
