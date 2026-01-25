package content

import (
	"context"
	"errors"
	"io"

	"github.com/3leaps/gonimbus/pkg/provider"
)

type HeadBytesResult struct {
	Key  string
	Meta *provider.ObjectMeta
	Data []byte
	Err  error
}

type HeadBytesOptions struct {
	Parallel int
	Bytes    int64
}

// HeadBytes reads the first n bytes of an object.
//
// Behavior:
// - Always performs a Head first to capture metadata.
// - Uses GetRange when supported.
// - Falls back to GetObject and reads up to n bytes.
func HeadBytes(ctx context.Context, p provider.Provider, key string, n int64) ([]byte, *provider.ObjectMeta, error) {
	if n < 0 {
		return nil, nil, errors.New("head bytes must be >= 0")
	}

	meta, err := p.Head(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	if n == 0 {
		return nil, meta, nil
	}

	// If object is smaller, only request what exists.
	end := n - 1
	if meta.Size > 0 && meta.Size-1 < end {
		end = meta.Size - 1
	}
	if end < 0 {
		return nil, meta, nil
	}

	if r, ok := interface{}(p).(provider.ObjectRanger); ok {
		body, _, err := r.GetRange(ctx, key, 0, end)
		if err != nil {
			return nil, meta, err
		}
		defer func() { _ = body.Close() }()
		b, err := io.ReadAll(body)
		if err != nil {
			return nil, meta, err
		}
		return b, meta, nil
	}

	g, ok := interface{}(p).(provider.ObjectGetter)
	if !ok {
		return nil, meta, errors.New("provider does not support GetObject")
	}

	body, _, err := g.GetObject(ctx, key)
	if err != nil {
		return nil, meta, err
	}
	defer func() { _ = body.Close() }()

	b, err := io.ReadAll(io.LimitReader(body, n))
	if err != nil {
		return nil, meta, err
	}
	return b, meta, nil
}
