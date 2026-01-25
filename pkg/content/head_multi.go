package content

import (
	"context"
	"sync"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// HeadBytesMulti reads the first n bytes from many objects in parallel.
//
// Results are sent on the returned channel as they complete.
func HeadBytesMulti(ctx context.Context, p provider.Provider, keys []string, n int64, parallel int) <-chan HeadBytesResult {
	if parallel <= 0 {
		parallel = 4
	}

	out := make(chan HeadBytesResult, parallel)
	work := make(chan string)

	var wg sync.WaitGroup
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range work {
				data, meta, err := HeadBytes(ctx, p, key, n)
				select {
				case out <- HeadBytesResult{Key: key, Meta: meta, Data: data, Err: err}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		defer close(work)
		for _, k := range keys {
			select {
			case work <- k:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
