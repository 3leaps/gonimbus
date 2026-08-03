// Package connpolicy holds unexported construction helpers for GON-065 admitted-N
// formulas used when sizing HTTP connection pools. It is internal: not a Stable
// library surface. Helpers refuse overflow rather than wrapping to a smaller
// positive MaxConns under a parallel(N) claim.
package connpolicy

import (
	"fmt"
	"math"
)

// AddChecked returns a+b or an error if the sum overflows int.
func AddChecked(a, b int) (int, error) {
	if b > 0 {
		if a > math.MaxInt-b {
			return 0, fmt.Errorf("admitted-N add overflow: %d + %d", a, b)
		}
	} else if b < 0 {
		if a < math.MinInt-b {
			return 0, fmt.Errorf("admitted-N add underflow: %d + %d", a, b)
		}
	}
	return a + b, nil
}

// MulChecked returns a*b or an error if the product overflows int.
func MulChecked(a, b int) (int, error) {
	if a == 0 || b == 0 {
		return 0, nil
	}
	// Avoid abs of MinInt.
	if a == math.MinInt || b == math.MinInt {
		return 0, fmt.Errorf("admitted-N mul overflow: %d * %d", a, b)
	}
	if a > 0 {
		if b > 0 {
			if a > math.MaxInt/b {
				return 0, fmt.Errorf("admitted-N mul overflow: %d * %d", a, b)
			}
		} else { // b < 0
			if b < math.MinInt/a {
				return 0, fmt.Errorf("admitted-N mul overflow: %d * %d", a, b)
			}
		}
	} else { // a < 0
		if b > 0 {
			if a < math.MinInt/b {
				return 0, fmt.Errorf("admitted-N mul overflow: %d * %d", a, b)
			}
		} else { // b < 0
			if a < math.MaxInt/b {
				return 0, fmt.Errorf("admitted-N mul overflow: %d * %d", a, b)
			}
		}
	}
	return a * b, nil
}

// TransferSourceAdmittedN is the as-built shared-source ceiling for non-reflow
// transfer: list workers (L) plus transfer workers (C), and an additional L when
// sharded discovery can overlap regular listing.
//
// Callers must resolve defaults so C and L are positive before calling.
func TransferSourceAdmittedN(concurrency, listConcurrency int, shardingEnabled bool) (int, error) {
	if concurrency < 1 {
		return 0, fmt.Errorf("transfer concurrency must be >= 1, got %d", concurrency)
	}
	if listConcurrency < 1 {
		return 0, fmt.Errorf("list concurrency must be >= 1, got %d", listConcurrency)
	}
	if !shardingEnabled {
		return AddChecked(concurrency, listConcurrency)
	}
	twoL, err := MulChecked(listConcurrency, 2)
	if err != nil {
		return 0, err
	}
	return AddChecked(concurrency, twoL)
}

// TransferDestAdmittedN is the destination-client ceiling (transfer workers only).
func TransferDestAdmittedN(concurrency int) (int, error) {
	if concurrency < 1 {
		return 0, fmt.Errorf("transfer concurrency must be >= 1, got %d", concurrency)
	}
	return concurrency, nil
}

// ContentWorkerPlusEnumerator returns workers, or workers+1 when a prefix/glob
// enumerator may LIST on the same client while workers run.
func ContentWorkerPlusEnumerator(workers int, enumeratorConcurrent bool) (int, error) {
	if workers < 1 {
		return 0, fmt.Errorf("content concurrency must be >= 1, got %d", workers)
	}
	if !enumeratorConcurrent {
		return workers, nil
	}
	return AddChecked(workers, 1)
}
