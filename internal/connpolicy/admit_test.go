package connpolicy

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddChecked(t *testing.T) {
	t.Parallel()
	sum, err := AddChecked(16, 16)
	require.NoError(t, err)
	require.Equal(t, 32, sum)

	_, err = AddChecked(math.MaxInt, 1)
	require.Error(t, err)

	_, err = AddChecked(0, 1)
	require.Error(t, err)
	_, err = AddChecked(1, 0)
	require.Error(t, err)
	_, err = AddChecked(-1, 2)
	require.Error(t, err)
}

func TestMulCheckedPositiveDomain(t *testing.T) {
	t.Parallel()
	prod, err := MulChecked(16, 2)
	require.NoError(t, err)
	require.Equal(t, 32, prod)

	_, err = MulChecked(math.MaxInt, 2)
	require.Error(t, err)

	// Out of positive domain — not a silent wrap.
	_, err = MulChecked(math.MinInt, 1)
	require.Error(t, err)
	_, err = MulChecked(1, math.MinInt)
	require.Error(t, err)
	_, err = MulChecked(0, 5)
	require.Error(t, err)
	_, err = MulChecked(-3, 2)
	require.Error(t, err)
}

func TestTransferSourceAdmittedN(t *testing.T) {
	t.Parallel()

	n, err := TransferSourceAdmittedN(16, 16, false)
	require.NoError(t, err)
	require.Equal(t, 32, n)

	n, err = TransferSourceAdmittedN(16, 16, true)
	require.NoError(t, err)
	require.Equal(t, 48, n)

	_, err = TransferSourceAdmittedN(math.MaxInt, 1, false)
	require.Error(t, err)

	// C + 2*L overflow path when sharding enabled.
	half := math.MaxInt/2 + 1
	_, err = TransferSourceAdmittedN(half, half, true)
	require.Error(t, err)

	_, err = TransferSourceAdmittedN(0, 16, false)
	require.Error(t, err)
	_, err = TransferSourceAdmittedN(16, 0, false)
	require.Error(t, err)
}

func TestTransferDestAdmittedN(t *testing.T) {
	t.Parallel()
	n, err := TransferDestAdmittedN(16)
	require.NoError(t, err)
	require.Equal(t, 16, n)
	_, err = TransferDestAdmittedN(0)
	require.Error(t, err)
}

func TestContentWorkerPlusEnumerator(t *testing.T) {
	t.Parallel()
	n, err := ContentWorkerPlusEnumerator(16, false)
	require.NoError(t, err)
	require.Equal(t, 16, n)

	n, err = ContentWorkerPlusEnumerator(16, true)
	require.NoError(t, err)
	require.Equal(t, 17, n)

	_, err = ContentWorkerPlusEnumerator(math.MaxInt, true)
	require.Error(t, err)

	_, err = ContentWorkerPlusEnumerator(0, false)
	require.Error(t, err)
}
