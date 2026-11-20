package transfer

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/stretchr/testify/require"
)

type receiptSource struct {
	provider.Provider
	reads        int
	revisions    int
	lastModified time.Time
}

func (p *receiptSource) GetObjectVersioned(context.Context, string) (io.ReadCloser, provider.ObjectMeta, error) {
	p.reads++
	return io.NopCloser(strings.NewReader("abcdefgh")), provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{Size: 8, ETag: "src-etag-v1", LastModified: p.lastModified}, Version: "src-ver-1"}, nil
}

func (p *receiptSource) GetObject(context.Context, string) (io.ReadCloser, int64, error) {
	p.reads++
	return io.NopCloser(strings.NewReader("abcdefgh")), 8, nil
}

func (p *receiptSource) GetObjectRevision(ctx context.Context, key string, _ provider.SourceRevision) (io.ReadCloser, provider.ObjectMeta, error) {
	p.revisions++
	return p.GetObjectVersioned(ctx, key)
}

type receiptDestination struct {
	provider.Provider
	receiptPutter
}

type receiptMultipartDestination struct {
	provider.Provider
	uploadMock
}

type receiptGate struct{ stages []string }

func (g *receiptGate) Do(ctx context.Context, stage string, fn func(context.Context) error) error {
	g.stages = append(g.stages, stage)
	return fn(ctx)
}

func TestCopyReceiptPhaseSplitAndRevision(t *testing.T) {
	for _, revision := range []*provider.SourceRevision{nil, {Kind: provider.RevisionNative, Value: "pinned"}} {
		src := &receiptSource{lastModified: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
		dst := &receiptDestination{}
		gate := &receiptGate{}
		result, err := CopyObjectWithReceipt(context.Background(), src, dst, "source", "dest", 8, CopyReceiptOptions{Revision: revision, Gate: gate})
		require.NoError(t, err)
		require.Equal(t, []string{CopyStageSourceRead, CopyStageDestWrite}, gate.stages)
		require.Equal(t, 1, src.reads)
		if revision != nil {
			require.Equal(t, 1, src.revisions)
		}
		require.Equal(t, payloadSHA256("abcdefgh"), result.SHA256)
		require.Equal(t, "returned-version", result.Version)
		if revision != nil {
			require.Equal(t, src.lastModified, result.SourceLastModified)
		} else {
			require.True(t, result.SourceLastModified.IsZero())
		}
		require.Equal(t, "abcdefgh", string(dst.payload))
	}
}

func TestCopyReceiptMultipartGates(t *testing.T) {
	for _, expectedSize := range []int64{0, 8} {
		src := &receiptSource{}
		dst := &receiptMultipartDestination{}
		gate := &receiptGate{}
		result, err := CopyObjectWithReceipt(context.Background(), src, dst, "source", "dest", expectedSize, CopyReceiptOptions{Gate: gate, Upload: UploadOptions{PartSizeBytes: 3, MultipartThreshold: 4, MultipartThresholdSet: true}})
		require.NoError(t, err)
		require.Equal(t, payloadSHA256("abcdefgh"), result.SHA256)
		require.Equal(t, int64(8), result.Bytes)
		require.Equal(t, 1, src.reads)
		if expectedSize == 0 {
			require.Equal(t, []string{CopyStageSourceRead, CopyStageDestWrite}, gate.stages)
		} else {
			require.Equal(t, []string{CopyStageCoupled}, gate.stages)
		}
	}
}

func TestCopyReceiptRejectsMismatchBeforeWrite(t *testing.T) {
	src := &receiptSource{}
	dst := &receiptDestination{}
	result, err := CopyObjectWithReceipt(context.Background(), src, dst, "source", "dest", 10, CopyReceiptOptions{})
	require.Error(t, err)
	require.Equal(t, CopyReceipt{}, result)
	require.Nil(t, dst.payload)
}

func TestCopyReceiptDualTokenExpectedSourceETag(t *testing.T) {
	rev := &provider.SourceRevision{Kind: provider.RevisionNative, Value: "src-ver-1"}

	// Matching ETag (quoted or unquoted) succeeds and populates source receipt fields.
	srcOK := &receiptSource{}
	dstOK := &receiptDestination{}
	receipt, err := CopyObjectWithReceipt(context.Background(), srcOK, dstOK, "source", "dest", 8, CopyReceiptOptions{
		Revision:           rev,
		ExpectedSourceETag: `"src-etag-v1"`,
	})
	require.NoError(t, err)
	require.Equal(t, "abcdefgh", string(dstOK.payload))
	require.Equal(t, "src-etag-v1", receipt.SourceETag)
	require.Equal(t, "src-ver-1", receipt.SourceVersion)

	// Mismatched ETag fails closed with SourceETagMismatchError before any destination write.
	srcBad := &receiptSource{}
	dstBad := &receiptDestination{}
	receipt, err = CopyObjectWithReceipt(context.Background(), srcBad, dstBad, "source", "dest", 8, CopyReceiptOptions{
		Revision:           rev,
		ExpectedSourceETag: "expected-other-etag",
	})
	require.Error(t, err)
	require.True(t, provider.IsSourceChanged(err))
	var mismatch *SourceETagMismatchError
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, "expected-other-etag", mismatch.ExpectedETag)
	require.Equal(t, "src-etag-v1", mismatch.ObservedETag)
	require.Equal(t, "src-ver-1", mismatch.ObservedVersion)
	require.Equal(t, CopyReceipt{}, receipt)
	require.Nil(t, dstBad.payload, "mismatched dual-token ETag must write zero bytes to destination")
}
