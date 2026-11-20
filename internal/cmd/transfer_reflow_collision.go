package cmd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/pkg/provider"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
	"github.com/3leaps/gonimbus/pkg/transfer"
)

func reflowActionForTask(task reflowTask) string {
	if task.RoutingClass == "quarantine" {
		return "quarantined"
	}
	return "landed"
}

func newCollisionInfo(kind string, destMeta *provider.ObjectMeta, decisionPath string) *reflowpkg.CollisionInfo {
	info := &reflowpkg.CollisionInfo{Kind: kind, DecisionPath: decisionPath}
	if destMeta != nil {
		size := destMeta.Size
		info.DestETagObserved = reflowpkg.SafeReceiptHandle(destMeta.ETag)
		info.DestSizeObserved = &size
	}
	return info
}

func newSourceNewerCollisionInfo(kind string, destMeta *provider.ObjectMeta, srcLastModified time.Time, decisionPath string, decisionReason string) *reflowpkg.CollisionInfo {
	info := newCollisionInfo(kind, destMeta, decisionPath)
	if !srcLastModified.IsZero() {
		t := srcLastModified.UTC()
		info.SrcLastModified = &t
	}
	if destMeta != nil && !destMeta.LastModified.IsZero() {
		t := destMeta.LastModified.UTC()
		info.DestLastModifiedObserved = &t
	}
	info.DecisionReason = decisionReason
	return info
}

func recordWithCollision(rec reflowpkg.Record, collision *reflowpkg.CollisionInfo) reflowpkg.Record {
	if collision == nil {
		return rec
	}
	rec.Collision = collision
	return rec
}

func isConditionalExists(err error) bool {
	if provider.IsSourceChanged(err) {
		return false
	}
	return provider.IsAlreadyExists(err) || provider.IsPreconditionFailed(err)
}

func isDuplicateCollision(srcProvider string, destProvider string, srcETag string, srcSize int64, dstMeta *provider.ObjectMeta) bool {
	if !collisionETagsComparable(srcProvider, destProvider) || dstMeta == nil || srcETag == "" || dstMeta.ETag == "" || srcETag != dstMeta.ETag {
		return false
	}
	if isMultipartCollisionETag(srcETag) || isMultipartCollisionETag(dstMeta.ETag) {
		return false
	}
	return srcSize <= 0 || dstMeta.Size <= 0 || srcSize == dstMeta.Size
}

func collisionETagsComparable(srcProvider string, destProvider string) bool {
	srcProvider = strings.TrimSpace(srcProvider)
	if srcProvider == "" {
		srcProvider = string(provider.ProviderS3)
	}
	return srcProvider == destProvider && srcProvider != string(provider.ProviderFile)
}

func isMultipartCollisionETag(etag string) bool {
	etag = strings.Trim(strings.TrimSpace(etag), `"`)
	if etag == "" {
		return false
	}
	idx := strings.LastIndex(etag, "-")
	if idx <= 0 || idx == len(etag)-1 {
		return false
	}
	for _, r := range etag[idx+1:] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func canCompareObjectBodies(src provider.Provider, dst provider.Provider) bool {
	_, srcOK := src.(provider.ObjectGetter)
	_, dstOK := dst.(provider.ObjectGetter)
	return srcOK && dstOK
}

func isDuplicateCollisionForReflow(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey string, dstKey string, sourceProvider string, destProvider string, srcETag string, srcSize int64, dstMeta *provider.ObjectMeta, revision *provider.SourceRevision, expectedSourceETag string) (bool, error) {
	if (revision == nil || revision.Kind != provider.RevisionNative) && isDuplicateCollision(sourceProvider, destProvider, srcETag, srcSize, dstMeta) {
		return true, nil
	}
	if dstMeta == nil {
		return false, nil
	}
	if revision == nil || revision.Kind != provider.RevisionNative {
		if srcSize != dstMeta.Size {
			return false, nil
		}
	} else if srcSize > 0 && srcSize != dstMeta.Size {
		return false, nil
	}
	if revision == nil && !canCompareObjectBodies(src, dst) {
		return false, nil
	}
	if revision != nil {
		if _, dstOK := dst.(provider.ObjectGetter); !dstOK {
			return false, nil
		}
	}
	return objectBodiesEqual(ctx, src, dst, srcKey, dstKey, revision, expectedSourceETag)
}

func objectBodiesEqual(ctx context.Context, src provider.Provider, dst provider.Provider, srcKey string, dstKey string, revision *provider.SourceRevision, expectedSourceETag string) (bool, error) {
	dstGetter, ok := dst.(provider.ObjectGetter)
	if !ok {
		return false, fmt.Errorf("destination provider does not support GetObject")
	}

	var srcBody io.ReadCloser
	var err error
	if revision != nil {
		if err := revision.Validate(); err != nil {
			return false, err
		}
		revGetter, ok := src.(provider.RevisionGetter)
		if !ok {
			return false, provider.ErrReplayUnverifiable
		}
		var meta provider.ObjectMeta
		srcBody, meta, err = revGetter.GetObjectRevision(ctx, srcKey, *revision)
		if err == nil && expectedSourceETag != "" && reflowpkg.NormalizeETagForCompare(meta.ETag) != reflowpkg.NormalizeETagForCompare(expectedSourceETag) {
			_ = srcBody.Close()
			return false, &transfer.SourceETagMismatchError{
				Key:             srcKey,
				ExpectedETag:    reflowpkg.NormalizeETagForCompare(expectedSourceETag),
				ObservedETag:    meta.ETag,
				ObservedVersion: meta.Version,
			}
		}
	} else {
		srcGetter, ok := src.(provider.ObjectGetter)
		if !ok {
			return false, fmt.Errorf("source provider does not support GetObject")
		}
		srcBody, _, err = srcGetter.GetObject(ctx, srcKey)
	}
	if err != nil {
		return false, err
	}
	defer func() { _ = srcBody.Close() }()

	dstBody, _, err := dstGetter.GetObject(ctx, dstKey)
	if err != nil {
		return false, err
	}
	defer func() { _ = dstBody.Close() }()

	srcHash := sha256.New()
	if _, err := io.Copy(srcHash, srcBody); err != nil {
		return false, err
	}

	dstHash := sha256.New()
	if _, err := io.Copy(dstHash, dstBody); err != nil {
		return false, err
	}

	return bytes.Equal(srcHash.Sum(nil), dstHash.Sum(nil)), nil
}
