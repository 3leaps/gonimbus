package reflow

import (
	"testing"

	"github.com/3leaps/gonimbus/pkg/provider"
	"github.com/stretchr/testify/require"
)

func TestIsDuplicateCollisionRejectsMultipartETagShortcut(t *testing.T) {
	meta := &provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{ETag: `"abc123-2"`, Size: 12}}

	require.False(t, isDuplicateCollision("s3", "s3", `"abc123-2"`, 12, meta))
}

func TestIsDuplicateCollisionAllowsSimpleSameProviderETagShortcut(t *testing.T) {
	meta := &provider.ObjectMeta{ObjectSummary: provider.ObjectSummary{ETag: `"abc123"`, Size: 12}}

	require.True(t, isDuplicateCollision("s3", "s3", `"abc123"`, 12, meta))
}
