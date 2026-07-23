package reflow

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/3leaps/gonimbus/pkg/provider"
)

func validSidecarPlan() ProvenancePlan {
	return ProvenancePlan{
		Mode:         ProvenanceModeSidecar,
		Suffix:       ".gnb.json",
		OnWriteError: ProvenanceOnWriteErrorWarn,
		Placement:    ProvenancePlacementPlan{Mode: ProvenancePlacementSibling},
		RunID:        "run-123",
		ToolVersion:  "gonimbus test",
	}
}

// TestProvenancePlanValidate pins the library-owned pre-I/O validation contract
// : a resolved plan is only admitted when its mode, write
// policy, suffix (with explicit unsafe-suffix confirmation), run identity, and
// mirrored-root authority are all coherent.
func TestProvenancePlanValidate(t *testing.T) {
	t.Run("valid sidecar plan", func(t *testing.T) {
		require.NoError(t, validSidecarPlan().Validate())
	})
	t.Run("mode none is a no-op", func(t *testing.T) {
		require.NoError(t, ProvenancePlan{Mode: ProvenanceModeNone}.Validate())
		require.NoError(t, ProvenancePlan{}.Validate())
	})
	t.Run("invalid mode", func(t *testing.T) {
		p := validSidecarPlan()
		p.Mode = "bogus"
		require.ErrorContains(t, p.Validate(), "provenance mode")
	})
	t.Run("invalid on-write-error", func(t *testing.T) {
		p := validSidecarPlan()
		p.OnWriteError = "explode"
		require.ErrorContains(t, p.Validate(), "on-write-error")
	})
	t.Run("disabled mode still rejects an invalid on-write-error (CLI/plan parity)", func(t *testing.T) {
		// The command adapter rejects an invalid on-write-error even when provenance
		// is disabled; ProvenancePlan.Validate delegates to the same shared validator
		// before the disabled short-circuit so it agrees rather than silently
		// accepting the value.
		require.ErrorContains(t, ProvenancePlan{Mode: ProvenanceModeNone, OnWriteError: "explode"}.Validate(), "on-write-error")
		require.ErrorContains(t, ProvenancePlan{OnWriteError: "explode"}.Validate(), "on-write-error")
	})
	t.Run("unsafe suffix requires explicit confirmation", func(t *testing.T) {
		p := validSidecarPlan()
		p.Suffix = ".xml"
		require.ErrorContains(t, p.Validate(), "unsafe-suffix")
		p.AllowUnsafeSuffix = true
		require.NoError(t, p.Validate())
	})
	t.Run("suffix rules", func(t *testing.T) {
		for _, bad := range []string{"gnb.json", ".a/b", ".a*b"} {
			p := validSidecarPlan()
			p.Suffix = bad
			require.Error(t, p.Validate(), "suffix %q must be rejected", bad)
		}
	})
	t.Run("run identity required", func(t *testing.T) {
		p := validSidecarPlan()
		p.RunID = "  "
		require.ErrorContains(t, p.Validate(), "run_id")
		p = validSidecarPlan()
		p.ToolVersion = ""
		require.ErrorContains(t, p.Validate(), "tool_version")
	})
	t.Run("mirrored-root object-store cross-bucket refused", func(t *testing.T) {
		p := validSidecarPlan()
		p.Placement = ProvenancePlacementPlan{
			Mode:        ProvenancePlacementMirror,
			SidecarRoot: &ProvenanceSidecarRoot{Provider: string(provider.ProviderGCS), Bucket: "other", Prefix: "s/", BaseURI: "gs://other/s/", SameBucketAsDest: false},
		}
		require.ErrorContains(t, p.Validate(), "cross-bucket")
		p.Placement.SidecarRoot.SameBucketAsDest = true
		require.NoError(t, p.Validate())
	})
	t.Run("mirrored-root base_uri must match the declared layout", func(t *testing.T) {
		// Object-store: a base_uri naming a different provider/bucket/prefix than the
		// structured fields is refused pre-I/O, so the run echo cannot publish an
		// authority the sidecar was not written through.
		p := validSidecarPlan()
		p.Placement = ProvenancePlacementPlan{Mode: ProvenancePlacementMirror, SidecarRoot: &ProvenanceSidecarRoot{Provider: string(provider.ProviderS3), Bucket: "b", Prefix: "p/", BaseURI: "s3://other/p/", SameBucketAsDest: true}}
		require.ErrorContains(t, p.Validate(), "base_uri bucket")
		p.Placement.SidecarRoot.BaseURI = "gs://b/p/"
		require.ErrorContains(t, p.Validate(), "base_uri provider")
		p.Placement.SidecarRoot.BaseURI = "s3://b/different/"
		require.ErrorContains(t, p.Validate(), "base_uri prefix")
		p.Placement.SidecarRoot.BaseURI = "s3://b/p/"
		require.NoError(t, p.Validate())

		// File: the entarch mislabeled-file-mirror case — a file root whose base_uri
		// names an object store (or a different base dir) is refused, even though the
		// injected handle passes the structural checks.
		fileProv := newCopyMemoryProvider()
		p = validSidecarPlan()
		p.Placement = ProvenancePlacementPlan{Mode: ProvenancePlacementMirror, SidecarProvider: fileProv, SidecarRoot: &ProvenanceSidecarRoot{Provider: string(provider.ProviderFile), BaseDir: "/mirror", BaseURI: "s3://other/root/"}}
		require.ErrorContains(t, p.Validate(), "base_uri provider")
		p.Placement.SidecarRoot.BaseURI = "file:///elsewhere/"
		require.ErrorContains(t, p.Validate(), "base_uri base dir")
		p.Placement.SidecarRoot.BaseURI = "file:///mirror/"
		require.NoError(t, p.Validate())
	})
	t.Run("mirrored-root requires a resolved root", func(t *testing.T) {
		p := validSidecarPlan()
		p.Placement = ProvenancePlacementPlan{Mode: ProvenancePlacementMirror}
		require.ErrorContains(t, p.Validate(), "resolved sidecar root")
	})
	t.Run("structured authority self-consistency", func(t *testing.T) {
		// Object-store root must carry a bucket and no file base dir.
		p := validSidecarPlan()
		p.Placement = ProvenancePlacementPlan{Mode: ProvenancePlacementMirror, SidecarRoot: &ProvenanceSidecarRoot{Provider: string(provider.ProviderS3), SameBucketAsDest: true}}
		require.ErrorContains(t, p.Validate(), "requires a bucket")
		p.Placement.SidecarRoot = &ProvenanceSidecarRoot{Provider: string(provider.ProviderS3), Bucket: "b", BaseDir: "/x", SameBucketAsDest: true}
		require.ErrorContains(t, p.Validate(), "must not carry a file base dir")

		// File root must carry a base dir, no bucket, and an injected provider.
		p.Placement = ProvenancePlacementPlan{Mode: ProvenancePlacementMirror, SidecarRoot: &ProvenanceSidecarRoot{Provider: string(provider.ProviderFile), Bucket: "leak", BaseDir: "/mirror"}}
		require.ErrorContains(t, p.Validate(), "must not carry a bucket")
		p.Placement = ProvenancePlacementPlan{Mode: ProvenancePlacementMirror, SidecarRoot: &ProvenanceSidecarRoot{Provider: string(provider.ProviderFile), BaseDir: "/mirror"}}
		require.ErrorContains(t, p.Validate(), "requires an injected sidecar provider")
	})
}

// TestRunnerRefusesInvalidProvenancePlanBeforeIO pins that an enabled but invalid
// provenance plan is refused before any stream read, event emission, or
// destination mutation.
func TestRunnerRefusesInvalidProvenancePlanBeforeIO(t *testing.T) {
	src, dst := newCopyMemoryProvider(), newCopyMemoryProvider()
	src.putFixture("a/b.xml", "payload", "etag-a")
	sink := &collectSink{}
	cfg := copyConfig(dst, sink)
	cfg.Provenance = ProvenancePlan{
		Mode:         ProvenanceModeSidecar,
		Suffix:       ".gnb.json",
		OnWriteError: ProvenanceOnWriteErrorWarn,
		// RunID intentionally empty -> invalid.
		ToolVersion: "gonimbus test",
	}
	runner, err := NewRunner(cfg)
	require.NoError(t, err)

	reader := &countingReader{r: strings.NewReader(s3DryRunLine)}
	_, runErr := runner.Run(context.Background(), RecordStreamSource{
		Records: reader,
		Resolve: func(context.Context, string) (provider.Provider, error) { return src, nil },
	})
	require.Error(t, runErr)
	require.Contains(t, runErr.Error(), "run_id")
	require.Zero(t, reader.n, "the record stream must not be read on an invalid provenance plan")
	require.False(t, sink.emitted(), "refusal must precede any event emission")
	require.Empty(t, dst.preconditions(), "no destination probe/mutation on a refused plan")
}
