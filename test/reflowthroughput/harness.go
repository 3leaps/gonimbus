package reflowthroughput

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	providers3 "github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/3leaps/gonimbus/test/cloudtest"
)

// Options configures a harness invocation.
type Options struct {
	// Binary is the absolute path to the built gonimbus binary.
	Binary string
	// Profile name (empty → smoke).
	Profile string
	// Provider is file (default), moto, s3-compatible, or gcs.
	// BYO cloud uses the same GONIMBUS_S3_TEST_* / GONIMBUS_GCS_TEST_* env
	// conventions as test/cloudtest and make test-cloud-real (opt-in; skip/fail
	// clearly when unset for a non-file provider).
	Provider string
	// RunRoot is the operator-supplied external test root (created if needed).
	RunRoot string
	// GOMEMLIMIT is operator-supplied only; never auto-set by the harness.
	// Under the product's minimum-selection limit chain a GOMEMLIMIT binds
	// only when it is the lowest candidate, so this constrains an envelope
	// rather than raising one.
	GOMEMLIMIT string
	// MemoryBudget is the operator --memory-budget value for arms that set the
	// budget directly. Operator-supplied only; never invented by the harness.
	MemoryBudget string
	// TmpfsCheckpointRoot when set is used for checkpoint_class=tmpfs points.
	// The path itself is never written into the report.
	// Also accepted via env TMPFS_CHECKPOINT_ROOT / GONIMBUS_THROUGHPUT_TMPFS_CHECKPOINT_ROOT.
	TmpfsCheckpointRoot string
	// Keep retains minted roots (report only retained invocation id).
	Keep bool
	// PointTimeout bounds each measured point.
	PointTimeout time.Duration
	// ConstrainedGOMEMLIMIT is the GOMEMLIMIT used by constrained-envelope arms
	// of the ceiling-lift and checkpoint profiles. When empty it falls back to
	// GOMEMLIMIT. Accepted via CONSTRAINED_GOMEMLIMIT, or the older
	// CEILING_LIFT_GOMEMLIMIT spelling.
	ConstrainedGOMEMLIMIT string
	// WorktreeCommit is optional instrument commit override (harness identity).
	// It is NEVER applied to measured BinaryCommit (GON-066 R3).
	WorktreeCommit string
	// ScheduleID selects checkpoint-scale counterbalancing plan
	// (disk_first_odd default; tmpfs_first_odd reverse). Ignored for other profiles.
	ScheduleID string

	// Recipe overrides scale the profile's synthetic corpus at invocation. Zero
	// means keep the profile default. Accepted via OBJECT_COUNT / SIZE_BYTES /
	// PARTITIONS (GONIMBUS_THROUGHPUT_OBJECT_COUNT / _SIZE_BYTES / _PARTITIONS).
	// Absurd values fail closed in Recipe.Validate; the effective corpus is
	// recorded in the report's corpus block so evidence names what was measured.
	RecipeObjectCount int
	RecipeSizeBytes   int
	RecipePartitions  int
}

// applyRecipeOverrides scales a profile's recipe by the operator overrides.
// A zero override keeps the profile default. Negative values are neither applied
// here nor treated as "unset" — resolveRecipe rejects them first so they cannot
// silently fall through to the default.
func applyRecipeOverrides(r Recipe, opts Options) Recipe {
	if opts.RecipeObjectCount > 0 {
		r.ObjectCount = opts.RecipeObjectCount
	}
	if opts.RecipeSizeBytes > 0 {
		r.SizeBytes = opts.RecipeSizeBytes
	}
	if opts.RecipePartitions > 0 {
		r.Partitions = opts.RecipePartitions
	}
	return r
}

// resolveRecipe is the resolved override path: it rejects negative overrides
// (fail closed — a set-negative value is an error, never a silent revert to the
// profile default), applies positive overrides, and validates the result so
// out-of-bounds and oversized-aggregate corpora are rejected before generation.
func resolveRecipe(base Recipe, opts Options) (Recipe, error) {
	for _, o := range []struct {
		name string
		val  int
	}{
		{"OBJECT_COUNT", opts.RecipeObjectCount},
		{"SIZE_BYTES", opts.RecipeSizeBytes},
		{"PARTITIONS", opts.RecipePartitions},
	} {
		if o.val < 0 {
			return Recipe{}, fmt.Errorf("%s override %d must be >= 0 (0 = profile default)", o.name, o.val)
		}
	}
	r := applyRecipeOverrides(base, opts)
	if err := r.Validate(); err != nil {
		return Recipe{}, err
	}
	return r, nil
}

// pointRun declares one measured point. Named fields rather than positional
// arguments: the run carries four independently meaningful strings (shape,
// checkpoint class, GOMEMLIMIT, memory budget) that are otherwise easy to
// transpose silently.
type pointRun struct {
	Parallel         int
	ProbeConcurrency int
	CheckpointClass  string
	Shape            string
	GOMEMLIMIT       string
	MemoryBudget     string
	MemoryEnvelope   string
}

// resolvedArm is a MemoryArm with its operator-supplied values filled in.
type resolvedArm struct {
	Label        string
	GOMEMLIMIT   string
	MemoryBudget string
}

// resolveMemoryArms binds each declared arm to operator-supplied values. A
// profile that declares no arms runs one unlabeled arm that still carries any
// generic GOMEMLIMIT / memory budget the operator supplied: dropping them
// would run a different envelope than the caller asked for, which is the same
// evidence failure the labeled arms exist to prevent.
func resolveMemoryArms(spec ProfileSpec, opts Options) []resolvedArm {
	if len(spec.MemoryArms) == 0 {
		return []resolvedArm{{
			Label:        "",
			GOMEMLIMIT:   opts.GOMEMLIMIT,
			MemoryBudget: opts.MemoryBudget,
		}}
	}
	constrained := firstNonEmpty(opts.ConstrainedGOMEMLIMIT, opts.GOMEMLIMIT)
	out := make([]resolvedArm, 0, len(spec.MemoryArms))
	for _, arm := range spec.MemoryArms {
		r := resolvedArm{Label: arm.Label}
		if arm.UseGOMEMLIMIT {
			r.GOMEMLIMIT = constrained
		}
		if arm.UseMemoryBudget {
			r.MemoryBudget = opts.MemoryBudget
		}
		out = append(out, r)
	}
	return out
}

// requireMemoryArmInputs refuses a profile whose declared arms need operator
// values that were not supplied. The harness never invents a memory envelope:
// silently dropping an arm would publish a report whose arm set does not match
// its profile.
func requireMemoryArmInputs(spec ProfileSpec, opts Options) error {
	for _, arm := range spec.MemoryArms {
		if arm.UseGOMEMLIMIT && strings.TrimSpace(firstNonEmpty(opts.ConstrainedGOMEMLIMIT, opts.GOMEMLIMIT)) == "" {
			return fmt.Errorf("profile %s arm %s requires operator-supplied GOMEMLIMIT or CONSTRAINED_GOMEMLIMIT (the harness never sets one)", spec.Name, arm.Label)
		}
		if arm.UseMemoryBudget && strings.TrimSpace(opts.MemoryBudget) == "" {
			return fmt.Errorf("profile %s arm %s requires operator-supplied MEMORY_BUDGET (the harness never sets one)", spec.Name, arm.Label)
		}
	}
	return nil
}

// pointMeasurement is everything one completed point contributes to its report
// row: the arm configuration it was asked to run under, the timings and counts
// observed while running it, and the product output parsed from that run. Named
// fields rather than positional arguments for the same reason pointRun uses
// them — the row carries many independently meaningful strings, counts, and
// rates that are otherwise easy to transpose silently.
type pointMeasurement struct {
	PointID          string
	Shape            string
	Parallel         int
	ProbeConcurrency int
	CheckpointClass  string
	GOMEMLIMIT       string
	MemoryBudget     string
	MemoryEnvelope   string
	NoAdaptive       bool

	ElapsedSeconds         float64
	CompletedObjects       int64
	EndToEndRate           float64
	ProbeDeliveredRate     float64
	ProbeSaturationRate    float64
	TapValidRows           int64
	TapCopyIntervalSeconds float64

	HonestyOK      *bool
	HonestyMessage string

	StageExitCodes map[string]int

	// Parsed is the product's own reported telemetry for this point, as
	// extracted from its stdout. It is empty for probe_drain, which runs no
	// reflow.
	Parsed ParsedReflowOutput
}

// buildPointReport assembles one report row from a completed measurement. It is
// the measuring path's only PointReport construction site, and the only place a
// point's product-reported telemetry — including the memory provenance tuple —
// is populated.
//
// It is not the only place a row can acquire a value. Rows are still amended
// after construction with harness-derived fields (content parity, occupancy),
// and the parser that produces the ParsedReflowOutput consumed here could
// synthesize upstream of it. So this covers the construction step, not every
// route by which a report could come to carry something the product never
// reported; the controls assert against the parser as well as the builder.
//
// It is a package-level function rather than a closure inside Run so that this
// construction step is directly testable: a test can drive the real builder
// with a real parse of product records that withhold a value, and observe what
// the builder does with the absence.
func buildPointReport(m pointMeasurement) PointReport {
	pt := PointReport{
		PointID:                m.PointID,
		ExecutionShape:         m.Shape,
		ProbeConcurrency:       m.ProbeConcurrency,
		GOMEMLIMITSet:          m.GOMEMLIMIT != "",
		GOMEMLIMITValue:        m.GOMEMLIMIT,
		MemoryBudgetRequested:  m.MemoryBudget,
		MemoryEnvelope:         m.MemoryEnvelope,
		ElapsedSeconds:         m.ElapsedSeconds,
		CompletedObjects:       m.CompletedObjects,
		EndToEndRate:           m.EndToEndRate,
		ProbeDeliveredRate:     m.ProbeDeliveredRate,
		ProbeSaturationRate:    m.ProbeSaturationRate,
		TapValidRows:           m.TapValidRows,
		TapCopyIntervalSeconds: m.TapCopyIntervalSeconds,
		HonestyOK:              m.HonestyOK,
		HonestyMessage:         m.HonestyMessage,
		StageExitCodes:         m.StageExitCodes,
	}
	if m.Shape != "probe_drain" {
		pt.Parallel = m.Parallel
		pt.CheckpointClass = m.CheckpointClass
		// Memory provenance exactly as the product reported it. A missing
		// source is a failure the report validator must catch, not
		// something to paper over with a placeholder label.
		pt.MemoryLimitSource = m.Parsed.MemoryLimitSource
		pt.MemoryBudgetSource = m.Parsed.MemoryBudgetSource
		pt.MemoryLimitBytes = m.Parsed.MemoryLimitBytes
		pt.MemoryBudgetEffectiveBytes = m.Parsed.MemoryBudgetEffectiveBytes
		pt.RetryBufferCapBytes = m.Parsed.RetryBufferCapBytes
		pt.ConcurrencyTimeAvgActive = m.Parsed.SummaryTimeAvgActive
		adaptiveMode := "adaptive"
		if m.NoAdaptive {
			adaptiveMode = "fixed"
		}
		pt.AdaptiveMode = adaptiveMode
		pt.ConcurrencyRequested = intPtr(m.Parsed.Requested)
		pt.ConcurrencyEffective = intPtr(m.Parsed.Effective)
		pt.ConcurrencyReason = strPtr(m.Parsed.Reason)
		pt.ConcurrencyMaxActive = intPtr(m.Parsed.MaxActive)
		pt.ConcurrencyFinal = intPtr(m.Parsed.Final)
		pt.AdaptiveEnabled = boolPtrVal(m.Parsed.AdaptiveEnabled)
		pt.CheckpointWriterStats = m.Parsed.CheckpointWriterStats
	}
	return pt
}

func joinCleanup(runErr, cleanupErr error) error {
	if cleanupErr == nil {
		return runErr
	}
	// Join rather than replace: a cleanup failure must never hide why the run
	// failed, and a run that succeeded but left residue is not a success.
	return errors.Join(runErr, fmt.Errorf("cleanup: %w", cleanupErr))
}

// Run executes the named profile and returns a sanitized report.
func Run(ctx context.Context, opts Options) (report Report, runErr error) {
	spec, err := ResolveProfile(opts.Profile)
	if err != nil {
		return Report{}, err
	}
	spec.Recipe, err = resolveRecipe(spec.Recipe, opts)
	if err != nil {
		return Report{}, fmt.Errorf("recipe override: %w", err)
	}
	if err := requireMemoryArmInputs(spec, opts); err != nil {
		return Report{}, err
	}
	if opts.Binary == "" {
		return Report{}, fmt.Errorf("binary path is required")
	}
	absBin, err := filepath.Abs(opts.Binary)
	if err != nil {
		return Report{}, err
	}
	if _, err := os.Stat(absBin); err != nil {
		return Report{}, fmt.Errorf("binary: %w", err)
	}
	binSHA, err := HashFile(absBin)
	if err != nil {
		return Report{}, err
	}
	// Capture version/commit from the measured binary only. Unknown stays unknown —
	// never backfill BinaryCommit from harness HEAD / WorktreeCommit (GON-066 R3).
	binVer, binCommit := probeBinaryIdentity(ctx, absBin)
	binCommit = NormalizeMeasuredBinaryCommit(binCommit)

	// Instrument = harness process (this test binary content) + worktree commit/dirty.
	// Capture once before arms; re-probe after last arm and fail if either mutates
	// (GON-066 R3 — fail-closed provenance, not omitempty false clean).
	instSHA, instCommit, instDirty, err := captureInstrumentIdentity(opts.WorktreeCommit)
	if err != nil {
		return Report{}, err
	}

	if opts.RunRoot == "" {
		return Report{}, fmt.Errorf("run root is required")
	}
	if err := os.MkdirAll(opts.RunRoot, 0o755); err != nil {
		return Report{}, err
	}
	if resolved, err := filepath.EvalSymlinks(opts.RunRoot); err == nil {
		opts.RunRoot = resolved
	}
	if opts.TmpfsCheckpointRoot == "" {
		opts.TmpfsCheckpointRoot = firstNonEmpty(
			os.Getenv("GONIMBUS_THROUGHPUT_TMPFS_CHECKPOINT_ROOT"),
			os.Getenv("TMPFS_CHECKPOINT_ROOT"),
		)
	}
	if opts.PointTimeout <= 0 {
		opts.PointTimeout = 10 * time.Minute
	}

	invID, err := randomID(8)
	if err != nil {
		return Report{}, err
	}

	// Invocation-scoped subdirectory under operator RUN_ROOT (no overwrite of prior keep runs).
	invRoot := filepath.Join(opts.RunRoot, "inv-"+invID)
	if err := os.MkdirAll(invRoot, 0o755); err != nil {
		return Report{}, err
	}
	if resolved, err := filepath.EvalSymlinks(invRoot); err == nil {
		invRoot = resolved
	}
	opts.RunRoot = invRoot

	if hasTmpfs(spec) && strings.TrimSpace(opts.TmpfsCheckpointRoot) == "" {
		return Report{}, fmt.Errorf("profile %s requires TMPFS_CHECKPOINT_ROOT or GONIMBUS_THROUGHPUT_TMPFS_CHECKPOINT_ROOT (tmpfs class path; never reported)", spec.Name)
	}

	providerClass, err := ResolveProviderClass(opts.Provider)
	if err != nil {
		return Report{}, err
	}

	corpus, err := Generate(GenerateOptions{Recipe: spec.Recipe, RunRoot: opts.RunRoot})
	if err != nil {
		return Report{}, fmt.Errorf("generate corpus: %w", err)
	}

	// BYO S3-compatible (same env lane as cloudtest / make test-cloud-real).
	var (
		byoS3          BYOS3Config
		s3Prov         *providers3.Provider
		s3SourcePrefix string
		s3InputPath    string
		s3ExtraArgs    []string
	)
	// Register provider close before ledger cleanup so LIFO defer order keeps
	// the provider usable while minted prefixes are deleted and verified.
	defer func() {
		if s3Prov != nil {
			_ = s3Prov.Close()
		}
	}()
	// Ownership ledger of everything this run mints. Installed before the
	// provider switch below, which mints the source prefix and then uploads to
	// it: registering ownership only after a successful upload leaves the whole
	// upload window uncovered, and the mint and the first PUT are separated by
	// an entire corpus.
	ledger := &CleanupLedger{Keep: opts.Keep}
	cleanup := ledger.Run

	// The error is surfaced rather than discarded. A cleanup that silently
	// failed would leave residue in a bucket while the run reported success,
	// which is the condition this exists to prevent.
	defer func() {
		runErr = joinCleanup(runErr, cleanup())
	}()

	switch providerClass {
	case ProviderS3Compatible:
		var ok bool
		byoS3, ok = LoadBYOS3Config()
		if !ok {
			return Report{}, fmt.Errorf("provider %s requires %s (same BYO opt-in as make test-cloud-real / test/cloudtest)", providerClass, cloudtest.RealS3BucketEnv)
		}
		s3Prov, err = OpenS3Provider(ctx, byoS3)
		if err != nil {
			return Report{}, fmt.Errorf("open BYO S3 provider: %w", err)
		}
		ledger.DeletePrefix = func(c context.Context, prefix string) error {
			return DeleteS3PrefixVerified(c, s3Prov, prefix)
		}
		s3InputPath = filepath.Join(opts.RunRoot, "reflow.input.s3.jsonl")
		s3SourcePrefix, err = PrepareSourcePrefix(ledger,
			func() string { return byoS3.MintUniquePrefix("src-" + invID[:8]) },
			func(prefix string) error {
				return UploadCorpusToS3(ctx, s3Prov, byoS3, corpus, prefix, s3InputPath)
			})
		if err != nil {
			return Report{}, fmt.Errorf("upload synthetic corpus to BYO prefix: %w", err)
		}
		s3ExtraArgs = CLIProviderFlags(byoS3)
	case ProviderGCS:
		// Explicit opt-in required; full GCS upload path shares cloudtest env constants.
		if strings.TrimSpace(os.Getenv(cloudtest.RealGCSBucketEnv)) == "" {
			return Report{}, fmt.Errorf("provider gcs requires %s (same BYO opt-in as make test-cloud-real / test/cloudtest)", cloudtest.RealGCSBucketEnv)
		}
		return Report{}, fmt.Errorf("provider gcs: BYO env present but gcs reflow harness path is not yet implemented for this cut — use s3-compatible or file (do not claim gcs as runnable)")
	case ProviderMoto:
		if !MotoAvailable() {
			return Report{}, fmt.Errorf("provider moto: moto not reachable (make moto-start; same as test-cloud lane)")
		}
		// Moto reuses cloudtest endpoint/credentials + CreateBucket pattern.
		motoBucket := fmt.Sprintf("gnb-tp-%s", invID[:12])
		if err := CreateMotoBucket(ctx, motoBucket); err != nil {
			return Report{}, fmt.Errorf("create moto bucket: %w", err)
		}
		byoS3 = BYOS3Config{
			Bucket:          motoBucket,
			Endpoint:        cloudtest.Endpoint,
			Region:          cloudtest.Region,
			ForcePathStyle:  true,
			AccessKeyID:     cloudtest.TestAccessKeyID,
			SecretAccessKey: cloudtest.TestSecretAccessKey,
		}
		s3Prov, err = OpenS3Provider(ctx, byoS3)
		if err != nil {
			return Report{}, fmt.Errorf("open moto provider: %w", err)
		}
		// Same ownership lifecycle as the s3-compatible lane. Without an
		// installed DeletePrefix the ledger holds registrations it cannot act
		// on, so neither the source nor the destinations are ever removed;
		// registering the source before the first PUT is what covers the
		// upload window itself.
		ledger.DeletePrefix = func(c context.Context, prefix string) error {
			return DeleteS3PrefixVerified(c, s3Prov, prefix)
		}
		s3InputPath = filepath.Join(opts.RunRoot, "reflow.input.s3.jsonl")
		s3SourcePrefix, err = PrepareSourcePrefix(ledger,
			func() string { return byoS3.MintUniquePrefix("src-" + invID[:8]) },
			func(prefix string) error {
				return UploadCorpusToS3(ctx, s3Prov, byoS3, corpus, prefix, s3InputPath)
			})
		if err != nil {
			return Report{}, fmt.Errorf("upload corpus to moto: %w", err)
		}
		s3ExtraArgs = CLIProviderFlags(byoS3)
		providerClass = ProviderMoto
	}

	report = NewReport(spec.Name, providerClass, invID, binSHA, corpus.Manifest.Compact(), opts.Keep)
	report.BinaryVersion = binVer
	report.BinaryCommit = binCommit
	report.InstrumentCommit = instCommit
	report.InstrumentSHA256 = instSHA
	report.InstrumentDirty = instDirty
	report.OS = runtime.GOOS
	report.Arch = runtime.GOARCH

	pointOrdinal := 0
	// lastDestDir is the local destination of the most recent reflow/full_pipe point
	// (for content-parity snapshots).
	var lastDestDir string
	// lastS3DestPrefix is the cloud analogue of lastDestDir, so the A/B parity
	// check has something to compare on an object store.
	var lastS3DestPrefix string
	runPoint := func(rp pointRun) error {
		parallel := rp.Parallel
		probeConc := rp.ProbeConcurrency
		ckClass := rp.CheckpointClass
		shape := rp.Shape
		gomem := rp.GOMEMLIMIT
		memEnvelope := rp.MemoryEnvelope
		pointOrdinal++
		pointID := fmt.Sprintf("%s-p%02d-%s", spec.Name, pointOrdinal, invID[:8])
		lastDestDir = ""

		ckRoot := opts.RunRoot
		if ckClass == "tmpfs" {
			if opts.TmpfsCheckpointRoot == "" {
				return fmt.Errorf("point %s: tmpfs checkpoint class requires TmpfsCheckpointRoot", pointID)
			}
			ckRoot = opts.TmpfsCheckpointRoot
		}
		ckPath := filepath.Join(ckRoot, "ckpt-"+pointID+".db")
		if err := EnsureAbsent(ckPath); err != nil {
			return fmt.Errorf("point %s checkpoint: %w", pointID, err)
		}

		var destURI string
		var destDir string
		var s3DestPrefix string
		var extraArgs []string
		inputPath := corpus.ReflowInputPath
		useCloud := providerClass == ProviderS3Compatible || providerClass == ProviderMoto

		// probe_drain has no destination, so it mints none on any backend.
		if useCloud && shape == "probe_drain" {
			inputPath = s3InputPath
			extraArgs = s3ExtraArgs
		} else if useCloud {
			s3DestPrefix = byoS3.MintUniquePrefix("dst-" + pointID)
			if n, err := CountS3Prefix(ctx, s3Prov, s3DestPrefix); err != nil {
				return fmt.Errorf("point %s dest list: %w", pointID, err)
			} else if n != 0 {
				return fmt.Errorf("point %s: destination prefix not empty", pointID)
			}
			ledger.RegisterDestPrefix(s3DestPrefix)
			lastS3DestPrefix = s3DestPrefix
			destURI = byoS3.ObjectURI(s3DestPrefix)
			extraArgs = s3ExtraArgs
			inputPath = s3InputPath
			ledger.RegisterPoint(MintedPoint{CheckpointPath: ckPath})
		} else if shape != "probe_drain" {
			destDir = filepath.Join(opts.RunRoot, "dest-"+pointID)
			if err := EnsureEmptyDir(destDir); err != nil {
				return fmt.Errorf("point %s dest: %w", pointID, err)
			}
			if resolved, err := filepath.EvalSymlinks(destDir); err == nil {
				destDir = resolved
			}
			destURI = fileURIFromAbs(destDir) + "/"
			lastDestDir = destDir
			ledger.RegisterPoint(MintedPoint{DestDir: destDir, CheckpointPath: ckPath})
		} else {
			// probe_drain: no destination; still track a throwaway checkpoint path absence.
			ledger.RegisterPoint(MintedPoint{CheckpointPath: ckPath})
		}

		pctx, cancel := context.WithTimeout(ctx, opts.PointTimeout)
		defer cancel()

		stdoutPath := filepath.Join(opts.RunRoot, "stdout-"+pointID+".jsonl")

		var pr PointResult
		var runErr error
		switch shape {
		case "full_pipe":
			srcPrefix := fileURIFromAbs(corpus.Root) + "/"
			var probeArgs []string
			var rewriteFrom string
			if useCloud {
				srcPrefix = byoS3.ObjectURI(s3SourcePrefix)
				probeArgs = CLIProbeProviderFlags(byoS3)
				// Probe emits the full object key, which on an object store is
				// prefixed by the minted run prefix. The bare four-segment
				// template matches none of them, so carry the prefix as literal
				// leading segments and map back to the bare key at the
				// destination.
				rewriteFrom = strings.TrimSuffix(s3SourcePrefix, "/") + "/" + defaultHiveRewrite
			}
			pr, runErr = RunFullPipe(pctx, FullPipeOpts{
				Binary:          absBin,
				SourcePrefix:    srcPrefix,
				ProbeConfig:     corpus.ProbeConfigPath,
				DestURI:         destURI,
				ProbeConc:       probeConc,
				ReflowParallel:  parallel,
				CheckpointPath:  ckPath,
				GOMEMLIMIT:      gomem,
				MemoryBudget:    rp.MemoryBudget,
				StdoutPath:      stdoutPath,
				ProviderClass:   providerClass,
				ProbeExtraArgs:  probeArgs,
				ReflowExtraArgs: extraArgs,
				ChildExtraEnv:   byoS3.ChildAWSEnv(),
				RewriteFrom:     rewriteFrom,
			})
		case "probe_drain":
			srcPrefix := fileURIFromAbs(corpus.Root) + "/"
			var probeArgs []string
			if useCloud {
				srcPrefix = byoS3.ObjectURI(s3SourcePrefix)
				probeArgs = CLIProbeProviderFlags(byoS3)
			}
			pr, runErr = RunProbeDrain(pctx, ProbeDrainOpts{
				Binary:         absBin,
				SourcePrefix:   srcPrefix,
				ProbeConfig:    corpus.ProbeConfigPath,
				ProbeConc:      probeConc,
				GOMEMLIMIT:     gomem,
				ProviderClass:  providerClass,
				ProbeExtraArgs: probeArgs,
				ChildExtraEnv:  byoS3.ChildAWSEnv(),
			})
		default:
			childEnv := byoS3.ChildAWSEnv()
			pr, runErr = RunReflowOnly(pctx, StageRunOpts{
				Binary:         absBin,
				InputPath:      inputPath,
				DestURI:        destURI,
				Parallel:       parallel,
				CheckpointPath: ckPath,
				GOMEMLIMIT:     gomem,
				MemoryBudget:   rp.MemoryBudget,
				NoAdaptive:     spec.NoAdaptive,
				ProviderClass:  providerClass,
				ExtraArgs:      extraArgs,
				ChildExtraEnv:  childEnv,
				StdoutPath:     stdoutPath,
			})
		}
		pr.PointID = pointID
		pr.Profile = spec.Name
		pr.CheckpointClass = ckClass
		pr.ProbeConcurrency = probeConc

		// Structural: stage exits
		if runErr != nil {
			detail := ""
			if st, ok := pr.Stages["reflow"]; ok && st.Stderr != "" {
				detail = "; stderr: " + truncate(st.Stderr, 512)
			}
			if st, ok := pr.Stages["probe"]; ok && st.Stderr != "" {
				detail += "; probe_stderr: " + truncate(st.Stderr, 256)
			}
			return fmt.Errorf("point %s: %w%s", pointID, runErr, detail)
		}
		for name, st := range pr.Stages {
			if st.ExitCode != 0 {
				return fmt.Errorf("point %s: stage %s exit %d: %s", pointID, name, st.ExitCode, st.Err)
			}
		}

		var parsed ParsedReflowOutput
		if shape != "probe_drain" {
			if pr.StdoutPath != "" {
				parsed, err = ParseReflowFile(pr.StdoutPath)
			} else {
				parsed, err = ParseReflowStdout(pr.Stdout)
			}
			if err != nil {
				return fmt.Errorf("point %s parse: %w", pointID, err)
			}
		}
		// Object count: local tree walk or S3 list (methodology: post-run count).
		var fileCount int64
		if shape == "probe_drain" {
			fileCount = pr.Tap.ValidReflowInputRows
		} else if useCloud {
			fileCount, err = CountS3Prefix(pctx, s3Prov, s3DestPrefix)
			if err != nil {
				return fmt.Errorf("point %s dest count: %w", pointID, err)
			}
		} else {
			fileCount, err = CountFilesRecursive(destDir)
			if err != nil {
				return fmt.Errorf("point %s dest count: %w", pointID, err)
			}
		}
		if shape != "probe_drain" {
			if fileCount != int64(corpus.ObjectCount) {
				return fmt.Errorf("point %s: dest file count %d != generated %d", pointID, fileCount, corpus.ObjectCount)
			}
			if parsed.ObjectComplete != 0 && parsed.ObjectComplete != fileCount {
				return fmt.Errorf("point %s: summary complete %d != dest file count %d", pointID, parsed.ObjectComplete, fileCount)
			}
		} else {
			fileCount = pr.Tap.ValidReflowInputRows
			if fileCount != int64(corpus.ObjectCount) {
				return fmt.Errorf("point %s: probe-drain rows %d != generated %d", pointID, fileCount, corpus.ObjectCount)
			}
		}
		tapRows := pr.Tap.ValidReflowInputRows
		if shape == "reflow_only" {
			tapRows = 0
		}
		if shape != "probe_drain" {
			if err := CheckCounts(corpus.ObjectCount, tapRows, fileCount, parsed.SummaryErrors, parsed.InvalidInputs); err != nil {
				return fmt.Errorf("point %s counts: %w", pointID, err)
			}
		}

		var honesty HonestyResult
		var honestyOK *bool
		if shape == "probe_drain" {
			// Honesty is not applicable: no reflow concurrency telemetry.
			honesty = HonestyResult{OK: false, Message: ""}
		} else {
			honesty = CheckHonesty(parsed, parallel)
			if !honesty.OK {
				return fmt.Errorf("point %s honesty: %s", pointID, honesty.Message)
			}
			honestyOK = boolPtrVal(true)
			// GON-066 C1: retained checkpoint-scale arms fail closed without
			// exactly one post-summary, structurally valid writer-stats record.
			if profileRequiresCheckpointWriterStats(spec.Name) {
				if err := CheckCheckpointWriterStatsAdmission(parsed); err != nil {
					return fmt.Errorf("point %s checkpoint writer stats: %w", pointID, err)
				}
			}
		}

		elapsedSec := pr.Elapsed.Seconds()
		var rate float64
		if elapsedSec > 0 && shape != "probe_drain" {
			rate = float64(fileCount) / elapsedSec
		}
		var probeRate float64
		var probeSatRate float64
		if shape == "full_pipe" {
			iv := pr.Tap.ActiveInterval().Seconds()
			if iv > 0 {
				probeRate = float64(pr.Tap.ValidReflowInputRows) / iv
			}
		}
		if shape == "probe_drain" {
			iv := pr.Tap.ActiveInterval().Seconds()
			if iv <= 0 {
				iv = elapsedSec
			}
			if iv > 0 {
				probeSatRate = float64(pr.Tap.ValidReflowInputRows) / iv
			}
		}

		stageCodes := map[string]int{}
		for k, v := range pr.Stages {
			stageCodes[k] = v.ExitCode
		}

		pt := buildPointReport(pointMeasurement{
			PointID:                pointID,
			Shape:                  shape,
			Parallel:               parallel,
			ProbeConcurrency:       probeConc,
			CheckpointClass:        ckClass,
			GOMEMLIMIT:             gomem,
			MemoryBudget:           rp.MemoryBudget,
			MemoryEnvelope:         memEnvelope,
			NoAdaptive:             spec.NoAdaptive,
			ElapsedSeconds:         elapsedSec,
			CompletedObjects:       fileCount,
			EndToEndRate:           rate,
			ProbeDeliveredRate:     probeRate,
			ProbeSaturationRate:    probeSatRate,
			TapValidRows:           pr.Tap.ValidReflowInputRows,
			TapCopyIntervalSeconds: pr.Tap.CopyDuration.Seconds(),
			HonestyOK:              honestyOK,
			HonestyMessage:         honesty.Message,
			StageExitCodes:         stageCodes,
			Parsed:                 parsed,
		})
		report.Points = append(report.Points, pt)
		return nil
	}

	// Drive points from profile.
	arms := resolveMemoryArms(spec, opts)
	switch spec.ExecutionShape {
	case "full_pipe":
		// Methodology: A/B canary — each pair runs twice; landed key+size+digest must match.
		// Both arms of a pair share one memory envelope so the comparison
		// isolates run-to-run variation, not the envelope.
		fullPipeArm := arms[0]
		for _, pair := range spec.FullPipePairs {
			armRun := pointRun{
				Parallel:         pair[1],
				ProbeConcurrency: pair[0],
				CheckpointClass:  "disk",
				Shape:            "full_pipe",
				GOMEMLIMIT:       fullPipeArm.GOMEMLIMIT,
				MemoryBudget:     fullPipeArm.MemoryBudget,
				MemoryEnvelope:   fullPipeArm.Label,
			}
			if err := runPoint(armRun); err != nil {
				return report, err
			}
			// A cloud point lands in a minted object prefix, not a local tree, so
			// the snapshot has to follow the destination rather than assume one.
			snapshotLanded := func() ([]LandedObjectID, error) {
				if providerClass == ProviderS3Compatible || providerClass == ProviderMoto {
					return SnapshotS3DestPrefix(ctx, s3Prov, lastS3DestPrefix)
				}
				return SnapshotDestTree(lastDestDir)
			}
			snapA, err := snapshotLanded()
			if err != nil {
				return report, fmt.Errorf("fullpipe arm A snapshot: %w", err)
			}
			if err := runPoint(armRun); err != nil {
				return report, err
			}
			snapB, err := snapshotLanded()
			if err != nil {
				return report, fmt.Errorf("fullpipe arm B snapshot: %w", err)
			}
			if err := CompareLandedMultisets(snapA, snapB); err != nil {
				return report, fmt.Errorf("fullpipe-ab content parity: %w", err)
			}
			ok := true
			report.Points[len(report.Points)-1].ContentParityOK = &ok
			report.Points[len(report.Points)-2].ContentParityOK = &ok
		}
	case "probe_drain":
		for _, pc := range spec.ProbeConcurrencyPoints {
			if err := runPoint(pointRun{ProbeConcurrency: pc, CheckpointClass: "disk", Shape: "probe_drain"}); err != nil {
				return report, err
			}
		}
	default:
		// reflow-only sweeps. checkpoint-scale uses a frozen counterbalanced
		// schedule (warmups discarded); other profiles keep class-outer order.
		if spec.Name == ProfileCheckpointScale {
			schedID := opts.ScheduleID
			if schedID == "" {
				schedID = ScheduleDiskFirstOdd
			}
			steps, err := CheckpointScaleSchedule(spec, schedID)
			if err != nil {
				return report, err
			}
			if err := ValidateCheckpointScaleSchedule(steps); err != nil {
				return report, fmt.Errorf("schedule: %w", err)
			}
			report.ScheduleID = schedID
			order := make([]string, 0, len(steps))
			armByLabel := map[string]resolvedArm{}
			for _, arm := range arms {
				armByLabel[arm.Label] = arm
			}
			for _, step := range steps {
				arm, ok := armByLabel[step.MemoryEnvelope]
				if !ok {
					if len(arms) == 0 {
						return report, fmt.Errorf("schedule step missing memory arm")
					}
					arm = arms[0]
				}
				var tag string
				if step.Warmup {
					tag = fmt.Sprintf("warm:%s:%d", step.CheckpointClass, step.Parallel)
				} else {
					tag = fmt.Sprintf("p%d:%s:%d", step.PairIndex, step.CheckpointClass, step.Parallel)
				}
				order = append(order, tag)
				before := len(report.Points)
				if err := runPoint(pointRun{
					Parallel:        step.Parallel,
					CheckpointClass: step.CheckpointClass,
					Shape:           "reflow_only",
					GOMEMLIMIT:      arm.GOMEMLIMIT,
					MemoryBudget:    arm.MemoryBudget,
					MemoryEnvelope:  step.MemoryEnvelope,
				}); err != nil {
					return report, err
				}
				if step.Warmup {
					if len(report.Points) != before+1 {
						return report, fmt.Errorf("warmup did not append exactly one point")
					}
					report.Points = report.Points[:before]
				}
			}
			report.ScheduleOrder = order
		} else {
			classes := spec.CheckpointClasses
			if len(classes) == 0 {
				classes = []string{"disk"}
			}
			for _, ck := range classes {
				for _, arm := range arms {
					for _, p := range spec.ParallelPoints {
						if err := runPoint(pointRun{
							Parallel:        p,
							CheckpointClass: ck,
							Shape:           "reflow_only",
							GOMEMLIMIT:      arm.GOMEMLIMIT,
							MemoryBudget:    arm.MemoryBudget,
							MemoryEnvelope:  arm.Label,
						}); err != nil {
							return report, err
						}
					}
				}
			}
		}
	}

	// Occupancy sampling for reflow-saturation: one warm-up + 3 samples on a mid point.
	if spec.RequireOccupancy && len(report.Points) > 0 {
		base := report.Points[0]
		baseParallel := base.Parallel
		baseEffective := 0
		if base.ConcurrencyEffective != nil {
			baseEffective = *base.ConcurrencyEffective
		}
		samples := make([]int, 0, 3)
		// Samples reuse the base point's memory envelope so occupancy is not
		// compared across differently bound runs.
		baseArm := arms[0]
		occupancyRun := pointRun{
			Parallel:        baseParallel,
			CheckpointClass: "disk",
			Shape:           "reflow_only",
			GOMEMLIMIT:      baseArm.GOMEMLIMIT,
			MemoryBudget:    baseArm.MemoryBudget,
			MemoryEnvelope:  baseArm.Label,
		}
		beforeWarm := len(report.Points)
		if err := runPoint(occupancyRun); err != nil {
			return report, fmt.Errorf("occupancy warm-up: %w", err)
		}
		if len(report.Points) != beforeWarm+1 {
			return report, fmt.Errorf("occupancy warm-up did not append exactly one point")
		}
		report.Points = report.Points[:beforeWarm]
		for i := 0; i < 3; i++ {
			before := len(report.Points)
			if err := runPoint(occupancyRun); err != nil {
				return report, err
			}
			if len(report.Points) != before+1 {
				return report, fmt.Errorf("occupancy sample %d did not append a point", i)
			}
			pt := report.Points[len(report.Points)-1]
			if pt.ConcurrencyMaxActive == nil {
				return report, fmt.Errorf("occupancy sample missing max_active")
			}
			samples = append(samples, *pt.ConcurrencyMaxActive)
		}
		occ := CheckOccupancy(samples, baseEffective, 2)
		ok := occ.OK
		for i := range report.Points {
			if report.Points[i].Parallel == baseParallel {
				report.Points[i].OccupancySamples = samples
				report.Points[i].OccupancyOK = &ok
				report.Points[i].OccupancyMessage = occ.Message
			}
		}
		if !occ.OK {
			return report, fmt.Errorf("occupancy check failed: %s", occ.Message)
		}
	}

	// Verify corpus immutability by re-hashing source objects + manifest.
	if err := VerifyCorpusImmutable(corpus); err != nil {
		return report, err
	}
	gotSHA, err := HashFile(absBin)
	if err != nil {
		return report, err
	}
	if gotSHA != binSHA {
		return report, fmt.Errorf("binary sha changed during run")
	}
	// Re-probe instrument commit + dirty + content hash; any drift fails the set.
	if err := assertInstrumentIdentityUnchanged(opts.WorktreeCommit, instSHA, instCommit, instDirty); err != nil {
		return report, err
	}

	if err := ValidateReportEnvelope(report); err != nil {
		return report, err
	}
	if err := ValidateArmMatrix(spec, report); err != nil {
		return report, err
	}

	// Write report under run root (path not embedded in report body beyond write).
	rb, err := MarshalJSONReport(report)
	if err != nil {
		return report, err
	}
	reportPath := filepath.Join(opts.RunRoot, "report.json")
	if err := os.WriteFile(reportPath, rb, 0o644); err != nil {
		return report, err
	}

	if err := cleanup(); err != nil {
		return report, fmt.Errorf("cleanup: %w", err)
	}
	return report, nil
}

func randomID(nBytes int) (string, error) {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func probeBinaryIdentity(ctx context.Context, binary string) (version, commit string) {
	// Product supports `version --extended` (not --json).
	cmd := exec.CommandContext(ctx, binary, "version", "--extended")
	out, err := cmd.Output()
	if err != nil {
		cmd2 := exec.CommandContext(ctx, binary, "version")
		out2, err2 := cmd2.Output()
		if err2 != nil {
			return "", ""
		}
		fields := strings.Fields(string(out2))
		if len(fields) >= 2 {
			return fields[len(fields)-1], ""
		}
		return "", ""
	}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Commit:") {
			commit = strings.TrimSpace(strings.TrimPrefix(line, "Commit:"))
			continue
		}
		// First non-empty line: "<name> <version>"
		if version == "" && line != "" && !strings.Contains(line, ":") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				version = fields[len(fields)-1]
			}
		}
	}
	return version, commit
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func hasTmpfs(spec ProfileSpec) bool {
	for _, c := range spec.CheckpointClasses {
		if c == "tmpfs" {
			return true
		}
	}
	return false
}

func gitHeadShort() (string, error) {
	cmd := exec.Command("git", "rev-parse", "--short", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func gitWorktreeDirty() (bool, error) {
	cmd := exec.Command("git", "status", "--porcelain")
	out, err := cmd.Output()
	if err != nil {
		return false, err
	}
	return len(strings.TrimSpace(string(out))) > 0, nil
}

// captureInstrumentIdentity records harness process content SHA plus Git
// commit/dirty for provenance. Probe failures never become "clean" or empty
// commit that could be admitted as known-good (GON-066 R3).
//
// worktreeCommitOverride, when non-empty, replaces the Git HEAD probe (tests).
func captureInstrumentIdentity(worktreeCommitOverride string) (sha, commit string, dirty bool, err error) {
	sha, err = HashFile(os.Args[0])
	if err != nil {
		return "", "", false, fmt.Errorf("instrument sha: %w", err)
	}
	commit = worktreeCommitOverride
	if commit == "" {
		head, headErr := gitHeadShort()
		if headErr != nil {
			return "", "", false, fmt.Errorf("instrument commit probe: %w", headErr)
		}
		if head == "" {
			return "", "", false, fmt.Errorf("instrument commit probe: empty HEAD")
		}
		commit = head
	}
	dirty, dirtyErr := gitWorktreeDirty()
	if dirtyErr != nil {
		return "", "", false, fmt.Errorf("instrument dirty probe: %w", dirtyErr)
	}
	return sha, commit, dirty, nil
}

// assertInstrumentIdentityUnchanged re-probes instrument identity after the arm
// set and fails if content SHA, commit, or dirty state drifted.
func assertInstrumentIdentityUnchanged(worktreeCommitOverride, wantSHA, wantCommit string, wantDirty bool) error {
	gotSHA, gotCommit, gotDirty, err := captureInstrumentIdentity(worktreeCommitOverride)
	if err != nil {
		return fmt.Errorf("instrument identity re-probe: %w", err)
	}
	if gotSHA != wantSHA {
		return fmt.Errorf("instrument sha changed during run")
	}
	if gotCommit != wantCommit {
		return fmt.Errorf("instrument commit changed during run: %q -> %q", wantCommit, gotCommit)
	}
	if gotDirty != wantDirty {
		return fmt.Errorf("instrument dirty state changed during run: %v -> %v", wantDirty, gotDirty)
	}
	return nil
}

// VerifyCorpusImmutable re-hashes every source object and checks the manifest digest.
func VerifyCorpusImmutable(c GeneratedCorpus) error {
	if err := VerifyManifestUnchanged(c.ManifestPath, c.Manifest.Digest); err != nil {
		return err
	}
	entries := make([]ManifestEntry, 0, len(c.Manifest.Entries))
	for _, e := range c.Manifest.Entries {
		abs := filepath.Join(c.Root, filepath.FromSlash(e.RelativeKey))
		body, err := os.ReadFile(abs) // #nosec G304 -- harness-owned corpus
		if err != nil {
			return fmt.Errorf("re-read corpus object %s: %w", e.RelativeKey, err)
		}
		got := ContentDigest(body)
		if got != e.ContentDigest {
			return fmt.Errorf("corpus object %s content changed: %s vs %s", e.RelativeKey, got, e.ContentDigest)
		}
		if int64(len(body)) != e.SizeBytes {
			return fmt.Errorf("corpus object %s size changed", e.RelativeKey)
		}
		entries = append(entries, ManifestEntry{RelativeKey: e.RelativeKey, SizeBytes: e.SizeBytes, ContentDigest: got})
	}
	if d := DigestManifestEntries(entries); d != c.Manifest.Digest {
		return fmt.Errorf("recomputed corpus digest mismatch")
	}
	return nil
}

// BuildBinary builds ./cmd/gonimbus into outPath and returns absolute path + sha.
func BuildBinary(ctx context.Context, repoRoot, outPath string) (string, string, error) {
	cmd := exec.CommandContext(ctx, "go", "build", "-o", outPath, "./cmd/gonimbus")
	cmd.Dir = repoRoot
	cmd.Env = os.Environ()
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", "", fmt.Errorf("go build: %w\n%s", err, string(out))
	}
	abs, err := filepath.Abs(outPath)
	if err != nil {
		return "", "", err
	}
	sha, err := HashFile(abs)
	if err != nil {
		return "", "", err
	}
	return abs, sha, nil
}
