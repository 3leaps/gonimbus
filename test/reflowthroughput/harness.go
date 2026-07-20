package reflowthroughput

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
	// WorktreeCommit is optional fallback commit identity when the binary
	// reports "unknown" (plain go test builds without ldflags).
	WorktreeCommit string

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
// A zero override keeps the profile default; the result is validated by the
// caller so out-of-bounds values fail closed.
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

// Run executes the named profile and returns a sanitized report.
func Run(ctx context.Context, opts Options) (Report, error) {
	spec, err := ResolveProfile(opts.Profile)
	if err != nil {
		return Report{}, err
	}
	spec.Recipe = applyRecipeOverrides(spec.Recipe, opts)
	if err := spec.Recipe.Validate(); err != nil {
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
	// Capture version/commit from binary; fall back to worktree HEAD for commit.
	binVer, binCommit := probeBinaryIdentity(ctx, absBin)
	if (binCommit == "" || binCommit == "unknown") && opts.WorktreeCommit != "" {
		binCommit = opts.WorktreeCommit
	}
	if binCommit == "" || binCommit == "unknown" {
		if head, err := gitHeadShort(); err == nil && head != "" {
			binCommit = head
		}
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
		s3DestPrefixes []string
	)
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
		defer func() { _ = s3Prov.Close() }()
		s3SourcePrefix = byoS3.MintUniquePrefix("src-" + invID[:8])
		s3InputPath = filepath.Join(opts.RunRoot, "reflow.input.s3.jsonl")
		if err := UploadCorpusToS3(ctx, s3Prov, byoS3, corpus, s3SourcePrefix, s3InputPath); err != nil {
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
		defer func() { _ = s3Prov.Close() }()
		s3SourcePrefix = byoS3.MintUniquePrefix("src-" + invID[:8])
		s3InputPath = filepath.Join(opts.RunRoot, "reflow.input.s3.jsonl")
		if err := UploadCorpusToS3(ctx, s3Prov, byoS3, corpus, s3SourcePrefix, s3InputPath); err != nil {
			return Report{}, fmt.Errorf("upload corpus to moto: %w", err)
		}
		s3ExtraArgs = CLIProviderFlags(byoS3)
		providerClass = ProviderMoto
	}

	report := NewReport(spec.Name, providerClass, invID, binSHA, corpus.Manifest.Compact(), opts.Keep)
	report.BinaryVersion = binVer
	report.BinaryCommit = binCommit
	report.OS = runtime.GOOS
	report.Arch = runtime.GOARCH

	// Ownership ledger of minted destination/checkpoint relative names.
	type minted struct {
		destDir        string
		s3DestPrefix   string
		checkpointPath string
	}
	var mintedPoints []minted

	cleanup := func() error {
		if opts.Keep {
			return nil
		}
		var first error
		if s3Prov != nil {
			if s3SourcePrefix != "" {
				if err := DeleteS3PrefixVerified(context.Background(), s3Prov, s3SourcePrefix); err != nil && first == nil {
					first = fmt.Errorf("source prefix cleanup: %w", err)
				}
			}
			for _, pref := range s3DestPrefixes {
				if err := DeleteS3PrefixVerified(context.Background(), s3Prov, pref); err != nil && first == nil {
					first = fmt.Errorf("dest prefix cleanup: %w", err)
				}
			}
		}
		for _, m := range mintedPoints {
			if m.destDir != "" {
				if err := os.RemoveAll(m.destDir); err != nil && first == nil {
					first = err
				}
			}
			if m.checkpointPath != "" {
				if err := os.Remove(m.checkpointPath); err != nil && !os.IsNotExist(err) && first == nil {
					first = fmt.Errorf("checkpoint remove: %w", err)
				}
				if _, err := os.Stat(m.checkpointPath); !os.IsNotExist(err) {
					if first == nil {
						first = fmt.Errorf("checkpoint still present after cleanup")
					}
				}
			}
		}
		// Verify local dests gone.
		for _, m := range mintedPoints {
			if m.destDir == "" {
				continue
			}
			if _, err := os.Stat(m.destDir); !os.IsNotExist(err) {
				if first == nil {
					first = fmt.Errorf("cleanup left destination %s", filepath.Base(m.destDir))
				}
			}
		}
		return first
	}
	defer func() {
		// Best-effort fallback if caller ignores cleanup error path.
		_ = cleanup()
	}()

	pointOrdinal := 0
	// lastDestDir is the local destination of the most recent reflow/full_pipe point
	// (for content-parity snapshots).
	var lastDestDir string
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

		if useCloud {
			if shape == "full_pipe" {
				return fmt.Errorf("point %s: full_pipe on %s not in this cut (reflow-only BYO first; file fullpipe-ab remains)", pointID, providerClass)
			}
			if shape == "probe_drain" {
				return fmt.Errorf("point %s: probe_drain is file-local only", pointID)
			}
			s3DestPrefix = byoS3.MintUniquePrefix("dst-" + pointID)
			if n, err := CountS3Prefix(ctx, s3Prov, s3DestPrefix); err != nil {
				return fmt.Errorf("point %s dest list: %w", pointID, err)
			} else if n != 0 {
				return fmt.Errorf("point %s: destination prefix not empty", pointID)
			}
			s3DestPrefixes = append(s3DestPrefixes, s3DestPrefix)
			destURI = byoS3.ObjectURI(s3DestPrefix)
			extraArgs = s3ExtraArgs
			inputPath = s3InputPath
			mintedPoints = append(mintedPoints, minted{s3DestPrefix: s3DestPrefix, checkpointPath: ckPath})
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
			mintedPoints = append(mintedPoints, minted{destDir: destDir, checkpointPath: ckPath})
		} else {
			// probe_drain: no destination; still track a throwaway checkpoint path absence.
			mintedPoints = append(mintedPoints, minted{checkpointPath: ckPath})
		}

		pctx, cancel := context.WithTimeout(ctx, opts.PointTimeout)
		defer cancel()

		stdoutPath := filepath.Join(opts.RunRoot, "stdout-"+pointID+".jsonl")

		var pr PointResult
		var runErr error
		switch shape {
		case "full_pipe":
			srcPrefix := fileURIFromAbs(corpus.Root) + "/"
			pr, runErr = RunFullPipe(pctx, FullPipeOpts{
				Binary:         absBin,
				SourcePrefix:   srcPrefix,
				ProbeConfig:    corpus.ProbeConfigPath,
				DestURI:        destURI,
				ProbeConc:      probeConc,
				ReflowParallel: parallel,
				CheckpointPath: ckPath,
				GOMEMLIMIT:     gomem,
				MemoryBudget:   rp.MemoryBudget,
				StdoutPath:     stdoutPath,
			})
		case "probe_drain":
			srcPrefix := fileURIFromAbs(corpus.Root) + "/"
			pr, runErr = RunProbeDrain(pctx, absBin, srcPrefix, corpus.ProbeConfigPath, probeConc, gomem)
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

		pt := PointReport{
			PointID:                pointID,
			ExecutionShape:         shape,
			ProbeConcurrency:       probeConc,
			GOMEMLIMITSet:          gomem != "",
			GOMEMLIMITValue:        gomem,
			MemoryBudgetRequested:  rp.MemoryBudget,
			MemoryEnvelope:         memEnvelope,
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
		}
		if shape != "probe_drain" {
			pt.Parallel = parallel
			pt.CheckpointClass = ckClass
			// Memory provenance exactly as the product reported it. A missing
			// source is a failure the report validator must catch, not
			// something to paper over with a placeholder label.
			pt.MemoryLimitSource = parsed.MemoryLimitSource
			pt.MemoryBudgetSource = parsed.MemoryBudgetSource
			pt.MemoryLimitBytes = parsed.MemoryLimitBytes
			pt.MemoryBudgetEffectiveBytes = parsed.MemoryBudgetEffectiveBytes
			pt.RetryBufferCapBytes = parsed.RetryBufferCapBytes
			pt.ConcurrencyTimeAvgActive = parsed.SummaryTimeAvgActive
			adaptiveMode := "adaptive"
			if spec.NoAdaptive {
				adaptiveMode = "fixed"
			}
			pt.AdaptiveMode = adaptiveMode
			pt.ConcurrencyRequested = intPtr(parsed.Requested)
			pt.ConcurrencyEffective = intPtr(parsed.Effective)
			pt.ConcurrencyReason = strPtr(parsed.Reason)
			pt.ConcurrencyMaxActive = intPtr(parsed.MaxActive)
			pt.ConcurrencyFinal = intPtr(parsed.Final)
			pt.AdaptiveEnabled = boolPtrVal(parsed.AdaptiveEnabled)
		}
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
			destA := lastDestDir
			snapA, err := SnapshotDestTree(destA)
			if err != nil {
				return report, fmt.Errorf("fullpipe arm A snapshot: %w", err)
			}
			if err := runPoint(armRun); err != nil {
				return report, err
			}
			snapB, err := SnapshotDestTree(lastDestDir)
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
		// reflow-only sweeps: every declared memory envelope over every
		// checkpoint class and parallel point.
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
	// Prevent deferred double-clean issues: clear minted after successful cleanup.
	mintedPoints = nil

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
