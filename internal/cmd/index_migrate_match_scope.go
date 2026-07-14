package cmd

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/indexstore"
	"github.com/3leaps/gonimbus/pkg/manifest"
	"github.com/3leaps/gonimbus/pkg/scope"
)

// index migrate-match-scope audits and converts prefix-shaped match.includes
// into an explicit build.scope prefix_list.
// Pure audit: no provider construction, authority, markers, or publication.

var (
	indexMigrateMatchScopeJob   string
	indexMigrateMatchScopeEmit  string
	indexMigrateMatchScopeJSON  bool
	indexMigrateMatchScopeForce bool
)

var indexMigrateMatchScopeCmd = &cobra.Command{
	Use:   "migrate-match-scope",
	Short: "Audit/convert prefix-shaped match.includes to build.scope prefix_list",
	Long: `Classify and convert only proven prefix-shaped build.match.includes
(literal non-root prefix + terminal /**, no residual predicates) into an
explicit build.scope type: prefix_list.

This is a pure audit/conversion command:
  - no cloud provider construction
  - no index authority, directory, marker, or publication side effects
  - does not rewrite the source manifest in place

Ambiguous includes, excludes, filters, include_hidden, and existing scope
(with residual match) fail closed with stable reason codes. Default includes
["**"] classify as already_compatible. A previously emitted proposed form
classifies as already_migrated.

Cutover after a successful conversion is an operator workflow:
  emit proposed → build new set → compare projections → pin new receipt →
  move consumers → validation window → reclaim old set via whole-set index gc.

Non-prefix match controls (excludes, non-prefix globs, metadata filters) stay open.`,
	RunE: runIndexMigrateMatchScope,
}

func init() {
	indexCmd.AddCommand(indexMigrateMatchScopeCmd)
	indexMigrateMatchScopeCmd.Flags().StringVar(&indexMigrateMatchScopeJob, "job", "", "Index manifest path to audit/convert (required)")
	indexMigrateMatchScopeCmd.Flags().StringVar(&indexMigrateMatchScopeEmit, "emit-manifest", "", "Write proposed manifest to this new path (exclusive create unless --force)")
	indexMigrateMatchScopeCmd.Flags().BoolVar(&indexMigrateMatchScopeJSON, "json", false, "Emit MigrationPlan JSON on stdout")
	indexMigrateMatchScopeCmd.Flags().BoolVar(&indexMigrateMatchScopeForce, "force", false, "Allow overwriting --emit-manifest destination via atomic replacement")
	_ = indexMigrateMatchScopeCmd.MarkFlagRequired("job")
}

func runIndexMigrateMatchScope(cmd *cobra.Command, args []string) error {
	_ = args
	jobPath := strings.TrimSpace(indexMigrateMatchScopeJob)
	if jobPath == "" {
		return fmt.Errorf("--job is required")
	}

	// Operator-supplied path; path is not derived from untrusted network input.
	raw, err := os.ReadFile(jobPath) // #nosec G304 -- CLI --job path
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("manifest not found: %s", jobPath)
		}
		return fmt.Errorf("read manifest: %w", err)
	}

	// Refuse before any FS mutation beyond the read above.
	plan, err := scope.PlanMatchPrefixMigration(raw, scope.PlanOptions{
		GonimbusVersion: versionInfo.Version,
		ComputeIdentity: computeMigrationIdentityEvidence,
	})
	if err != nil {
		return err
	}

	emitPath := strings.TrimSpace(indexMigrateMatchScopeEmit)
	if emitPath != "" {
		if plan.Classification != scope.ClassificationConvertible {
			return fmt.Errorf("cannot --emit-manifest: classification=%s reason=%s (%s)",
				plan.Classification, plan.ReasonCode, plan.Detail)
		}
		if err := writeProposedManifestSafe(jobPath, emitPath, plan.ProposedManifestYAML, indexMigrateMatchScopeForce); err != nil {
			return err
		}
	}

	if indexMigrateMatchScopeJSON {
		out := *plan
		if emitPath != "" {
			out.ProposedManifestYAML = ""
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(&out); err != nil {
			return fmt.Errorf("encode plan json: %w", err)
		}
		return nil
	}

	return printMatchScopeMigrationPlan(plan, jobPath, emitPath)
}

// computeMigrationIdentityEvidence uses the storageful identity helper from the
// CLI adapter so pkg/scope stays storage-free (ADR-0006).
func computeMigrationIdentityEvidence(m *manifest.IndexManifest, gonimbusVersion string) (*scope.ComputedIdentityEvidence, error) {
	if m == nil {
		return nil, fmt.Errorf("manifest is nil")
	}
	var scopeHash string
	var includes []string
	includeHidden := false
	var excludes []string
	sourceType := manifest.DefaultIndexSource
	if m.Build != nil {
		if m.Build.Source != "" {
			sourceType = m.Build.Source
		}
		if m.Build.Scope != nil {
			h, err := scope.HashConfig(m.Build.Scope)
			if err != nil {
				return nil, err
			}
			scopeHash = h
		}
		if m.Build.Match != nil {
			includes = append([]string(nil), m.Build.Match.Includes...)
			excludes = append([]string(nil), m.Build.Match.Excludes...)
			includeHidden = m.Build.Match.IncludeHidden
		}
	}
	if len(includes) == 0 {
		includes = []string{manifest.DefaultIndexIncludes}
	}

	params := indexstore.IndexSetParams{
		BaseURI:  m.Connection.BaseURI,
		Provider: m.Connection.Provider,
		Endpoint: m.Connection.Endpoint,
		BuildParams: indexstore.BuildParams{
			SourceType:      sourceType,
			SchemaVersion:   indexstore.SchemaVersion,
			GonimbusVersion: gonimbusVersion,
			Includes:        includes,
			Excludes:        excludes,
			IncludeHidden:   includeHidden,
			ScopeHash:       scopeHash,
		},
	}
	if m.Identity != nil {
		params.StorageProvider = m.Identity.StorageProvider
		params.CloudProvider = m.Identity.CloudProvider
		params.RegionKind = m.Identity.RegionKind
		params.Region = m.Identity.Region
		params.EndpointHost = m.Identity.EndpointHost
	}
	if m.PathDate != nil {
		params.BuildParams.PathDateExtraction = &indexstore.PathDateExtraction{
			Method:       m.PathDate.Method,
			Regex:        m.PathDate.Regex,
			SegmentIndex: m.PathDate.SegmentIndex,
		}
	}

	result, err := indexstore.ComputeIndexSetID(params)
	if err != nil {
		return nil, err
	}
	return &scope.ComputedIdentityEvidence{
		Kind:            "computed",
		IndexSetID:      result.IndexSetID,
		CanonicalSHA256: result.CanonicalSHA256,
		ScopeHash:       scopeHash,
		Includes:        includes,
		GonimbusVersion: gonimbusVersion,
	}, nil
}

// writeProposedManifestSafe writes the proposed manifest with:
//   - refusal when source and destination are the same path or hard-link alias
//   - refusal of symlink destinations
//   - exclusive create when force is false
//   - same-directory temp + atomic rename when force is true
//   - final owner-only mode (0600)
//   - cleanup of partial temp files on failure
func writeProposedManifestSafe(sourcePath, destPath, content string, force bool) error {
	if strings.TrimSpace(content) == "" {
		return fmt.Errorf("proposed manifest is empty")
	}
	absSrc, err := filepath.Abs(sourcePath)
	if err != nil {
		return fmt.Errorf("resolve source path: %w", err)
	}
	absDst, err := filepath.Abs(destPath)
	if err != nil {
		return fmt.Errorf("resolve emit path: %w", err)
	}
	if absSrc == absDst {
		return fmt.Errorf("refuse to overwrite source manifest: --emit-manifest must differ from --job")
	}
	srcInfo, srcErr := os.Lstat(absSrc)
	dstInfo, dstErr := os.Lstat(absDst)
	if srcErr == nil && dstErr == nil && os.SameFile(srcInfo, dstInfo) {
		return fmt.Errorf("refuse to overwrite source manifest alias: --emit-manifest resolves to the same file as --job")
	}
	if dstErr == nil {
		if dstInfo.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("emit path is a symlink (no-follow policy): %s", destPath)
		}
		if !dstInfo.Mode().IsRegular() {
			return fmt.Errorf("emit path is not a regular file: %s", destPath)
		}
		if !force {
			return fmt.Errorf("emit path exists (use --force to overwrite): %s", destPath)
		}
	} else if !os.IsNotExist(dstErr) {
		return fmt.Errorf("stat emit path: %w", dstErr)
	}

	dir := filepath.Dir(absDst)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create emit directory: %w", err)
	}

	// Write a complete, synced, owner-only same-directory temp first, then publish
	// the inode: exclusive no-replace via hard-link, or force via rename.
	tmp, err := createOwnerOnlyTemp(dir, ".gonimbus-migrate-")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		_ = tmp.Close()
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := tmp.WriteString(content); err != nil {
		return fmt.Errorf("write temp emit: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync temp emit: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp emit: %w", err)
	}
	// Re-assert owner-only mode in case umask widened create mode. Final pathname
	// inherits this mode from the published inode; no post-publish chmod.
	if err := os.Chmod(tmpPath, 0o600); err != nil {
		return fmt.Errorf("chmod temp emit: %w", err)
	}

	if !force {
		// Atomic exclusive publish: link a fully-written 0600 temp to the final
		// path. os.Link is no-replace (fails if dest exists). Do not fall back to
		// rename or O_EXCL+copy — those either replace or expose partial content.
		if err := os.Link(tmpPath, absDst); err != nil {
			if errors.Is(err, os.ErrExist) || os.IsExist(err) {
				return fmt.Errorf("emit path exists (use --force to overwrite): %s", destPath)
			}
			return fmt.Errorf("exclusive atomic publish failed (hard-link required; no replace fallback): %w", err)
		}
		// Dest and temp share the complete inode; drop the temp name only.
		return nil
	}

	// Force: atomic replace destination (rename replaces symlink/file at path without following).
	if err := os.Rename(tmpPath, absDst); err != nil {
		return fmt.Errorf("atomic replace emit path: %w", err)
	}
	cleanup = false
	return nil
}

func createOwnerOnlyTemp(dir, prefix string) (*os.File, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, fmt.Errorf("temp id: %w", err)
	}
	name := filepath.Join(dir, prefix+hex.EncodeToString(b[:])+".tmp")
	// #nosec G304 -- temp path under operator emit directory
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create temp emit: %w", err)
	}
	return f, nil
}

func printMatchScopeMigrationPlan(plan *scope.MigrationPlan, jobPath, emitPath string) error {
	w := os.Stdout
	_, _ = fmt.Fprintln(w, "Match→Scope Prefix Migration Plan (audit)")
	_, _ = fmt.Fprintln(w, "==========================================")
	_, _ = fmt.Fprintln(w)
	_, _ = fmt.Fprintf(w, "source: %s\n", jobPath)
	_, _ = fmt.Fprintf(w, "schema: %s v%d\n", plan.Schema, plan.Version)
	_, _ = fmt.Fprintf(w, "source_manifest_digest: %s\n", plan.SourceManifestDigest)
	_, _ = fmt.Fprintf(w, "classification: %s\n", plan.Classification)
	_, _ = fmt.Fprintf(w, "reason_code: %s\n", plan.ReasonCode)
	if plan.Detail != "" {
		_, _ = fmt.Fprintf(w, "detail: %s\n", plan.Detail)
	}
	_, _ = fmt.Fprintln(w)

	if len(plan.CanonicalLegacyIncludes) > 0 {
		_, _ = fmt.Fprintln(w, "legacy includes:")
		for _, inc := range plan.CanonicalLegacyIncludes {
			_, _ = fmt.Fprintf(w, "  - %s\n", inc)
		}
		_, _ = fmt.Fprintln(w)
	}

	if plan.LegacyPlanCount > 0 {
		_, _ = fmt.Fprintf(w, "legacy LIST plan: count=%d digest=%s\n", plan.LegacyPlanCount, plan.LegacyPlanDigest)
		for _, p := range plan.LegacyProviderPrefixes {
			_, _ = fmt.Fprintf(w, "  - %s\n", p)
		}
		_, _ = fmt.Fprintln(w)
	}

	if plan.ProposedScope != nil {
		_, _ = fmt.Fprintf(w, "proposed scope.type: %s\n", plan.ProposedScope.Type)
		_, _ = fmt.Fprintf(w, "proposed scope_hash: %s\n", plan.ProposedScopeHash)
		_, _ = fmt.Fprintln(w, "proposed scope.prefixes:")
		for _, p := range plan.ProposedScope.Prefixes {
			_, _ = fmt.Fprintf(w, "  - %s\n", p)
		}
		_, _ = fmt.Fprintln(w)
	}

	if plan.ProposedPlanCount > 0 {
		_, _ = fmt.Fprintf(w, "proposed LIST plan: count=%d digest=%s\n", plan.ProposedPlanCount, plan.ProposedPlanDigest)
		for _, p := range plan.ProposedProviderPrefixes {
			_, _ = fmt.Fprintf(w, "  - %s\n", p)
		}
		_, _ = fmt.Fprintln(w)
	}

	if plan.ProposedManifestDigest != "" {
		_, _ = fmt.Fprintf(w, "proposed_manifest_digest: %s\n", plan.ProposedManifestDigest)
	}
	if emitPath != "" {
		_, _ = fmt.Fprintf(w, "emitted_manifest: %s\n", emitPath)
	} else if plan.ProposedManifestYAML != "" {
		_, _ = fmt.Fprintln(w)
		_, _ = fmt.Fprintln(w, "proposed manifest (YAML):")
		_, _ = fmt.Fprintln(w, "---")
		_, _ = fmt.Fprint(w, plan.ProposedManifestYAML)
		if !strings.HasSuffix(plan.ProposedManifestYAML, "\n") {
			_, _ = fmt.Fprintln(w)
		}
		_, _ = fmt.Fprintln(w, "---")
	}

	if plan.LegacyConfigIdentity != nil {
		_, _ = fmt.Fprintln(w)
		_, _ = fmt.Fprintln(w, "identity evidence (computed under current binary; not authoritative for live sets):")
		_, _ = fmt.Fprintf(w, "  legacy_index_set_id:   %s\n", plan.LegacyConfigIdentity.IndexSetID)
		if plan.ProposedConfigIdentity != nil {
			_, _ = fmt.Fprintf(w, "  proposed_index_set_id: %s\n", plan.ProposedConfigIdentity.IndexSetID)
		}
	}

	_, _ = fmt.Fprintln(w)
	_, _ = fmt.Fprintln(w, "cutover: build new set with proposed manifest → compare projections → pin new receipt →")
	_, _ = fmt.Fprintln(w, "         move consumers → validation window → reclaim old set via whole-set index gc.")
	_, _ = fmt.Fprintln(w, "non-prefix match controls (excludes / non-prefix globs / filters) remain open.")
	return nil
}
