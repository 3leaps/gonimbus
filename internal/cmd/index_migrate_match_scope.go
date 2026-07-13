package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/scope"
)

// index migrate-match-scope audits and converts prefix-shaped match.includes
// into an explicit build.scope prefix_list (GON-057 Slot 1.3 / G11 subset).
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
  move consumers → validation window → reclaim old set via index gc (G5a).

Remaining G11 controls (excludes, non-prefix globs, metadata filters) stay open.`,
	RunE: runIndexMigrateMatchScope,
}

func init() {
	indexCmd.AddCommand(indexMigrateMatchScopeCmd)
	indexMigrateMatchScopeCmd.Flags().StringVar(&indexMigrateMatchScopeJob, "job", "", "Index manifest path to audit/convert (required)")
	indexMigrateMatchScopeCmd.Flags().StringVar(&indexMigrateMatchScopeEmit, "emit-manifest", "", "Write proposed manifest to this new path (exclusive create unless --force)")
	indexMigrateMatchScopeCmd.Flags().BoolVar(&indexMigrateMatchScopeJSON, "json", false, "Emit MigrationPlan JSON on stdout")
	indexMigrateMatchScopeCmd.Flags().BoolVar(&indexMigrateMatchScopeForce, "force", false, "Allow overwriting --emit-manifest destination")
	_ = indexMigrateMatchScopeCmd.MarkFlagRequired("job")
}

func runIndexMigrateMatchScope(cmd *cobra.Command, args []string) error {
	_ = args
	jobPath := strings.TrimSpace(indexMigrateMatchScopeJob)
	if jobPath == "" {
		return fmt.Errorf("--job is required")
	}

	raw, err := os.ReadFile(jobPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("manifest not found: %s", jobPath)
		}
		return fmt.Errorf("read manifest: %w", err)
	}

	// Refuse before any FS mutation beyond the read above.
	plan, err := scope.PlanMatchPrefixMigration(raw, scope.PlanOptions{
		GonimbusVersion: versionInfo.Version,
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
		if err := writeProposedManifestExclusive(emitPath, plan.ProposedManifestYAML, indexMigrateMatchScopeForce); err != nil {
			return err
		}
		// Do not leave full YAML in JSON when written to disk (keep digest).
		// Human text still notes the path.
	}

	if indexMigrateMatchScopeJSON {
		// Avoid dumping full client keyspaces; proposed YAML is the operator
		// artifact. For JSON, keep digest + path note, strip full YAML body
		// when emit path was used (or always strip and rely on --emit-manifest).
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

func writeProposedManifestExclusive(path, content string, force bool) error {
	if strings.TrimSpace(content) == "" {
		return fmt.Errorf("proposed manifest is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && !os.IsExist(err) {
		// Dir may be "." — MkdirAll(".") is fine.
		if filepath.Dir(path) != "." {
			return fmt.Errorf("create emit directory: %w", err)
		}
	}

	flag := os.O_WRONLY | os.O_CREATE | os.O_EXCL
	if force {
		flag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	}
	f, err := os.OpenFile(path, flag, 0o644)
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("emit path exists (use --force to overwrite): %s", path)
		}
		return fmt.Errorf("open emit path: %w", err)
	}
	defer func() { _ = f.Close() }()

	if _, err := f.WriteString(content); err != nil {
		return fmt.Errorf("write emit path: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync emit path: %w", err)
	}
	return nil
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
	_, _ = fmt.Fprintln(w, "         move consumers → validation window → reclaim old set via index gc (G5a).")
	_, _ = fmt.Fprintln(w, "remaining G11 (excludes / non-prefix globs / filters) is still open.")
	return nil
}
