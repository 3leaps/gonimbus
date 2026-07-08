package cmd

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/internal/indexcompare"
	"github.com/3leaps/gonimbus/internal/indexsubstrate"
)

var indexCompareCmd = &cobra.Command{
	Use:   "compare",
	Short: "Compare index artifacts",
	Long:  `Compare index artifacts produced by gonimbus index workflows.`,
}

var indexCompareDurableDeltaCmd = &cobra.Command{
	Use:   "durable-delta",
	Short: "Compare two durable snapshots",
	Long: `Compare two durable snapshots from the same index set and report a
temporal delta with fail-closed coverage attribution.`,
	RunE: runIndexCompareDurableDelta,
}

func init() {
	indexCmd.AddCommand(indexCompareCmd)
	indexCompareCmd.AddCommand(indexCompareDurableDeltaCmd)

	indexCompareDurableDeltaCmd.Flags().String("before-manifest", "", "Before durable manifest path (required)")
	indexCompareDurableDeltaCmd.Flags().String("before-segments", "", "Before durable segment directory (required)")
	indexCompareDurableDeltaCmd.Flags().String("after-manifest", "", "After durable manifest path (required)")
	indexCompareDurableDeltaCmd.Flags().String("after-segments", "", "After durable segment directory (required)")
	indexCompareDurableDeltaCmd.Flags().Int("max-changes", indexcompare.DefaultMaxMismatches, "Maximum change details to include")
	_ = indexCompareDurableDeltaCmd.MarkFlagRequired("before-manifest")
	_ = indexCompareDurableDeltaCmd.MarkFlagRequired("before-segments")
	_ = indexCompareDurableDeltaCmd.MarkFlagRequired("after-manifest")
	_ = indexCompareDurableDeltaCmd.MarkFlagRequired("after-segments")
}

func runIndexCompareDurableDelta(cmd *cobra.Command, _ []string) error {
	beforeManifestPath, _ := cmd.Flags().GetString("before-manifest")
	beforeSegmentDir, _ := cmd.Flags().GetString("before-segments")
	afterManifestPath, _ := cmd.Flags().GetString("after-manifest")
	afterSegmentDir, _ := cmd.Flags().GetString("after-segments")
	maxChanges, _ := cmd.Flags().GetInt("max-changes")

	beforeManifest, err := readCompareManifestFlag("before-manifest", beforeManifestPath)
	if err != nil {
		return err
	}
	afterManifest, err := readCompareManifestFlag("after-manifest", afterManifestPath)
	if err != nil {
		return err
	}
	report, err := indexcompare.CompareDurableDelta(cmd.Context(), indexcompare.DurableDeltaInput{
		Before: indexcompare.DurableSnapshotInput{
			Manifest:   beforeManifest,
			SegmentDir: strings.TrimSpace(beforeSegmentDir),
			Artifact:   indexcompare.Artifact{ID: beforeManifest.RunID, Path: strings.TrimSpace(beforeManifestPath)},
		},
		After: indexcompare.DurableSnapshotInput{
			Manifest:   afterManifest,
			SegmentDir: strings.TrimSpace(afterSegmentDir),
			Artifact:   indexcompare.Artifact{ID: afterManifest.RunID, Path: strings.TrimSpace(afterManifestPath)},
		},
		MaxChanges: maxChanges,
	})
	if err != nil {
		return err
	}
	enc := json.NewEncoder(cmd.OutOrStdout())
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}

func readCompareManifestFlag(name, path string) (indexsubstrate.InternalManifest, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return indexsubstrate.InternalManifest{}, fmt.Errorf("--%s is required", name)
	}
	manifest, err := indexsubstrate.ReadInternalManifestFile(path)
	if err != nil {
		return indexsubstrate.InternalManifest{}, fmt.Errorf("read --%s: %w", name, err)
	}
	return manifest, nil
}
