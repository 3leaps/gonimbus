package cmd

import (
	"fmt"
	"strings"
)

func indexSubstrateJournalRunDir(indexSetID, runID string) (string, error) {
	indexSetID = strings.TrimSpace(indexSetID)
	runID = strings.TrimSpace(runID)
	if indexSetID == "" {
		return "", fmt.Errorf("index set id is required")
	}
	if runID == "" {
		return "", fmt.Errorf("run id is required")
	}
	cleanIndexSetID, err := cleanAppDataPathPart(indexSetID)
	if err != nil {
		return "", fmt.Errorf("index set id: %w", err)
	}
	cleanRunID, err := cleanAppDataPathPart(runID)
	if err != nil {
		return "", fmt.Errorf("run id: %w", err)
	}
	return appDataPath(appDataClassCrawlJournals, cleanIndexSetID, cleanRunID)
}

func indexSubstrateSegmentCacheDir(indexSetID string) (string, error) {
	indexSetID = strings.TrimSpace(indexSetID)
	if indexSetID == "" {
		return "", fmt.Errorf("index set id is required")
	}
	cleanIndexSetID, err := cleanAppDataPathPart(indexSetID)
	if err != nil {
		return "", fmt.Errorf("index set id: %w", err)
	}
	return appDataPath(appDataClassSegmentCache, cleanIndexSetID)
}

func cleanAppDataPathPart(part string) (string, error) {
	part = strings.TrimSpace(part)
	if part == "" || part == "." || part == ".." {
		return "", fmt.Errorf("invalid path part")
	}
	if strings.ContainsAny(part, `/\`) {
		return "", fmt.Errorf("must not contain path separators")
	}
	return part, nil
}
