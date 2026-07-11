package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/3leaps/gonimbus/pkg/jobregistry"
	"github.com/3leaps/gonimbus/pkg/manifest"
)

var afterManagedManifestRead = func() {}

func currentIndexBuildInvocation() jobregistry.IndexBuildInvocation {
	requestedFormat := strings.ToLower(strings.TrimSpace(indexBuildFormat))
	if requestedFormat == "" {
		requestedFormat = "durable"
	}
	if indexBuildExperimentalEngine {
		requestedFormat = "experimental-engine"
	}
	return jobregistry.IndexBuildInvocation{
		SchemaVersion:     jobregistry.IndexBuildInvocationVersion,
		RequestedFormat:   requestedFormat,
		EffectiveFormat:   selectedIndexBuildFormat(),
		ConfigPath:        strings.TrimSpace(cfgFile),
		Verbose:           verbose,
		ReadOnly:          IsReadOnly(),
		DBPath:            strings.TrimSpace(indexBuildDBPath),
		Since:             strings.TrimSpace(indexBuildSince),
		Name:              strings.TrimSpace(indexBuildName),
		StorageProvider:   strings.TrimSpace(indexBuildStorageProv),
		CloudProvider:     strings.TrimSpace(indexBuildCloudProv),
		RegionKind:        strings.TrimSpace(indexBuildRegionKind),
		Region:            strings.TrimSpace(indexBuildRegion),
		EndpointHost:      strings.TrimSpace(indexBuildEndpointHost),
		ScopeWarnPrefixes: indexBuildScopeWarnPrefix,
		ScopeMaxPrefixes:  indexBuildScopeMaxPrefix,
	}
}

func resolvedCurrentIndexBuildInvocation() (jobregistry.IndexBuildInvocation, error) {
	inv := currentIndexBuildInvocation()
	resolved, err := resolveAppDataRoot()
	if err != nil {
		return jobregistry.IndexBuildInvocation{}, fmt.Errorf("resolve effective data root: %w", err)
	}
	inv.DataRoot = resolved.Dir
	return inv, nil
}

func claimManagedIndexBuildJob(store *jobregistry.Store, jobID string) (*jobregistry.JobRecord, *manifest.IndexManifest, error) {
	var parsedManifest *manifest.IndexManifest
	job, err := store.ClaimQueued(jobID, os.Getpid(), func(job *jobregistry.JobRecord) error {
		if job.Invocation == nil {
			return fmt.Errorf("managed job %s has no effective invocation", jobID)
		}
		wantFingerprint, err := jobregistry.IndexBuildInvocationFingerprint(*job.Invocation)
		if err != nil {
			return fmt.Errorf("validate managed invocation: %w", err)
		}
		if wantFingerprint != strings.TrimSpace(job.InvocationFingerprint) {
			return fmt.Errorf("managed invocation fingerprint mismatch")
		}
		absManifest, err := filepath.Abs(strings.TrimSpace(indexBuildJobPath))
		if err != nil {
			return fmt.Errorf("resolve managed manifest path: %w", err)
		}
		manifestBytes, digest, err := jobregistry.ReadManifestBytesAndSHA256(absManifest)
		if err != nil {
			return err
		}
		afterManagedManifestRead()
		inv := job.Invocation
		if filepath.Clean(absManifest) != filepath.Clean(inv.ManifestPath) || digest != inv.ManifestSHA256 {
			return fmt.Errorf("managed manifest changed after enqueue")
		}
		parsedManifest, err = manifest.LoadIndexManifestFromBytes(manifestBytes, absManifest)
		if err != nil {
			return fmt.Errorf("load index manifest: %w", err)
		}
		return validateManagedIndexBuildFlags(*inv)
	})
	if err != nil {
		return nil, nil, err
	}
	return job, parsedManifest, nil
}

func persistCommittedIndexBuildJob(store *jobregistry.Store, job *jobregistry.JobRecord) error {
	if store == nil || job == nil {
		return nil
	}
	if err := store.Write(job); err != nil {
		return fmt.Errorf("index build committed but persist terminal job identity: %w", err)
	}
	return nil
}

func validateManagedIndexBuildFlags(inv jobregistry.IndexBuildInvocation) error {
	checks := []struct {
		name string
		got  string
		want string
	}{
		{"format", selectedIndexBuildFormat(), inv.EffectiveFormat},
		{"db", strings.TrimSpace(indexBuildDBPath), inv.DBPath},
		{"since", strings.TrimSpace(indexBuildSince), inv.Since},
		{"name", strings.TrimSpace(indexBuildName), inv.Name},
		{"storage-provider", strings.TrimSpace(indexBuildStorageProv), inv.StorageProvider},
		{"cloud-provider", strings.TrimSpace(indexBuildCloudProv), inv.CloudProvider},
		{"region-kind", strings.TrimSpace(indexBuildRegionKind), inv.RegionKind},
		{"region", strings.TrimSpace(indexBuildRegion), inv.Region},
		{"endpoint-host", strings.TrimSpace(indexBuildEndpointHost), inv.EndpointHost},
	}
	for _, check := range checks {
		if check.got != check.want {
			return fmt.Errorf("managed invocation %s mismatch", check.name)
		}
	}
	if indexBuildScopeWarnPrefix != inv.ScopeWarnPrefixes || indexBuildScopeMaxPrefix != inv.ScopeMaxPrefixes {
		return fmt.Errorf("managed invocation scope limit mismatch")
	}
	if indexBuildExperimentalEngine {
		return fmt.Errorf("managed child must use normalized --format, not --experimental-engine")
	}
	if strings.TrimSpace(cfgFile) != inv.ConfigPath || verbose != inv.Verbose || IsReadOnly() != inv.ReadOnly {
		return fmt.Errorf("managed invocation global option mismatch")
	}
	if inv.DataRoot != "" {
		resolved, err := resolveAppDataRoot()
		if err != nil || filepath.Clean(resolved.Dir) != filepath.Clean(inv.DataRoot) {
			return fmt.Errorf("managed invocation data root mismatch")
		}
	}
	return nil
}
