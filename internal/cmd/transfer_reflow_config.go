package cmd

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/match"
	"github.com/3leaps/gonimbus/pkg/provider"
	providerfile "github.com/3leaps/gonimbus/pkg/provider/file"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
)

// The destination-metadata decision plane lives in pkg/reflow. These aliases keep
// the CLI's flag-resolution, capability-check, and checkpoint-serialization code
// reading the same types the engine consumes.
type (
	reflowMetadataConfig    = reflowpkg.MetadataPlan
	metadataSourceKeyRule   = reflowpkg.MetadataSourceKeyRule
	metadataDerivedRule     = reflowpkg.MetadataDerivedRule
	metadataBudgetError     = reflowpkg.MetadataBudgetError
	metadataDerivationError = reflowpkg.MetadataDerivationError
)

type collisionConfig struct {
	Mode             string
	QuarantinePrefix string
	DeprecatedLog    bool
}

type reflowSourceConfig struct {
	Symlinks        string
	Hidden          string
	Excludes        []string
	PreserveMode    bool
	OnSourceFailure string
}

// metadataCapabilityFlags lists the CLI flags that demand a metadata-aware PUT,
// for capability-error messaging. CLI flag vocabulary stays out of pkg/reflow.
func metadataCapabilityFlags(c reflowMetadataConfig) []string {
	var out []string
	if c.Policy == metadataPolicyPreserve || c.Policy == metadataPolicyMerge {
		out = append(out, "--metadata-policy")
	}
	if len(c.Set) > 0 {
		out = append(out, "--metadata-set")
	}
	if len(c.SourceKeyRules) > 0 {
		out = append(out, "--metadata-set-from-source-key")
	}
	if len(c.DerivedRules) > 0 {
		out = append(out, "--metadata-set-from-source-derived")
	}
	if c.OnMissingSource != "" && c.OnMissingSource != metadataMissingSkip {
		out = append(out, "--metadata-on-missing-source")
	}
	if c.PreserveContentType {
		out = append(out, "--preserve-content-type")
	}
	if c.DestinationStorageClass != "" {
		out = append(out, "--destination-storage-class")
	}
	return out
}

// metadataRunConfig builds the gonimbus.reflow.run.v1 metadata block for a plan,
// or nil when nothing notable is configured. Lives at the CLI boundary because it
// references the file-provider sidecar default.
func metadataRunConfig(c reflowMetadataConfig) *reflowpkg.MetadataRunConfig {
	if !c.RequiresCapability() && c.MetadataSidecarSuffix == providerfile.DefaultMetadataSidecarSuffix && c.OnMissingSource == metadataMissingSkip {
		return nil
	}
	keys := make([]string, 0, len(c.Set))
	for key := range c.Set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	sourceKeys := metadataSourceRuleDestKeys(c.SourceKeyRules)
	derivedKeys := metadataDerivedRuleDestKeys(c.DerivedRules)
	onMissing := ""
	if c.HasPerObjectRules() || c.OnMissingSource != metadataMissingSkip {
		onMissing = c.OnMissingSource
	}
	return &reflowpkg.MetadataRunConfig{
		Policy:                  c.Policy,
		SetKeys:                 keys,
		SourceKeyRuleKeys:       sourceKeys,
		DerivedRuleKeys:         derivedKeys,
		OnMissingSource:         onMissing,
		PreserveContentType:     c.PreserveContentType,
		DestinationStorageClass: c.DestinationStorageClass,
		MetadataSidecarSuffix:   c.MetadataSidecarSuffix,
	}
}

func metadataSourceRuleDestKeys(rules []metadataSourceKeyRule) []string {
	out := make([]string, 0, len(rules))
	for _, rule := range rules {
		out = append(out, rule.DestKey)
	}
	sort.Strings(out)
	return out
}

func metadataDerivedRuleDestKeys(rules []metadataDerivedRule) []string {
	out := make([]string, 0, len(rules))
	for _, rule := range rules {
		out = append(out, rule.DestKey)
	}
	sort.Strings(out)
	return out
}

const (
	reflowMemoryBudgetFloorBytes = int64(64) << 20 // 64 MiB — below this a budget cannot hold even a few retry buffers
	reflowMemoryBudgetMaxBytes   = int64(4) << 40  // 4 TiB — sanity bound on operator values, documented in help
)

// resolveReflowMemoryBudgetBytes resolves the operator transfer memory budget
// (flag --memory-budget, config key memory_budget). Returns 0 when unset (the
// resolver derives the budget from the detected limit). Invalid values are
// refused here, before any provider construction or destination mutation.
func resolveReflowMemoryBudgetBytes(cmd *cobra.Command) (int64, error) {
	raw := ""
	if cmd != nil && cmd.Flags().Changed("memory-budget") {
		raw = reflowMemoryBudget
	} else if viper.IsSet("memory_budget") {
		raw = viper.GetString("memory_budget")
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	bytes, err := match.ParseSize(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid memory budget %q: %w", raw, err)
	}
	if bytes < reflowMemoryBudgetFloorBytes {
		return 0, fmt.Errorf("memory budget %q is below the 64MiB floor", raw)
	}
	if bytes > reflowMemoryBudgetMaxBytes {
		return 0, fmt.Errorf("memory budget %q exceeds the 4TiB sanity bound", raw)
	}
	return bytes, nil
}

func resolveCollisionConfig(cmd *cobra.Command) (collisionConfig, error) {
	cfg := collisionConfig{Mode: reflowCollisionSkip}
	if cmd != nil && cmd.Flags().Changed("on-collision") {
		cfg.Mode = reflowOnCollision
	} else if viper.IsSet("on_collision") {
		cfg.Mode = viper.GetString("on_collision")
	}
	if cmd != nil && cmd.Flags().Changed("collision-quarantine-prefix") {
		cfg.QuarantinePrefix = reflowCollQuar
	} else if viper.IsSet("collision_quarantine_prefix") {
		cfg.QuarantinePrefix = viper.GetString("collision_quarantine_prefix")
	}

	cfg.Mode = strings.TrimSpace(strings.ToLower(cfg.Mode))
	cfg.QuarantinePrefix = strings.TrimSpace(cfg.QuarantinePrefix)
	if cfg.Mode == reflowCollisionLog {
		cfg.Mode = reflowCollisionSkip
		cfg.DeprecatedLog = true
	}
	if err := validateCollisionConfig(cfg); err != nil {
		return cfg, err
	}
	cfg.QuarantinePrefix = strings.Trim(cfg.QuarantinePrefix, "/")
	return cfg, nil
}

func validateCollisionConfig(cfg collisionConfig) error {
	switch cfg.Mode {
	case reflowCollisionSkip, reflowCollisionFail, reflowCollisionOver, reflowCollisionQuar, reflowCollisionSrcNew:
		// ok
	default:
		return fmt.Errorf("on-collision must be one of: skip-if-duplicate, fail, overwrite, quarantine, overwrite-if-source-newer")
	}
	if cfg.Mode == reflowCollisionQuar {
		if cfg.QuarantinePrefix == "" {
			return fmt.Errorf("collision_quarantine_prefix is required when on_collision=quarantine")
		}
		if !isRelativeQuarantinePrefix(cfg.QuarantinePrefix) {
			return fmt.Errorf("collision_quarantine_prefix must be a relative destination prefix")
		}
	}
	return nil
}

func resolveMetadataConfig(cmd *cobra.Command) (reflowMetadataConfig, error) {
	cfg := reflowMetadataConfig{
		Policy:                metadataPolicyClear,
		MetadataSidecarSuffix: providerfile.DefaultMetadataSidecarSuffix,
		OnMissingSource:       metadataMissingSkip,
	}
	if cmd != nil && cmd.Flags().Changed("metadata-policy") {
		cfg.Policy = reflowMetaPolicy
	} else if viper.IsSet("metadata.policy") {
		cfg.Policy = viper.GetString("metadata.policy")
	}
	if cmd != nil && cmd.Flags().Changed("metadata-set") {
		cfg.Set = parseMetadataSetRaw(reflowMetaSets)
	} else if viper.IsSet("metadata.set") {
		cfg.Set = parseMetadataSetRaw(viper.GetStringSlice("metadata.set"))
	}
	var err error
	if cmd != nil && cmd.Flags().Changed("metadata-set-from-source-key") {
		cfg.SourceKeyRules, err = reflowpkg.ParseMetadataSourceKeyRules(reflowMetaSrcKeys)
	} else if viper.IsSet("metadata.set_from_source_key") {
		cfg.SourceKeyRules, err = reflowpkg.ParseMetadataSourceKeyRules(viper.GetStringSlice("metadata.set_from_source_key"))
	}
	if err != nil {
		return cfg, err
	}
	if cmd != nil && cmd.Flags().Changed("metadata-set-from-source-derived") {
		cfg.DerivedRules, err = reflowpkg.ParseMetadataDerivedRules(reflowMetaDerived)
	} else if viper.IsSet("metadata.set_from_source_derived") {
		cfg.DerivedRules, err = reflowpkg.ParseMetadataDerivedRules(viper.GetStringSlice("metadata.set_from_source_derived"))
	}
	if err != nil {
		return cfg, err
	}
	if cmd != nil && cmd.Flags().Changed("preserve-content-type") {
		cfg.PreserveContentType = reflowMetaContent
	} else if viper.IsSet("metadata.preserve_content_type") {
		cfg.PreserveContentType = viper.GetBool("metadata.preserve_content_type")
	}
	if cmd != nil && cmd.Flags().Changed("destination-storage-class") {
		cfg.DestinationStorageClass = reflowMetaStorage
	} else if viper.IsSet("metadata.destination_storage_class") {
		cfg.DestinationStorageClass = viper.GetString("metadata.destination_storage_class")
	}
	if cmd != nil && cmd.Flags().Changed("metadata-on-missing-source") {
		cfg.OnMissingSource = reflowMetaMissing
	} else if viper.IsSet("metadata.on_missing_source") {
		cfg.OnMissingSource = viper.GetString("metadata.on_missing_source")
	}
	if cmd != nil && cmd.Flags().Changed("metadata-sidecar-suffix") {
		cfg.MetadataSidecarSuffix = reflowMetaSuffix
	} else if viper.IsSet("metadata.sidecar_suffix") {
		cfg.MetadataSidecarSuffix = viper.GetString("metadata.sidecar_suffix")
	}

	cfg.Policy = strings.TrimSpace(strings.ToLower(cfg.Policy))
	cfg.DestinationStorageClass = strings.TrimSpace(cfg.DestinationStorageClass)
	cfg.OnMissingSource = strings.TrimSpace(strings.ToLower(cfg.OnMissingSource))
	cfg.MetadataSidecarSuffix = strings.TrimSpace(cfg.MetadataSidecarSuffix)
	if cfg.MetadataSidecarSuffix == "" {
		cfg.MetadataSidecarSuffix = providerfile.DefaultMetadataSidecarSuffix
	}
	if err := validateMetadataConfig(cfg); err != nil {
		return cfg, err
	}
	if strings.EqualFold(cfg.DestinationStorageClass, storageClassPropagate) {
		cfg.DestinationStorageClass = storageClassPropagate
	} else {
		cfg.DestinationStorageClass = strings.ToUpper(cfg.DestinationStorageClass)
	}
	return cfg, nil
}

func parseMetadataSetRaw(raw []string) map[string]string {
	if len(raw) == 0 {
		return nil
	}
	out := make(map[string]string, len(raw))
	for _, entry := range raw {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			out[""] = ""
			continue
		}
		out[strings.ToLower(strings.TrimSpace(key))] = value
	}
	return out
}

func validateMetadataConfig(cfg reflowMetadataConfig) error {
	// Resolved-plan enum + rule validation is owned by pkg/reflow so library
	// callers share the same invariants the CLI enforces.
	if err := cfg.Validate(); err != nil {
		return err
	}
	// The file-destination sidecar suffix is a destination-provider concern the
	// CLI carries; it is not part of the engine's metadata plan validation.
	if !strings.HasPrefix(cfg.MetadataSidecarSuffix, ".") {
		return fmt.Errorf("metadata-sidecar-suffix must start with a leading dot")
	}
	if strings.Contains(cfg.MetadataSidecarSuffix, "/") {
		return fmt.Errorf("metadata-sidecar-suffix must not contain '/'")
	}
	return nil
}

func resolveSourceConfig(cmd *cobra.Command) (reflowSourceConfig, error) {
	cfg := reflowSourceConfig{
		Symlinks:        reflowSymlinkSkip,
		Hidden:          reflowHiddenSkip,
		OnSourceFailure: reflowSourceFailSkip,
	}
	if cmd != nil && cmd.Flags().Changed("symlinks") {
		cfg.Symlinks = reflowSymlinks
	} else if viper.IsSet("source.symlinks") {
		cfg.Symlinks = viper.GetString("source.symlinks")
	}
	if cmd != nil && cmd.Flags().Changed("hidden") {
		cfg.Hidden = reflowHidden
	} else if viper.IsSet("source.hidden") {
		cfg.Hidden = viper.GetString("source.hidden")
	}
	if cmd != nil && cmd.Flags().Changed("exclude") {
		cfg.Excludes = append([]string(nil), reflowExcludes...)
	} else if viper.IsSet("source.exclude") {
		cfg.Excludes = viper.GetStringSlice("source.exclude")
	}
	if cmd != nil && cmd.Flags().Changed("preserve-mode") {
		cfg.PreserveMode = reflowPreserve
	} else if viper.IsSet("source.preserve_mode") {
		cfg.PreserveMode = viper.GetBool("source.preserve_mode")
	}
	if cmd != nil && cmd.Flags().Changed("on-source-failure") {
		cfg.OnSourceFailure = reflowSrcFailure
	} else if viper.IsSet("source.on_failure") {
		cfg.OnSourceFailure = viper.GetString("source.on_failure")
	}
	cfg.Symlinks = strings.TrimSpace(strings.ToLower(cfg.Symlinks))
	cfg.Hidden = strings.TrimSpace(strings.ToLower(cfg.Hidden))
	cfg.OnSourceFailure = strings.TrimSpace(strings.ToLower(cfg.OnSourceFailure))
	for i := range cfg.Excludes {
		cfg.Excludes[i] = filepath.ToSlash(strings.TrimSpace(cfg.Excludes[i]))
	}
	return cfg, validateSourceConfig(cfg)
}

func validateSourceConfig(cfg reflowSourceConfig) error {
	switch cfg.Symlinks {
	case "", reflowSymlinkSkip, reflowSymlinkFollow:
	default:
		if cfg.Symlinks == "preserve" {
			return fmt.Errorf("--symlinks=preserve is not supported in v1; deferred to follow-up brief covering symlink-aware provider capability + preserve-mode escape policy. Use --symlinks=skip or --symlinks=follow")
		}
		return fmt.Errorf("symlinks must be one of: skip, follow")
	}
	switch cfg.Hidden {
	case "", reflowHiddenSkip, reflowHiddenInclude:
	default:
		return fmt.Errorf("hidden must be one of: skip, include")
	}
	switch cfg.OnSourceFailure {
	case "", reflowSourceFailSkip, reflowSourceFailFail:
	default:
		if cfg.OnSourceFailure == "quarantine" {
			return fmt.Errorf("--on-source-failure=quarantine is not supported in v1; source failures have no readable body to quarantine. Use --on-source-failure=skip|fail")
		}
		return fmt.Errorf("on-source-failure must be one of: skip, fail")
	}
	for _, pattern := range cfg.Excludes {
		if pattern == "" {
			continue
		}
		if _, err := pathMatch(pattern, "x"); err != nil {
			return fmt.Errorf("invalid exclude glob %q: %w", pattern, err)
		}
	}
	return nil
}

func pathMatch(pattern, name string) (bool, error) {
	return filepath.Match(pattern, name)
}

func ensureMetadataCapability(dst provider.Provider, destProvider string, cfg reflowMetadataConfig) error {
	if !cfg.RequiresCapability() {
		return nil
	}
	_, err := providerdispatch.RequireCapability[provider.MetadataAwarePutter](dst, operationTransferReflow, destProvider, "metadata-aware PUT (MetadataAwarePutter)")
	if err != nil {
		return fmt.Errorf("%w required by %s", err, strings.Join(metadataCapabilityFlags(cfg), ", "))
	}
	return nil
}

func ensureCollisionCapability(dst provider.Provider, destProvider string, cfg collisionConfig) error {
	if cfg.Mode != reflowCollisionSrcNew {
		return nil
	}
	if destProvider == "" {
		destProvider = "destination"
	}
	if destProvider == string(provider.ProviderGCS) {
		return fmt.Errorf("%s provider %q does not support ConditionalPutter.IfMatchETag required by --on-collision=%s", operationTransferReflow, destProvider, reflowCollisionSrcNew)
	}
	_, err := providerdispatch.RequireCapability[provider.ConditionalPutter](dst, operationTransferReflow, destProvider, "ConditionalPutter.IfMatchETag")
	if err != nil {
		return fmt.Errorf("%w required by --on-collision=%s", err, reflowCollisionSrcNew)
	}
	return nil
}

func cloneMetadataMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func metadataSetRawFromMap(values map[string]string) []string {
	if len(values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key+"="+values[key])
	}
	return out
}

func metadataSourceRuleRaw(rules []metadataSourceKeyRule) []string {
	if len(rules) == 0 {
		return nil
	}
	out := make([]string, 0, len(rules))
	for _, rule := range rules {
		out = append(out, rule.Raw)
	}
	sort.Strings(out)
	return out
}

func metadataDerivedRuleRaw(rules []metadataDerivedRule) []string {
	if len(rules) == 0 {
		return nil
	}
	out := make([]string, 0, len(rules))
	for _, rule := range rules {
		out = append(out, rule.Raw)
	}
	sort.Strings(out)
	return out
}
