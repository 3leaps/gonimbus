package cmd

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/3leaps/gonimbus/internal/providerdispatch"
	"github.com/3leaps/gonimbus/pkg/provider"
	providerfile "github.com/3leaps/gonimbus/pkg/provider/file"
	reflowpkg "github.com/3leaps/gonimbus/pkg/reflow"
)

type collisionConfig struct {
	Mode             string
	QuarantinePrefix string
	DeprecatedLog    bool
}

type reflowMetadataConfig struct {
	Policy                  string
	Set                     map[string]string
	SourceKeyRules          []metadataSourceKeyRule
	DerivedRules            []metadataDerivedRule
	OnMissingSource         string
	PreserveContentType     bool
	DestinationStorageClass string
	MetadataSidecarSuffix   string
}

type reflowSourceConfig struct {
	Symlinks        string
	Hidden          string
	Excludes        []string
	PreserveMode    bool
	OnSourceFailure string
}

type metadataBudgetError struct {
	OverLimitKeys []string
	PairLimit     int
	TotalBytes    int
	TotalLimit    int
	Count         int
}

func (e *metadataBudgetError) Error() string {
	return fmt.Sprintf("user metadata exceeds S3 metadata budget: keys=%v count=%d total_bytes=%d total_limit=%d pair_limit=%d", e.OverLimitKeys, e.Count, e.TotalBytes, e.TotalLimit, e.PairLimit)
}

func (e *metadataBudgetError) details() map[string]any {
	return map[string]any{
		"metadata_keys":        append([]string(nil), e.OverLimitKeys...),
		"metadata_count":       e.Count,
		"metadata_total_bytes": e.TotalBytes,
		"metadata_total_limit": e.TotalLimit,
		"metadata_pair_limit":  e.PairLimit,
	}
}

func (c reflowMetadataConfig) needsSourceHead() bool {
	return c.Policy == metadataPolicyPreserve || c.Policy == metadataPolicyMerge || c.PreserveContentType || c.DestinationStorageClass == storageClassPropagate || c.hasPerObjectRules()
}

func (c reflowMetadataConfig) requiresCapability() bool {
	return c.Policy == metadataPolicyPreserve || c.Policy == metadataPolicyMerge || len(c.Set) > 0 || c.hasPerObjectRules() || c.PreserveContentType || c.DestinationStorageClass != ""
}

func (c reflowMetadataConfig) capabilityFlags() []string {
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

func (c reflowMetadataConfig) runConfig() *reflowpkg.MetadataRunConfig {
	if !c.requiresCapability() && c.MetadataSidecarSuffix == providerfile.DefaultMetadataSidecarSuffix && c.OnMissingSource == metadataMissingSkip {
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
	if c.hasPerObjectRules() || c.OnMissingSource != metadataMissingSkip {
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
		cfg.SourceKeyRules, err = parseMetadataSourceKeyRules(reflowMetaSrcKeys)
	} else if viper.IsSet("metadata.set_from_source_key") {
		cfg.SourceKeyRules, err = parseMetadataSourceKeyRules(viper.GetStringSlice("metadata.set_from_source_key"))
	}
	if err != nil {
		return cfg, err
	}
	if cmd != nil && cmd.Flags().Changed("metadata-set-from-source-derived") {
		cfg.DerivedRules, err = parseMetadataDerivedRules(reflowMetaDerived)
	} else if viper.IsSet("metadata.set_from_source_derived") {
		cfg.DerivedRules, err = parseMetadataDerivedRules(viper.GetStringSlice("metadata.set_from_source_derived"))
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
	switch cfg.Policy {
	case metadataPolicyClear, metadataPolicyPreserve, metadataPolicyMerge:
		// ok
	default:
		return fmt.Errorf("metadata-policy must be one of: clear, preserve, merge")
	}
	if _, bad := cfg.Set[""]; bad {
		return fmt.Errorf("metadata-set entries must use non-empty key=value syntax")
	}
	for key := range cfg.Set {
		if strings.ContainsAny(key, " \t\r\n=") {
			return fmt.Errorf("metadata-set keys must be non-empty tokens without whitespace or '='")
		}
	}
	switch cfg.OnMissingSource {
	case "", metadataMissingSkip, metadataMissingFail, metadataMissingEmpty:
	default:
		return fmt.Errorf("metadata-on-missing-source must be one of: skip, fail, empty")
	}
	if err := validatePerObjectMetadataRules(cfg.SourceKeyRules, cfg.DerivedRules); err != nil {
		return err
	}
	if !strings.HasPrefix(cfg.MetadataSidecarSuffix, ".") {
		return fmt.Errorf("metadata-sidecar-suffix must start with a leading dot")
	}
	if strings.Contains(cfg.MetadataSidecarSuffix, "/") {
		return fmt.Errorf("metadata-sidecar-suffix must not contain '/'")
	}
	if cfg.DestinationStorageClass == "" {
		return nil
	}
	if strings.EqualFold(cfg.DestinationStorageClass, storageClassPropagate) {
		return nil
	}
	if !isValidPutStorageClass(strings.ToUpper(cfg.DestinationStorageClass)) {
		return fmt.Errorf("destination-storage-class is not a valid PUT target")
	}
	return nil
}

func isValidPutStorageClass(storageClass string) bool {
	switch strings.ToUpper(strings.TrimSpace(storageClass)) {
	case "STANDARD", "INTELLIGENT_TIERING", "STANDARD_IA", "ONEZONE_IA", "GLACIER_IR", "REDUCED_REDUNDANCY":
		return true
	default:
		return false
	}
}

func validateMetadataBudget(metadata map[string]string) error {
	if len(metadata) == 0 {
		return nil
	}
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	total := 0
	overLimitKeys := make([]string, 0)
	for _, key := range keys {
		pairBytes := len([]byte(key)) + len([]byte(metadata[key]))
		total += pairBytes
		if pairBytes > metadataMaxPairBytes {
			overLimitKeys = append(overLimitKeys, key)
		}
	}
	if total > metadataMaxTotalBytes {
		overLimitKeys = append(overLimitKeys, keys...)
	}
	if len(overLimitKeys) == 0 {
		return nil
	}
	overLimitKeys = uniqueSortedStrings(overLimitKeys)
	return &metadataBudgetError{
		OverLimitKeys: overLimitKeys,
		PairLimit:     metadataMaxPairBytes,
		TotalBytes:    total,
		TotalLimit:    metadataMaxTotalBytes,
		Count:         len(metadata),
	}
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

func uniqueSortedStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	sort.Strings(values)
	out := values[:0]
	for _, value := range values {
		if len(out) == 0 || out[len(out)-1] != value {
			out = append(out, value)
		}
	}
	return out
}

func ensureMetadataCapability(dst provider.Provider, destProvider string, cfg reflowMetadataConfig) error {
	if !cfg.requiresCapability() {
		return nil
	}
	_, err := providerdispatch.RequireCapability[provider.MetadataAwarePutter](dst, operationTransferReflow, destProvider, "metadata-aware PUT (MetadataAwarePutter)")
	if err != nil {
		return fmt.Errorf("%w required by %s", err, strings.Join(cfg.capabilityFlags(), ", "))
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

func (c reflowMetadataConfig) putOptions(source *provider.ObjectMeta) (provider.PutOptions, error) {
	var opts provider.PutOptions
	switch c.Policy {
	case metadataPolicyPreserve:
		userMeta, err := canonicalizeSourceMetadata(source)
		if err != nil {
			return opts, err
		}
		opts.UserMetadata = userMeta
	case metadataPolicyMerge:
		userMeta, err := canonicalizeSourceMetadata(source)
		if err != nil {
			return opts, err
		}
		opts.UserMetadata = userMeta
	case metadataPolicyClear:
	}
	if err := c.applyPerObjectMetadata(&opts, source); err != nil {
		return opts, err
	}
	if len(c.Set) > 0 {
		if opts.UserMetadata == nil {
			opts.UserMetadata = map[string]string{}
		}
		for key, value := range c.Set {
			opts.UserMetadata[key] = value
		}
	}
	if c.PreserveContentType {
		if source == nil {
			return opts, fmt.Errorf("source metadata is required to preserve content type")
		}
		opts.ContentType = source.ContentType
	}
	if c.DestinationStorageClass != "" {
		if c.DestinationStorageClass == storageClassPropagate {
			if source == nil {
				return opts, fmt.Errorf("source metadata is required to propagate storage class")
			}
			storageClass := source.StorageClass
			if storageClass == "" {
				storageClass = "STANDARD"
			}
			storageClass = strings.ToUpper(storageClass)
			if !isValidPutStorageClass(storageClass) {
				return opts, fmt.Errorf("source storage class is not a valid PUT target")
			}
			opts.StorageClass = storageClass
		} else {
			opts.StorageClass = strings.ToUpper(c.DestinationStorageClass)
		}
	}
	return opts, nil
}

func canonicalizeSourceMetadata(source *provider.ObjectMeta) (map[string]string, error) {
	if source == nil {
		return nil, fmt.Errorf("source metadata is required for metadata-policy")
	}
	out := make(map[string]string, len(source.Metadata))
	seenOriginal := make(map[string]string, len(source.Metadata))
	for key, value := range source.Metadata {
		canon := strings.ToLower(strings.TrimSpace(key))
		if canon == "" {
			continue
		}
		if first, ok := seenOriginal[canon]; ok && first != key {
			keys := []string{first, key}
			sort.Strings(keys)
			return nil, &sourceMetadataCollisionError{Keys: keys}
		}
		seenOriginal[canon] = key
		out[canon] = value
	}
	return out, nil
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
