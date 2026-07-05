package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	gfconfig "github.com/fulmenhq/gofulmen/config"
	"github.com/spf13/viper"
)

const (
	appDataRootSourceEnv      = "GONIMBUS_DATA_DIR"
	appDataRootSourceEnvAlias = "GONIMBUS_DATA_ROOT"
	appDataRootSourceConfig   = "config.data_root"
	appDataRootSourceXDG      = "XDG_DATA_HOME"
	appDataRootSourcePlatform = "platform default"
)

type appDataClass string

const (
	appDataClassIndexes              appDataClass = "indexes"
	appDataClassIndexBuildJobs       appDataClass = "index-build-jobs"
	appDataClassOperationCheckpoints appDataClass = "operation-checkpoints"
	appDataClassCrawlJournals        appDataClass = "crawl-journals"
	appDataClassSegmentCache         appDataClass = "segment-cache"
)

var appDataClassPaths = map[appDataClass][]string{
	appDataClassIndexes:              {"indexes"},
	appDataClassIndexBuildJobs:       {"jobs", "index-build"},
	appDataClassOperationCheckpoints: {"operation-checkpoints"},
	appDataClassCrawlJournals:        {"journals", "crawl"},
	appDataClassSegmentCache:         {"cache", "segments"},
}

type appDataRootResolution struct {
	Dir      string
	Source   string
	Explicit bool
}

func resolveAppDataRoot() (appDataRootResolution, error) {
	identity := GetAppIdentity()
	if identity == nil || strings.TrimSpace(identity.ConfigName) == "" {
		return appDataRootResolution{}, fmt.Errorf("app identity is not available")
	}
	return resolveAppDataRootForConfigName(identity.ConfigName)
}

func resolveAppDataRootForConfigName(configName string) (appDataRootResolution, error) {
	configName = strings.TrimSpace(configName)
	if configName == "" {
		return appDataRootResolution{}, fmt.Errorf("app config name is required")
	}

	if value, source, ok := lookupAppDataRootEnv(); ok {
		return resolveAppDataRootCandidate(value, source, true)
	}
	if value, ok := lookupAppDataRootConfig(); ok {
		return resolveAppDataRootCandidate(value, appDataRootSourceConfig, true)
	}

	dir := gfconfig.GetAppDataDir(configName)
	source := appDataRootSourcePlatform
	if strings.TrimSpace(os.Getenv("XDG_DATA_HOME")) != "" {
		source = appDataRootSourceXDG
	}
	return resolveAppDataRootCandidate(dir, source, false)
}

func lookupAppDataRootEnv() (string, string, bool) {
	if value, ok := os.LookupEnv(appDataRootSourceEnv); ok && strings.TrimSpace(value) != "" {
		return value, appDataRootSourceEnv, true
	}
	if value, ok := os.LookupEnv(appDataRootSourceEnvAlias); ok && strings.TrimSpace(value) != "" {
		return value, appDataRootSourceEnvAlias, true
	}
	return "", "", false
}

func lookupAppDataRootConfig() (string, bool) {
	for _, key := range []string{"data_root", "data_dir"} {
		if !viper.IsSet(key) {
			continue
		}
		value := strings.TrimSpace(viper.GetString(key))
		if value != "" {
			return value, true
		}
	}
	return "", false
}

func resolveExplicitAppDataRoot(raw string, source string) (appDataRootResolution, error) {
	return resolveAppDataRootCandidate(raw, source, true)
}

func resolveAppDataRootCandidate(raw string, source string, explicit bool) (appDataRootResolution, error) {
	dir, err := normalizeAppDataRoot(raw)
	if err != nil {
		return appDataRootResolution{}, fmt.Errorf("resolve data root from %s: %w", source, err)
	}
	if root, ok := findGitRepositoryRoot(dir); ok {
		return appDataRootResolution{}, fmt.Errorf("data root from %s must be outside git working tree %s", source, root)
	}
	return appDataRootResolution{Dir: dir, Source: source, Explicit: explicit}, nil
}

func appDataPath(class appDataClass, more ...string) (string, error) {
	resolved, err := resolveAppDataRoot()
	if err != nil {
		return "", err
	}
	parts, ok := appDataClassPaths[class]
	if !ok {
		return "", fmt.Errorf("unknown app data class %q", class)
	}
	joined := append([]string{resolved.Dir}, parts...)
	joined = append(joined, more...)
	return filepath.Join(joined...), nil
}

func normalizeAppDataRoot(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("path is empty")
	}
	abs, err := filepath.Abs(filepath.Clean(raw))
	if err != nil {
		return "", err
	}
	return resolvePathForPolicy(abs)
}

func resolvePathForPolicy(path string) (string, error) {
	clean := filepath.Clean(path)
	if resolved, err := filepath.EvalSymlinks(clean); err == nil {
		return filepath.Abs(resolved)
	}

	parts := splitCleanPath(clean)
	for i := len(parts); i >= 0; i-- {
		prefix := joinCleanPath(parts[:i])
		if prefix == "" {
			continue
		}
		info, err := os.Lstat(prefix)
		if err != nil {
			continue
		}
		if !info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
			return "", fmt.Errorf("path component is not a directory: %s", prefix)
		}
		resolvedPrefix, err := filepath.EvalSymlinks(prefix)
		if err != nil {
			return "", err
		}
		suffix := joinCleanPath(parts[i:])
		if suffix == "" {
			return filepath.Abs(resolvedPrefix)
		}
		return filepath.Abs(filepath.Join(resolvedPrefix, suffix))
	}
	return filepath.Abs(clean)
}

func splitCleanPath(path string) []string {
	volume := filepath.VolumeName(path)
	rest := strings.TrimPrefix(path, volume)
	rest = strings.TrimPrefix(rest, string(filepath.Separator))
	if rest == "" {
		if volume != "" {
			return []string{volume + string(filepath.Separator)}
		}
		return []string{string(filepath.Separator)}
	}
	parts := strings.Split(rest, string(filepath.Separator))
	if filepath.IsAbs(path) {
		return append([]string{volume + string(filepath.Separator)}, parts...)
	}
	if volume != "" {
		return append([]string{volume}, parts...)
	}
	return parts
}

func joinCleanPath(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	return filepath.Join(parts...)
}

func findGitRepositoryRoot(start string) (string, bool) {
	dir, err := filepath.Abs(filepath.Clean(start))
	if err != nil {
		return "", false
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return dir, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}

func mkdirAppDataDir(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	return os.Chmod(path, 0o700)
}
