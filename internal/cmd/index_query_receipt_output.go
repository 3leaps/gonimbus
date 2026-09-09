package cmd

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/3leaps/gonimbus/pkg/provider"
)

const indexQueryReceiptTempPrefix = ".gonimbus-query-receipt-"

var (
	indexQueryReceiptBeforeLocalBind    func(directory string) error
	indexQueryReceiptBeforeLocalPublish func(tempPath, finalPath string) error
	indexQueryReceiptBeforeLocalCleanup func(tempPath string) error
)

type indexQueryReceiptLocalStage struct {
	root         *os.Root
	directory    string
	tempName     string
	finalName    string
	tempInfo     os.FileInfo
	committed    bool
	tempRemoved  bool
	rootIsClosed bool
}

func createIndexQueryReceiptOutputTemp(spec *outputDestSpec) (*os.File, *indexQueryReceiptLocalStage, error) {
	if spec == nil {
		return nil, nil, fmt.Errorf("output destination is required")
	}
	if spec.Provider != string(provider.ProviderFile) {
		file, err := os.CreateTemp("", "gonimbus-query-receipt-*.jsonl")
		if err != nil {
			return nil, nil, fmt.Errorf("create staged output: %w", err)
		}
		if err := file.Chmod(0o600); err != nil {
			_ = file.Close()
			_ = os.Remove(file.Name())
			return nil, nil, fmt.Errorf("restrict staged output: %w", err)
		}
		return file, nil, nil
	}

	// Receipt publication deliberately requires an existing directory. This
	// lets the entire create/inspect/link/remove sequence stay relative to one
	// retained directory capability without creating through an unbound path.
	if err := validateIndexQueryReceiptOutputDirectory(spec.BaseDir); err != nil {
		return nil, nil, err
	}
	if indexQueryReceiptBeforeLocalBind != nil {
		if err := indexQueryReceiptBeforeLocalBind(spec.BaseDir); err != nil {
			return nil, nil, err
		}
	}
	root, err := os.OpenRoot(spec.BaseDir)
	if err != nil {
		return nil, nil, fmt.Errorf("bind output directory: %w", err)
	}
	stage := &indexQueryReceiptLocalStage{
		root:      root,
		directory: spec.BaseDir,
		finalName: spec.Key,
	}
	if err := stage.verifyDirectoryBinding(); err != nil {
		_ = root.Close()
		return nil, nil, err
	}
	if err := stage.rejectExistingFinal(); err != nil {
		_ = root.Close()
		return nil, nil, err
	}
	file, err := stage.createTemp()
	if err != nil {
		_ = root.Close()
		return nil, nil, err
	}
	return file, stage, nil
}

func (s *indexQueryReceiptLocalStage) createTemp() (*os.File, error) {
	for range 10 {
		var nonce [12]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return nil, fmt.Errorf("create staged output name: %w", err)
		}
		name := indexQueryReceiptTempPrefix + hex.EncodeToString(nonce[:]) + ".tmp"
		file, err := s.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(err, os.ErrExist) || os.IsExist(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("create sibling staged output: %w", err)
		}
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			_ = s.root.Remove(name)
			return nil, fmt.Errorf("inspect staged output: %w", err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			_ = file.Close()
			_ = s.root.Remove(name)
			return nil, fmt.Errorf("staged output must be a regular 0600 file")
		}
		s.tempName = name
		s.tempInfo = info
		return file, nil
	}
	return nil, fmt.Errorf("create sibling staged output: temporary name collisions exhausted")
}

func (s *indexQueryReceiptLocalStage) Publish() ([]string, error) {
	if s == nil || s.root == nil || s.tempName == "" {
		return nil, fmt.Errorf("local output stage is incomplete")
	}
	if err := s.verifyDirectoryBinding(); err != nil {
		return nil, err
	}
	if err := s.verifyTemp(); err != nil {
		return nil, err
	}
	if err := s.rejectExistingFinal(); err != nil {
		return nil, err
	}
	if indexQueryReceiptBeforeLocalPublish != nil {
		if err := indexQueryReceiptBeforeLocalPublish(s.tempPath(), s.finalPath()); err != nil {
			return nil, err
		}
	}
	if err := s.verifyDirectoryBinding(); err != nil {
		return nil, err
	}
	if err := s.verifyTemp(); err != nil {
		return nil, err
	}
	if err := s.rejectExistingFinal(); err != nil {
		return nil, err
	}
	if err := s.root.Link(s.tempName, s.finalName); err != nil {
		if errors.Is(err, os.ErrExist) || os.IsExist(err) {
			return nil, fmt.Errorf("output path exists; refusing to replace: %s", s.finalPath())
		}
		return nil, fmt.Errorf("atomic no-replace output publish failed: %w", err)
	}

	// The final link is not an accepted commit until its directory entry is
	// synced. If that sync fails, remove the link so the same destination can be
	// retried instead of leaving a known non-authoritative success stream.
	if err := s.syncDirectory(); err != nil {
		rollbackErr := s.root.Remove(s.finalName)
		if rollbackErr == nil {
			_ = s.syncDirectory()
		}
		return nil, errors.Join(err, rollbackErr)
	}
	s.committed = true

	var warnings []string
	if indexQueryReceiptBeforeLocalCleanup != nil {
		if err := indexQueryReceiptBeforeLocalCleanup(s.tempPath()); err != nil {
			warnings = append(warnings, fmt.Sprintf("warning: committed output staging cleanup deferred: %v", err))
			return warnings, nil
		}
	}
	if err := s.root.Remove(s.tempName); err != nil {
		warnings = append(warnings, fmt.Sprintf("warning: committed output staging cleanup deferred: %v", err))
		return warnings, nil
	}
	s.tempRemoved = true
	if err := s.syncDirectory(); err != nil {
		warnings = append(warnings, fmt.Sprintf("warning: committed output staging cleanup sync failed: %v", err))
	}
	return warnings, nil
}

func (s *indexQueryReceiptLocalStage) Cleanup() error {
	if s == nil || s.root == nil || s.rootIsClosed {
		return nil
	}
	var cleanupErr error
	if !s.tempRemoved && s.tempName != "" {
		if err := s.root.Remove(s.tempName); err != nil && !errors.Is(err, os.ErrNotExist) && !os.IsNotExist(err) {
			if s.committed {
				_, _ = fmt.Fprintf(os.Stderr, "warning: committed output staging residue remains: %v\n", err)
			} else {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove staged output: %w", err))
			}
		} else {
			s.tempRemoved = true
			if err := s.syncDirectory(); err != nil && !s.committed {
				cleanupErr = errors.Join(cleanupErr, err)
			}
		}
	}
	if err := s.root.Close(); err != nil {
		if s.committed {
			_, _ = fmt.Fprintf(os.Stderr, "warning: close committed output directory handle: %v\n", err)
		} else {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("close output directory handle: %w", err))
		}
	}
	s.rootIsClosed = true
	return cleanupErr
}

func (s *indexQueryReceiptLocalStage) verifyDirectoryBinding() error {
	if err := validateIndexQueryReceiptOutputDirectory(s.directory); err != nil {
		return err
	}
	bound, err := s.root.Stat(".")
	if err != nil {
		return fmt.Errorf("inspect bound output directory: %w", err)
	}
	named, err := os.Lstat(s.directory)
	if err != nil {
		return fmt.Errorf("inspect named output directory: %w", err)
	}
	if named.Mode()&os.ModeSymlink != 0 || !named.IsDir() || !os.SameFile(bound, named) {
		return fmt.Errorf("output directory binding changed or is not a no-follow directory")
	}
	return nil
}

func (s *indexQueryReceiptLocalStage) verifyTemp() error {
	info, err := s.root.Lstat(s.tempName)
	if err != nil {
		return fmt.Errorf("inspect staged output: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() ||
		info.Mode().Perm() != 0o600 || !os.SameFile(s.tempInfo, info) {
		return fmt.Errorf("staged output binding changed or is not a regular 0600 file")
	}
	return nil
}

func (s *indexQueryReceiptLocalStage) rejectExistingFinal() error {
	info, err := s.root.Lstat(s.finalName)
	if errors.Is(err, os.ErrNotExist) || os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect output path: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("output path is a symlink; refusing no-follow publication: %s", s.finalPath())
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("output path is not a regular file: %s", s.finalPath())
	}
	return fmt.Errorf("output path exists; refusing to replace: %s", s.finalPath())
}

func (s *indexQueryReceiptLocalStage) syncDirectory() error {
	dir, err := s.root.Open(".")
	if err != nil {
		return fmt.Errorf("open output directory for sync: %w", err)
	}
	defer func() { _ = dir.Close() }()
	if err := dir.Sync(); err != nil {
		if runtime.GOOS == "windows" {
			return nil
		}
		return fmt.Errorf("sync output directory: %w", err)
	}
	return nil
}

func (s *indexQueryReceiptLocalStage) tempPath() string {
	return filepath.Join(s.directory, s.tempName)
}

func (s *indexQueryReceiptLocalStage) finalPath() string {
	return filepath.Join(s.directory, s.finalName)
}

func validateIndexQueryReceiptOutputDirectory(path string) error {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve output directory: %w", err)
	}
	volume := filepath.VolumeName(absolute)
	remainder := absolute[len(volume):]
	current := volume
	if filepath.IsAbs(absolute) {
		current += string(filepath.Separator)
		remainder = remainder[1:]
	}
	for _, component := range splitIndexQueryReceiptPath(remainder) {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("receipt output directory must already exist: %s", path)
			}
			return fmt.Errorf("inspect output directory component: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			if runtime.GOOS == "darwin" && isDarwinSystemOutputAlias(current) {
				continue
			}
			return fmt.Errorf("output directory uses a symlink: %s", current)
		}
		if !info.IsDir() {
			return fmt.Errorf("output directory component is not a directory: %s", current)
		}
	}
	return nil
}

func splitIndexQueryReceiptPath(path string) []string {
	var components []string
	for path != "" && path != "." {
		dir, base := filepath.Split(path)
		if base != "" {
			components = append([]string{base}, components...)
		}
		next := filepath.Clean(dir)
		if next == path {
			break
		}
		path = next
	}
	return components
}

func isDarwinSystemOutputAlias(path string) bool {
	clean := filepath.Clean(path)
	var expected string
	switch clean {
	case "/etc":
		expected = "/private/etc"
	case "/tmp":
		expected = "/private/tmp"
	case "/var":
		expected = "/private/var"
	default:
		return false
	}
	resolved, err := filepath.EvalSymlinks(clean)
	return err == nil && filepath.Clean(resolved) == expected
}
