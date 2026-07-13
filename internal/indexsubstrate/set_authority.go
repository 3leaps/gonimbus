package indexsubstrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	// SetAuthorityDirectoryName is the package-owned stable authority directory
	// beside set-specific segment roots. GC must never treat it as a set target.
	SetAuthorityDirectoryName = ".gonimbus-set-authority"
	setAuthorityDocType       = "gonimbus.index.set_authority.v1"
)

var (
	ErrSetAuthorityHeld  = errors.New("index set authority held")
	ErrSetAuthorityLost  = errors.New("index set authority not held")
	ErrSetAuthorityScope = errors.New("index set authority does not authorize target")
)

// SetAuthority is the stable, whole-set cross-process exclusion primitive.
// Its lock file is outside the set-specific segment root, so quarantining that
// root cannot detach the held lock from the canonical writer pathname.
type SetAuthority struct {
	f              *os.File
	path           string
	segmentSetRoot string
	indexSetID     string
	holder         string
	released       bool
}

type setAuthorityDoc struct {
	Type       string    `json:"type"`
	IndexSetID string    `json:"index_set_id"`
	Holder     string    `json:"holder"`
	AcquiredAt time.Time `json:"acquired_at"`
}

// SetAuthorityRootForSegmentSet returns the stable authority root shared by
// every set beneath the same segment-cache parent.
func SetAuthorityRootForSegmentSet(segmentSetRoot string) (string, error) {
	root, err := canonicalizeSegmentSetRoot(segmentSetRoot)
	if err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(root), SetAuthorityDirectoryName), nil
}

// AcquireSetAuthority takes whole-set authority before any canonical substrate
// is opened or mutated. Acquisition is non-blocking; process exit releases the
// OS lock automatically.
func AcquireSetAuthority(ctx context.Context, segmentSetRoot, indexSetID, holder string) (*SetAuthority, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	canonicalRoot, err := canonicalizeSegmentSetRoot(segmentSetRoot)
	if err != nil {
		return nil, err
	}
	indexSetID = strings.TrimSpace(indexSetID)
	holder = strings.TrimSpace(holder)
	if err := validateSetAuthorityPart(indexSetID, "index_set_id"); err != nil {
		return nil, err
	}
	if holder == "" {
		return nil, fmt.Errorf("authority holder is required")
	}
	authorityRoot, err := SetAuthorityRootForSegmentSet(canonicalRoot)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(authorityRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create set authority root: %w", err)
	}
	rootInfo, err := os.Lstat(authorityRoot)
	if err != nil || rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return nil, fmt.Errorf("set authority root must be a real directory")
	}
	resolved, err := filepath.EvalSymlinks(authorityRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve set authority root: %w", err)
	}
	// Parent path aliases (notably /var -> /private/var on macOS) converge on
	// the same physical authority. The final authority directory itself was
	// already Lstat-verified as a real directory above.
	authorityRoot = filepath.Clean(resolved)
	root, err := os.OpenRoot(authorityRoot)
	if err != nil {
		return nil, fmt.Errorf("open set authority root: %w", err)
	}
	defer func() { _ = root.Close() }()
	name := indexSetID + ".lock"
	f, err := root.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open set authority: %w", err)
	}
	bound, err := f.Stat()
	if err != nil || !bound.Mode().IsRegular() {
		_ = f.Close()
		return nil, fmt.Errorf("set authority must be a regular file")
	}
	if err := lockFileExclusive(f); err != nil {
		_ = f.Close()
		if errors.Is(err, errLockWouldBlock) {
			return nil, fmt.Errorf("%w by concurrent set operation", ErrSetAuthorityHeld)
		}
		return nil, fmt.Errorf("lock set authority: %w", err)
	}
	named, err := root.Lstat(name)
	if err != nil || named.Mode()&os.ModeSymlink != 0 || !named.Mode().IsRegular() || !os.SameFile(bound, named) {
		_ = unlockFile(f)
		_ = f.Close()
		return nil, fmt.Errorf("set authority binding changed after lock")
	}
	_ = f.Truncate(0)
	_, _ = f.Seek(0, 0)
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	_ = enc.Encode(setAuthorityDoc{Type: setAuthorityDocType, IndexSetID: indexSetID, Holder: holder, AcquiredAt: time.Now().UTC()})
	_ = f.Sync()
	return &SetAuthority{
		f:              f,
		path:           filepath.Join(authorityRoot, name),
		segmentSetRoot: canonicalRoot,
		indexSetID:     indexSetID,
		holder:         holder,
	}, nil
}

func validateSetAuthorityPart(value, label string) error {
	if value == "" || value == "." || value == ".." || strings.ContainsAny(value, `/\\`) {
		return fmt.Errorf("%s is invalid", label)
	}
	return nil
}

func (a *SetAuthority) AssertHeld() error {
	if a == nil || a.released || a.f == nil {
		return ErrSetAuthorityLost
	}
	bound, err := a.f.Stat()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSetAuthorityLost, err)
	}
	named, err := os.Lstat(a.path)
	if err != nil || named.Mode()&os.ModeSymlink != 0 || !named.Mode().IsRegular() || !os.SameFile(bound, named) {
		return fmt.Errorf("%w: authority pathname no longer names the held lock", ErrSetAuthorityLost)
	}
	return nil
}

func (a *SetAuthority) AssertHeldFor(indexSetID, segmentSetRoot string) error {
	if err := a.AssertHeld(); err != nil {
		return err
	}
	root, err := canonicalizeSegmentSetRoot(segmentSetRoot)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSetAuthorityScope, err)
	}
	if strings.TrimSpace(indexSetID) != a.indexSetID || root != a.segmentSetRoot {
		return fmt.Errorf("%w: held set/root does not match requested authority", ErrSetAuthorityScope)
	}
	return nil
}

func (a *SetAuthority) Release() error {
	if a == nil || a.released {
		return nil
	}
	a.released = true
	var first error
	if err := unlockFile(a.f); err != nil {
		first = err
	}
	if err := a.f.Close(); err != nil && first == nil {
		first = err
	}
	a.f = nil
	return first
}

func (a *SetAuthority) SegmentSetRoot() string {
	if a == nil {
		return ""
	}
	return a.segmentSetRoot
}

func (a *SetAuthority) IndexSetID() string {
	if a == nil {
		return ""
	}
	return a.indexSetID
}

func (a *SetAuthority) Path() string {
	if a == nil {
		return ""
	}
	return a.path
}
