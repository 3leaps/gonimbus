package opcheckpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	SchemaVersion = 1

	StatusFailedResumable = "failed-resumable"
	StatusResuming        = "resuming"
	StatusSuccess         = "success"

	defaultRootName    = "operation-checkpoints"
	checkpointFileName = "checkpoint.json"
	leaseFileName      = "resume.lease.json"
	reclaimLockName    = "resume.lease.reclaim.lock"
	reclaimLockTTL     = 30 * time.Second
)

var (
	ErrPathInsideForbiddenRoot = errors.New("checkpoint path resolves inside a forbidden worktree")
	ErrLeaseHeld               = errors.New("resume lease is already held")
	ErrIdentityMismatch        = errors.New("checkpoint identity mismatch")
	ErrCredentialMaterial      = errors.New("checkpoint contains credential material")
	errInvalidReclaimLock      = errors.New("invalid stale lease reclaim lock")
)

// Config controls the local operation-checkpoint store.
type Config struct {
	// RootDir is an explicit checkpoint root. Empty uses AppDataDir.
	RootDir string
	// AppDataDir is the application data directory used for default storage.
	AppDataDir string
	// ForbiddenRoots are repo/worktree roots where checkpoint artifacts must not
	// be written. Paths are resolved before comparison.
	ForbiddenRoots []string
}

// Store persists sensitive local operation checkpoints outside the repository.
type Store struct {
	root string
}

// Envelope is the shared checkpoint wrapper. Operation-specific serializers own
// Payload; the shared layer owns identity, status, error class, progress, and
// lease semantics.
type Envelope struct {
	SchemaVersion     int               `json:"schema_version"`
	Operation         string            `json:"operation"`
	RunID             string            `json:"run_id"`
	ConfigFingerprint string            `json:"config_fingerprint"`
	CheckpointID      string            `json:"checkpoint_id"`
	Status            string            `json:"status"`
	ErrorClass        ErrorClass        `json:"error_class,omitempty"`
	CreatedAt         time.Time         `json:"created_at"`
	UpdatedAt         time.Time         `json:"updated_at"`
	Progress          map[string]int64  `json:"progress,omitempty"`
	Payload           json.RawMessage   `json:"payload,omitempty"`
	Events            []CheckpointEvent `json:"events,omitempty"`
}

// CheckpointEvent preserves audit evidence across failed-resumable -> resuming
// -> success promotion without forcing every operation into index-run tables.
type CheckpointEvent struct {
	Type       string     `json:"type"`
	At         time.Time  `json:"at"`
	ErrorClass ErrorClass `json:"error_class,omitempty"`
	Detail     string     `json:"detail,omitempty"`
}

// Identity identifies the operation state that must match before resume may
// skip work or promote a run.
type Identity struct {
	Operation         string
	RunID             string
	ConfigFingerprint string
}

// Lease records an exclusive resume claim.
type Lease struct {
	RunID     string    `json:"run_id"`
	HolderID  string    `json:"holder_id"`
	ClaimedAt time.Time `json:"claimed_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

type reclaimLock struct {
	HolderID  string    `json:"holder_id"`
	ClaimedAt time.Time `json:"claimed_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

func Open(ctx context.Context, cfg Config) (*Store, error) {
	root, err := ResolveRoot(cfg)
	if err != nil {
		return nil, err
	}
	if err := mkdirSecure(root); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return &Store{root: root}, nil
}

func ResolveRoot(cfg Config) (string, error) {
	root := strings.TrimSpace(cfg.RootDir)
	if root == "" {
		appDataDir := strings.TrimSpace(cfg.AppDataDir)
		if appDataDir == "" {
			return "", fmt.Errorf("checkpoint app data dir is required")
		}
		root = filepath.Join(appDataDir, defaultRootName)
	}

	absRoot, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return "", fmt.Errorf("resolve checkpoint root: %w", err)
	}
	policyRoot, err := resolveForPolicy(absRoot)
	if err != nil {
		return "", err
	}
	for _, forbidden := range cfg.ForbiddenRoots {
		if insideRoot(policyRoot, forbidden) {
			return "", fmt.Errorf("%w: %s", ErrPathInsideForbiddenRoot, absRoot)
		}
	}
	return absRoot, nil
}

func (s *Store) RootDir() string {
	if s == nil {
		return ""
	}
	return s.root
}

func (s *Store) CheckpointPath(operation, runID string) (string, error) {
	dir, err := s.runDir(operation, runID)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, checkpointFileName), nil
}

func (s *Store) WriteCheckpoint(ctx context.Context, env Envelope) error {
	if err := validateEnvelope(env); err != nil {
		return err
	}
	env.SchemaVersion = SchemaVersion
	now := time.Now().UTC()
	if env.CreatedAt.IsZero() {
		env.CreatedAt = now
	}
	env.UpdatedAt = now
	if env.CheckpointID == "" {
		env.CheckpointID = checkpointID(env.Operation, env.RunID, env.ConfigFingerprint, env.UpdatedAt)
	}

	dir, err := s.ensureRunDir(env.Operation, env.RunID)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, checkpointFileName)
	data, err := marshalSecure(env)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return writeFileAtomic0600(dir, path, data)
}

func (s *Store) ReadCheckpoint(ctx context.Context, operation, runID string) (*Envelope, error) {
	path, err := s.CheckpointPath(operation, runID)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	data, err := os.ReadFile(path) // #nosec G304 -- path is rooted under the checkpoint store after operation/run validation.
	if err != nil {
		return nil, err
	}
	if err := validateNoCredentialMaterial(data); err != nil {
		return nil, err
	}
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("parse checkpoint: %w", err)
	}
	if err := validateEnvelope(env); err != nil {
		return nil, err
	}
	return &env, nil
}

func (s *Store) ValidateIdentity(env *Envelope, want Identity) error {
	if env == nil {
		return fmt.Errorf("checkpoint is nil")
	}
	if env.SchemaVersion != SchemaVersion ||
		env.Operation != want.Operation ||
		env.RunID != want.RunID ||
		env.ConfigFingerprint != want.ConfigFingerprint {
		return ErrIdentityMismatch
	}
	return nil
}

func (s *Store) ClaimLease(ctx context.Context, operation, runID, holderID string, ttl time.Duration) (*Lease, error) {
	holderID = cleanSegment(holderID)
	if holderID == "" {
		return nil, fmt.Errorf("lease holder_id is invalid")
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("lease ttl must be positive")
	}
	dir, err := s.ensureRunDir(operation, runID)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, leaseFileName)
	now := time.Now().UTC()
	lease := Lease{
		RunID:     runID,
		HolderID:  holderID,
		ClaimedAt: now,
		ExpiresAt: now.Add(ttl),
	}
	data, err := json.MarshalIndent(lease, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal lease: %w", err)
	}
	data = append(data, '\n')

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		published, err := publishLeaseFile(dir, path, data)
		if err != nil {
			return nil, err
		}
		if published {
			return &lease, nil
		}
		existing, readErr := readLease(path)
		if readErr != nil {
			if errors.Is(readErr, os.ErrNotExist) {
				continue
			}
			return nil, readErr
		}
		if time.Now().UTC().Before(existing.ExpiresAt) {
			return nil, ErrLeaseHeld
		}
		if err := reclaimStaleLease(ctx, dir, path, holderID); err != nil {
			return nil, err
		}
	}
}

func reclaimStaleLease(ctx context.Context, dir, path, holderID string) error {
	lockPath := filepath.Join(dir, reclaimLockName)
	lock, err := claimReclaimLock(dir, lockPath, holderID)
	if err != nil {
		return err
	}
	defer releaseReclaimLock(lockPath, lock)

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	existing, err := readLease(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if time.Now().UTC().Before(existing.ExpiresAt) {
		return ErrLeaseHeld
	}
	if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return fmt.Errorf("remove stale lease: %w", removeErr)
	}
	return nil
}

func claimReclaimLock(dir, path, holderID string) (*reclaimLock, error) {
	for {
		now := time.Now().UTC()
		lock := reclaimLock{
			HolderID:  holderID,
			ClaimedAt: now,
			ExpiresAt: now.Add(reclaimLockTTL),
		}
		data, err := json.MarshalIndent(lock, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("marshal stale lease reclaim lock: %w", err)
		}
		data = append(data, '\n')
		published, err := publishExclusiveFileAtomic0600(dir, path, reclaimLockName+".tmp.*", data)
		if err != nil {
			return nil, fmt.Errorf("claim stale lease reclaim lock: %w", err)
		}
		if published {
			return &lock, nil
		}
		existing, err := readReclaimLock(path)
		if err == nil && time.Now().UTC().Before(existing.ExpiresAt) {
			return nil, ErrLeaseHeld
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, errInvalidReclaimLock) {
			return nil, err
		}
		if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return nil, fmt.Errorf("remove stale lease reclaim lock: %w", removeErr)
		}
	}
}

func releaseReclaimLock(path string, want *reclaimLock) {
	if want == nil {
		return
	}
	existing, err := readReclaimLock(path)
	if err != nil {
		return
	}
	if existing.HolderID == want.HolderID &&
		existing.ClaimedAt.Equal(want.ClaimedAt) &&
		existing.ExpiresAt.Equal(want.ExpiresAt) {
		_ = os.Remove(path)
	}
}

func publishLeaseFile(dir, finalPath string, data []byte) (bool, error) {
	return publishExclusiveFileAtomic0600(dir, finalPath, leaseFileName+".tmp.*", data)
}

func publishExclusiveFileAtomic0600(dir, finalPath, pattern string, data []byte) (bool, error) {
	tmp, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return false, fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return false, fmt.Errorf("chmod temp file: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return false, fmt.Errorf("write temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return false, fmt.Errorf("close temp file: %w", err)
	}

	if err := os.Link(tmpName, finalPath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return false, nil
		}
		return false, fmt.Errorf("publish file: %w", err)
	}
	_ = os.Chmod(finalPath, 0o600)
	return true, nil
}

func (s *Store) ReleaseLease(operation string, lease Lease) error {
	if err := validateLeaseIdentity(lease); err != nil {
		return err
	}
	dir, err := s.runDir(operation, lease.RunID)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, leaseFileName)
	lockPath := filepath.Join(dir, reclaimLockName)
	lock, err := claimReclaimLock(dir, lockPath, lease.HolderID)
	if err != nil {
		return err
	}
	defer releaseReclaimLock(lockPath, lock)

	current, err := readLease(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if !sameLease(current, &lease) {
		return ErrLeaseHeld
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove lease: %w", err)
	}
	return nil
}

func validateLeaseIdentity(lease Lease) error {
	if cleanSegment(lease.RunID) == "" {
		return fmt.Errorf("lease run_id is invalid")
	}
	if cleanSegment(lease.HolderID) == "" {
		return fmt.Errorf("lease holder_id is invalid")
	}
	if lease.ClaimedAt.IsZero() || lease.ExpiresAt.IsZero() {
		return fmt.Errorf("lease identity timestamps are required")
	}
	return nil
}

func sameLease(a, b *Lease) bool {
	if a == nil || b == nil {
		return false
	}
	return a.RunID == b.RunID &&
		a.HolderID == b.HolderID &&
		a.ClaimedAt.Equal(b.ClaimedAt) &&
		a.ExpiresAt.Equal(b.ExpiresAt)
}

func FingerprintConfig(v any) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("marshal fingerprint config: %w", err)
	}
	if err := validateNoCredentialMaterial(data); err != nil {
		return "", err
	}
	var normalized any
	if err := json.Unmarshal(data, &normalized); err != nil {
		return "", fmt.Errorf("normalize fingerprint config: %w", err)
	}
	buf := &bytes.Buffer{}
	writeCanonicalJSON(buf, normalized)
	sum := sha256.Sum256(buf.Bytes())
	return hex.EncodeToString(sum[:]), nil
}

func validateEnvelope(env Envelope) error {
	if env.SchemaVersion != 0 && env.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported checkpoint schema_version: %d", env.SchemaVersion)
	}
	if cleanSegment(env.Operation) == "" {
		return fmt.Errorf("checkpoint operation is required")
	}
	if cleanSegment(env.RunID) == "" {
		return fmt.Errorf("checkpoint run_id is required")
	}
	if strings.TrimSpace(env.ConfigFingerprint) == "" {
		return fmt.Errorf("checkpoint config_fingerprint is required")
	}
	if env.Status == "" {
		return fmt.Errorf("checkpoint status is required")
	}
	return nil
}

func marshalSecure(env Envelope) ([]byte, error) {
	data, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal checkpoint: %w", err)
	}
	data = append(data, '\n')
	if err := validateNoCredentialMaterial(data); err != nil {
		return nil, err
	}
	return data, nil
}

func validateNoCredentialMaterial(data []byte) error {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return fmt.Errorf("scan checkpoint for credential material: %w", err)
	}
	if containsCredentialMaterial(v) {
		return ErrCredentialMaterial
	}
	return nil
}

func containsCredentialMaterial(v any) bool {
	switch x := v.(type) {
	case map[string]any:
		for key, value := range x {
			if credentialKey(key) || containsCredentialMaterial(value) {
				return true
			}
		}
	case []any:
		for _, value := range x {
			if containsCredentialMaterial(value) {
				return true
			}
		}
	case string:
		return credentialString(x)
	}
	return false
}

func credentialKey(key string) bool {
	k := strings.ToLower(strings.ReplaceAll(key, "_", ""))
	k = strings.ReplaceAll(k, "-", "")
	needles := []string{
		"accesskey",
		"secretkey",
		"sessiontoken",
		"refreshtoken",
		"bearertoken",
		"authtoken",
		"signedurl",
		"clientsecret",
		"providercache",
	}
	for _, needle := range needles {
		if strings.Contains(k, needle) {
			return true
		}
	}
	return false
}

func credentialString(value string) bool {
	lower := strings.ToLower(value)
	needles := []string{
		"x-amz-signature=",
		"x-amz-credential=",
		"x-amz-security-token=",
		"x-goog-signature=",
		"x-goog-credential=",
		"x-goog-security-token=",
		"authorization: bearer ",
		"authtoken=",
		"aws_secret_access_key",
		"aws_session_token",
		"sharedaccesssignature",
	}
	for _, needle := range needles {
		if strings.Contains(lower, needle) {
			return true
		}
	}
	if strings.HasPrefix(lower, "sig=") || strings.Contains(lower, "?sig=") || strings.Contains(lower, "&sig=") {
		return true
	}
	if u, err := url.Parse(value); err == nil && u.Scheme != "" && u.Host != "" {
		if u.User != nil {
			if u.User.Username() != "" {
				return true
			}
			if password, ok := u.User.Password(); ok && password != "" {
				return true
			}
		}
		for key := range u.Query() {
			normalized := strings.ToLower(key)
			switch normalized {
			case "x-amz-signature", "x-amz-credential", "x-amz-security-token",
				"x-goog-signature", "x-goog-credential", "x-goog-security-token",
				"sig":
				return true
			}
		}
	}
	return false
}

func (s *Store) ensureRunDir(operation, runID string) (string, error) {
	dir, err := s.runDir(operation, runID)
	if err != nil {
		return "", err
	}
	if err := mkdirSecure(dir); err != nil {
		return "", err
	}
	return dir, nil
}

func (s *Store) runDir(operation, runID string) (string, error) {
	if s == nil || s.root == "" {
		return "", fmt.Errorf("checkpoint store is nil")
	}
	op := cleanSegment(operation)
	id := cleanSegment(runID)
	if op == "" {
		return "", fmt.Errorf("checkpoint operation is required")
	}
	if id == "" {
		return "", fmt.Errorf("checkpoint run_id is required")
	}
	return filepath.Join(s.root, op, id), nil
}

func cleanSegment(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || value == "." || value == ".." {
		return ""
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') ||
			r == '-' || r == '_' || r == '.' {
			continue
		}
		return ""
	}
	return value
}

func mkdirSecure(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create checkpoint directory: %w", err)
	}
	return os.Chmod(path, 0o700)
}

func writeFileAtomic0600(dir, finalPath string, data []byte) error {
	tmp, err := os.CreateTemp(dir, filepath.Base(finalPath)+".tmp.*")
	if err != nil {
		return fmt.Errorf("create checkpoint temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod checkpoint temp file: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write checkpoint temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close checkpoint temp file: %w", err)
	}
	if err := os.Rename(tmpName, finalPath); err != nil {
		return fmt.Errorf("rename checkpoint file: %w", err)
	}
	return os.Chmod(finalPath, 0o600)
}

func readLease(path string) (*Lease, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- path is rooted under the checkpoint store after operation/run validation.
	if err != nil {
		return nil, err
	}
	var lease Lease
	if err := json.Unmarshal(data, &lease); err != nil {
		return nil, fmt.Errorf("parse lease: %w", err)
	}
	return &lease, nil
}

func readReclaimLock(path string) (*reclaimLock, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- path is rooted under the checkpoint store after operation/run validation.
	if err != nil {
		return nil, err
	}
	var lock reclaimLock
	if err := json.Unmarshal(data, &lock); err != nil {
		return nil, fmt.Errorf("%w: %v", errInvalidReclaimLock, err)
	}
	if cleanSegment(lock.HolderID) == "" || lock.ClaimedAt.IsZero() || lock.ExpiresAt.IsZero() {
		return nil, errInvalidReclaimLock
	}
	return &lock, nil
}

func insideRoot(path, root string) bool {
	root = strings.TrimSpace(root)
	if root == "" {
		return false
	}
	absRoot, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return false
	}
	policyRoot, err := resolveForPolicy(absRoot)
	if err != nil {
		policyRoot = absRoot
	}
	rel, err := filepath.Rel(policyRoot, path)
	if err != nil {
		return false
	}
	return rel == "." || (!strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != "..")
}

func resolveForPolicy(path string) (string, error) {
	clean := filepath.Clean(path)
	if resolved, err := filepath.EvalSymlinks(clean); err == nil {
		return filepath.Abs(resolved)
	}

	parts := splitPath(clean)
	for i := len(parts); i >= 0; i-- {
		prefix := joinPath(parts[:i])
		if prefix == "" {
			continue
		}
		info, err := os.Lstat(prefix)
		if err != nil {
			continue
		}
		if !info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
			return "", fmt.Errorf("checkpoint path component is not a directory: %s", prefix)
		}
		resolvedPrefix, err := filepath.EvalSymlinks(prefix)
		if err != nil {
			return "", err
		}
		resolvedInfo, err := os.Stat(resolvedPrefix)
		if err != nil {
			return "", err
		}
		if !resolvedInfo.IsDir() {
			return "", fmt.Errorf("checkpoint path component is not a directory: %s", prefix)
		}
		remainder := parts[i:]
		if len(remainder) == 0 {
			return filepath.Abs(resolvedPrefix)
		}
		joined := filepath.Join(append([]string{resolvedPrefix}, remainder...)...)
		return filepath.Abs(joined)
	}

	return filepath.Abs(clean)
}

func splitPath(path string) []string {
	volume := filepath.VolumeName(path)
	rest := strings.TrimPrefix(path, volume)
	rest = strings.TrimPrefix(rest, string(filepath.Separator))
	parts := []string{}
	if volume != "" {
		parts = append(parts, volume+string(filepath.Separator))
	} else if filepath.IsAbs(path) {
		parts = append(parts, string(filepath.Separator))
	}
	for _, part := range strings.Split(rest, string(filepath.Separator)) {
		if part != "" {
			parts = append(parts, part)
		}
	}
	return parts
}

func joinPath(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 && (parts[0] == string(filepath.Separator) || strings.HasSuffix(parts[0], string(filepath.Separator))) {
		return parts[0]
	}
	return filepath.Join(parts...)
}

func checkpointID(operation, runID, fingerprint string, at time.Time) string {
	sum := sha256.Sum256([]byte(operation + "\x00" + runID + "\x00" + fingerprint + "\x00" + at.Format(time.RFC3339Nano)))
	return "chk_" + hex.EncodeToString(sum[:8])
}

func writeCanonicalJSON(buf *bytes.Buffer, value any) {
	switch v := value.(type) {
	case map[string]any:
		buf.WriteByte('{')
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for i, key := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			keyJSON, _ := json.Marshal(key)
			buf.Write(keyJSON)
			buf.WriteByte(':')
			writeCanonicalJSON(buf, v[key])
		}
		buf.WriteByte('}')
	case []any:
		buf.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				buf.WriteByte(',')
			}
			writeCanonicalJSON(buf, item)
		}
		buf.WriteByte(']')
	default:
		data, _ := json.Marshal(v)
		buf.Write(data)
	}
}
