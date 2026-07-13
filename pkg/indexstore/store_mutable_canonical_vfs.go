package indexstore

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"modernc.org/libc"
	sqlite3 "modernc.org/sqlite/lib"
)

var (
	canonicalSQLiteVFSToken atomic.Uint64
	canonicalSQLiteVFSMu    sync.RWMutex
	canonicalSQLiteVFSes    = map[uintptr]*canonicalSQLiteVFSState{}
	canonicalSQLiteVFSFiles = map[uintptr]*canonicalSQLiteVFSFile{}
)

type canonicalSQLiteVFS struct {
	name  string
	cname uintptr
	tls   *libc.TLS
	vfs   uintptr
	state *canonicalSQLiteVFSState
	once  sync.Once
	err   error
}

type canonicalSQLiteVFSState struct {
	path           string
	bound          *os.File
	directory      *os.File
	beforeMutation func() error
	hooks          mutableCanonicalOpenHooks
	base           uintptr
	self           uintptr
	mainFile       uintptr
	mu             sync.Mutex
	firstFailure   error
	artifactMu     sync.Mutex
	artifacts      map[string]canonicalSQLiteArtifactBinding
	reservations   map[string]*canonicalSQLiteArtifactReservation
}

type canonicalSQLiteVFSFile struct {
	state         *canonicalSQLiteVFSState
	original      *sqlite3.Tsqlite3_io_methods
	name          string
	deleteOnClose bool
	reservation   *canonicalSQLiteArtifactReservation
}

type canonicalSQLiteArtifactReservation struct {
	path    string
	file    *os.File
	binding canonicalSQLiteArtifactBinding
}

type canonicalSQLiteSidecarExpectation uint8

const (
	canonicalSQLiteSidecarsUnspecified canonicalSQLiteSidecarExpectation = iota
	canonicalSQLiteSidecarsMustBeAbsent
)

func registerCanonicalSQLiteVFS(path string, bound *os.File, beforeMutation func() error, sidecars canonicalSQLiteSidecarExpectation, hooks mutableCanonicalOpenHooks) (*canonicalSQLiteVFS, error) {
	tls := libc.NewTLS()
	base := sqlite3.Xsqlite3_vfs_find(tls, 0)
	if base == 0 {
		tls.Close()
		return nil, errors.New("locate default SQLite VFS")
	}
	abs, err := resolveCanonicalSQLitePath(path)
	if err != nil {
		tls.Close()
		return nil, fmt.Errorf("resolve canonical SQLite VFS path: %w", err)
	}
	name := fmt.Sprintf("gonimbus-authority-%x", canonicalSQLiteVFSToken.Add(1))
	cname, err := libc.CString(name)
	if err != nil {
		tls.Close()
		return nil, err
	}
	baseVFS := canonicalSQLiteVFSStruct(base)
	vfs := libc.Xmalloc(tls, uint64(unsafe.Sizeof(sqlite3.Tsqlite3_vfs{})))
	if vfs == 0 {
		libc.Xfree(tls, cname)
		tls.Close()
		return nil, errors.New("allocate canonical SQLite VFS")
	}
	*canonicalSQLiteVFSStruct(vfs) = *baseVFS
	registeredVFS := canonicalSQLiteVFSStruct(vfs)
	registeredVFS.FpNext = 0
	registeredVFS.FzName = cname
	registeredVFS.FxOpen = ccgoFunctionPointer(canonicalSQLiteVFSOpen)
	registeredVFS.FxDelete = ccgoFunctionPointer(canonicalSQLiteVFSDelete)
	registeredVFS.FxAccess = ccgoFunctionPointer(canonicalSQLiteVFSAccess)
	if sidecars != canonicalSQLiteSidecarsMustBeAbsent {
		libc.Xfree(tls, vfs)
		libc.Xfree(tls, cname)
		tls.Close()
		return nil, errors.New("canonical SQLite VFS requires an explicit sidecar expectation")
	}
	presentSidecars, err := SQLiteTransactionSidecars(abs)
	if err != nil {
		libc.Xfree(tls, vfs)
		libc.Xfree(tls, cname)
		tls.Close()
		return nil, fmt.Errorf("verify canonical SQLite sidecar handoff: %w", err)
	}
	if len(presentSidecars) != 0 {
		libc.Xfree(tls, vfs)
		libc.Xfree(tls, cname)
		tls.Close()
		return nil, fmt.Errorf("canonical SQLite transaction sidecars appeared after validation: %s", strings.Join(presentSidecars, ", "))
	}
	directory, err := openCanonicalSQLiteDirectory(filepath.Dir(abs))
	if err != nil {
		libc.Xfree(tls, vfs)
		libc.Xfree(tls, cname)
		tls.Close()
		return nil, fmt.Errorf("bind canonical SQLite directory: %w", err)
	}
	artifacts := make(map[string]canonicalSQLiteArtifactBinding, 3)
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		artifactPath := abs + suffix
		// The ordinary writer API accepts only the caller-validated absent
		// epoch. A sidecar that appears after the scan must fail the first VFS
		// access/open check rather than becoming a new starting truth.
		artifacts[artifactPath] = canonicalSQLiteArtifactBinding{}
	}
	state := &canonicalSQLiteVFSState{
		path:           abs,
		bound:          bound,
		directory:      directory,
		beforeMutation: beforeMutation,
		hooks:          hooks,
		base:           base,
		artifacts:      artifacts,
		reservations:   make(map[string]*canonicalSQLiteArtifactReservation),
	}
	state.self = vfs
	canonicalSQLiteVFSMu.Lock()
	canonicalSQLiteVFSes[state.self] = state
	canonicalSQLiteVFSMu.Unlock()
	if rc := sqlite3.Xsqlite3_vfs_register(tls, state.self, 0); rc != sqlite3.SQLITE_OK {
		canonicalSQLiteVFSMu.Lock()
		delete(canonicalSQLiteVFSes, state.self)
		canonicalSQLiteVFSMu.Unlock()
		libc.Xfree(tls, cname)
		libc.Xfree(tls, vfs)
		tls.Close()
		return nil, errors.Join(fmt.Errorf("register canonical SQLite VFS: result code %d", rc), directory.Close())
	}
	return &canonicalSQLiteVFS{name: name, cname: cname, tls: tls, vfs: vfs, state: state}, nil
}

func (v *canonicalSQLiteVFS) Close() error {
	if v == nil {
		return nil
	}
	v.once.Do(func() {
		if rc := sqlite3.Xsqlite3_vfs_unregister(v.tls, v.vfs); rc != sqlite3.SQLITE_OK {
			v.err = fmt.Errorf("unregister canonical SQLite VFS: result code %d", rc)
		}
		canonicalSQLiteVFSMu.Lock()
		delete(canonicalSQLiteVFSes, v.vfs)
		canonicalSQLiteVFSMu.Unlock()
		libc.Xfree(v.tls, v.cname)
		libc.Xfree(v.tls, v.vfs)
		v.tls.Close()
		if v.state != nil {
			v.err = errors.Join(v.err, v.state.releaseAllArtifactReservations())
		}
		if v.state != nil && v.state.directory != nil {
			v.err = errors.Join(v.err, v.state.directory.Close())
			v.state.directory = nil
		}
	})
	return v.err
}

func withSQLiteVFS(dsn, name string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	q := u.Query()
	q.Set("vfs", name)
	u.RawQuery = q.Encode()
	return u.String()
}

func (s *canonicalSQLiteVFSState) fail(err error) error {
	if err == nil {
		return nil
	}
	s.mu.Lock()
	if s.firstFailure == nil {
		s.firstFailure = err
	}
	s.mu.Unlock()
	return err
}

func (s *canonicalSQLiteVFSState) failure() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstFailure
}

func (s *canonicalSQLiteVFSState) guard(file uintptr, name, operation string) error {
	if err := s.beforeMutation(); err != nil {
		return s.fail(fmt.Errorf("canonical SQLite VFS %s authority: %w", operation, err))
	}
	if s.mainFile == 0 {
		return nil
	}
	if err := attestCanonicalSQLiteVFSFile(s.mainFile, s.bound, s.path); err != nil {
		return s.fail(fmt.Errorf("canonical SQLite VFS %s main-file binding: %w", operation, err))
	}
	if file != 0 && file != s.mainFile && isCanonicalSQLiteArtifact(s.path, name) {
		if err := s.bindOpenedArtifact(file, name); err != nil {
			return s.fail(fmt.Errorf("canonical SQLite VFS %s sidecar binding: %w", operation, err))
		}
	} else if file == 0 {
		if err := s.verifyArtifactEpoch(name); err != nil {
			return s.fail(fmt.Errorf("canonical SQLite VFS %s sidecar epoch: %w", operation, err))
		}
	}
	return nil
}

func (s *canonicalSQLiteVFSState) verifyArtifactEpoch(name string) error {
	clean := filepath.Clean(name)
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()
	binding, ok := s.artifacts[clean]
	if !ok {
		recognized, unknown := classifyCanonicalSQLiteArtifact(s.path, clean)
		if unknown {
			return fmt.Errorf("refusing unrecognized canonical SQLite transaction artifact %s", filepath.Base(clean))
		}
		if !recognized {
			return nil
		}
		captured, err := captureCanonicalSQLiteArtifactBinding(clean)
		if err != nil {
			return err
		}
		if captured.present {
			return fmt.Errorf("canonical SQLite transaction artifact %s appeared after the absent-sidecar handoff", filepath.Base(clean))
		}
		s.artifacts[clean] = captured
		return nil
	}
	return verifyCanonicalSQLiteArtifactBinding(clean, binding)
}

func (s *canonicalSQLiteVFSState) reserveArtifact(name string, create bool) (*canonicalSQLiteArtifactReservation, error) {
	clean := filepath.Clean(name)
	recognized, unknown := classifyCanonicalSQLiteArtifact(s.path, clean)
	if unknown {
		return nil, fmt.Errorf("refusing unrecognized canonical SQLite transaction artifact %s", filepath.Base(clean))
	}
	if !recognized {
		return nil, nil
	}
	if _, err := canonicalSQLiteArtifactRelativeName(s.path, clean); err != nil {
		return nil, err
	}

	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()
	binding, ok := s.artifacts[clean]
	if !ok {
		captured, err := captureCanonicalSQLiteArtifactBinding(clean)
		if err != nil {
			return nil, err
		}
		if captured.present {
			return nil, fmt.Errorf("canonical SQLite transaction artifact %s appeared after the absent-sidecar handoff", filepath.Base(clean))
		}
		binding = captured
		s.artifacts[clean] = captured
	}
	if err := verifyCanonicalSQLiteArtifactBinding(clean, binding); err != nil {
		return nil, err
	}
	if binding.present {
		return nil, nil
	}
	if !create {
		return nil, fmt.Errorf("refusing to open unowned canonical SQLite transaction artifact %s", filepath.Base(clean))
	}
	if s.hooks.beforeSidecarReservation != nil {
		if err := s.hooks.beforeSidecarReservation(clean); err != nil {
			return nil, fmt.Errorf("before canonical SQLite sidecar reservation: %w", err)
		}
	}
	file, next, err := reserveCanonicalSQLiteArtifact(s.directory, filepath.Dir(s.path), clean)
	if err != nil {
		return nil, fmt.Errorf("exclusively reserve canonical SQLite transaction artifact %s: %w", filepath.Base(clean), err)
	}
	s.artifacts[clean] = next
	return &canonicalSQLiteArtifactReservation{path: clean, file: file, binding: next}, nil
}

func (s *canonicalSQLiteVFSState) releaseArtifactReservation(reservation *canonicalSQLiteArtifactReservation, remove bool) error {
	if reservation == nil {
		return nil
	}
	var errs []error
	if reservation.file != nil {
		errs = append(errs, reservation.file.Close())
		reservation.file = nil
	}
	if remove {
		errs = append(errs, s.removeArtifactEpoch(reservation.path, reservation.binding))
	}
	return errors.Join(errs...)
}

func (s *canonicalSQLiteVFSState) retainArtifactReservation(reservation *canonicalSQLiteArtifactReservation) error {
	if reservation == nil {
		return nil
	}
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()
	if prior := s.reservations[reservation.path]; prior != nil {
		return fmt.Errorf("canonical SQLite artifact reservation is already retained")
	}
	s.reservations[reservation.path] = reservation
	return nil
}

func (s *canonicalSQLiteVFSState) releaseRetainedArtifactReservation(path string) error {
	clean := filepath.Clean(path)
	s.artifactMu.Lock()
	reservation := s.reservations[clean]
	delete(s.reservations, clean)
	s.artifactMu.Unlock()
	return s.releaseArtifactReservation(reservation, false)
}

func (s *canonicalSQLiteVFSState) releaseAllArtifactReservations() error {
	s.artifactMu.Lock()
	reservations := make([]*canonicalSQLiteArtifactReservation, 0, len(s.reservations))
	for path, reservation := range s.reservations {
		reservations = append(reservations, reservation)
		delete(s.reservations, path)
	}
	s.artifactMu.Unlock()
	var errs []error
	for _, reservation := range reservations {
		errs = append(errs, s.releaseArtifactReservation(reservation, false))
	}
	return errors.Join(errs...)
}

func (s *canonicalSQLiteVFSState) exactEpochRemovalHooks() exactEpochRemovalHooks {
	return exactEpochRemovalHooks{
		afterCapture: s.hooks.afterExactEpochCapture,
		afterAttest:  s.hooks.afterExactEpochAttest,
	}
}

func (s *canonicalSQLiteVFSState) removeArtifactEpoch(path string, binding canonicalSQLiteArtifactBinding) error {
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()
	clean := filepath.Clean(path)
	if s.hooks.beforeExactEpochRemoval != nil {
		if err := s.hooks.beforeExactEpochRemoval(clean); err != nil {
			return fmt.Errorf("before exact-epoch canonical SQLite removal: %w", err)
		}
	}
	if err := removeCanonicalSQLiteArtifactWithHooks(s.directory, filepath.Dir(s.path), clean, binding, s.exactEpochRemovalHooks()); err != nil {
		return err
	}
	s.artifacts[clean] = canonicalSQLiteArtifactBinding{}
	return nil
}

func (s *canonicalSQLiteVFSState) removeBoundArtifact(path string) error {
	clean := filepath.Clean(path)
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()
	binding, ok := s.artifacts[clean]
	if !ok || !binding.present {
		return nil
	}
	if s.hooks.beforeExactEpochRemoval != nil {
		if err := s.hooks.beforeExactEpochRemoval(clean); err != nil {
			return fmt.Errorf("before exact-epoch canonical SQLite removal: %w", err)
		}
	}
	if err := removeCanonicalSQLiteArtifactWithHooks(s.directory, filepath.Dir(s.path), clean, binding, s.exactEpochRemovalHooks()); err != nil {
		return err
	}
	s.artifacts[clean] = canonicalSQLiteArtifactBinding{}
	return nil
}

func (s *canonicalSQLiteVFSState) bindOpenedArtifact(file uintptr, name string) error {
	clean := filepath.Clean(name)
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()
	binding, ok := s.artifacts[clean]
	if !ok {
		return attestCanonicalSQLiteVFSNamedFile(file, clean)
	}
	next, err := bindCanonicalSQLiteArtifactFile(file, clean, binding)
	if err != nil {
		return err
	}
	s.artifacts[clean] = next
	return nil
}

func (s *canonicalSQLiteVFSState) bindSharedMemory(file uintptr) error {
	path := s.path + "-shm"
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()
	binding := s.artifacts[path]
	next, err := bindCanonicalSQLiteSharedMemory(file, path, binding)
	if err != nil {
		return err
	}
	s.artifacts[path] = next
	return nil
}

func (s *canonicalSQLiteVFSState) recordArtifactDeleted(name string) error {
	clean := filepath.Clean(name)
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()
	if _, ok := s.artifacts[clean]; !ok {
		return nil
	}
	binding, err := captureCanonicalSQLiteArtifactBinding(clean)
	if err != nil {
		return err
	}
	if binding.present {
		return fmt.Errorf("SQLite reported deletion but %s remains", filepath.Base(clean))
	}
	s.artifacts[clean] = binding
	return nil
}

func isCanonicalSQLiteArtifact(path, name string) bool {
	recognized, _ := classifyCanonicalSQLiteArtifact(path, name)
	return recognized
}

func classifyCanonicalSQLiteArtifact(path, name string) (recognized, unknown bool) {
	if name == "" {
		return false, false
	}
	cleanPath := filepath.Clean(path)
	cleanName := filepath.Clean(name)
	if suffix, ok := canonicalSQLiteArtifactSuffix(cleanPath, cleanName); ok {
		if isSQLiteTransactionSidecarSuffix(suffix) {
			return true, false
		}
		if strings.HasPrefix(suffix, "-") {
			return false, true
		}
	}
	return false, false
}

// canonicalSQLiteArtifactSuffix returns the sidecar suffix for name relative to
// the main database path. Matching prefers the full pathname and falls back to
// basename identity so dual concrete paths for the same directory (for example
// /var vs /private/var before resolution) still classify correctly.
func canonicalSQLiteArtifactSuffix(path, name string) (string, bool) {
	cleanPath := filepath.Clean(path)
	cleanName := filepath.Clean(name)
	if suffix := strings.TrimPrefix(cleanName, cleanPath); suffix != cleanName && suffix != "" {
		return suffix, true
	}
	mainBase := filepath.Base(cleanPath)
	nameBase := filepath.Base(cleanName)
	if suffix := strings.TrimPrefix(nameBase, mainBase); suffix != nameBase && suffix != "" {
		return suffix, true
	}
	return "", false
}

// resolveCanonicalSQLiteOpenArtifact maps an xOpen request onto the connection-
// owned main path plus a known sidecar suffix. WAL and main rollback journals
// are selected from SQLite open-type flags (the same strategy as SHM); other
// transaction artifacts fall back to classified names under the bound base.
func (s *canonicalSQLiteVFSState) resolveCanonicalSQLiteOpenArtifact(name string, flags int32) (string, bool, error) {
	switch {
	case flags&sqlite3.SQLITE_OPEN_WAL != 0:
		return s.path + "-wal", true, nil
	case flags&sqlite3.SQLITE_OPEN_MAIN_JOURNAL != 0:
		return s.path + "-journal", true, nil
	}
	return s.resolveCanonicalSQLiteArtifactName(name)
}

func (s *canonicalSQLiteVFSState) resolveCanonicalSQLiteArtifactName(name string) (string, bool, error) {
	if name == "" {
		return "", false, nil
	}
	clean := filepath.Clean(name)
	recognized, unknown := classifyCanonicalSQLiteArtifact(s.path, clean)
	if unknown {
		return "", false, fmt.Errorf("refusing unrecognized canonical SQLite transaction artifact %s", filepath.Base(clean))
	}
	if !recognized {
		return "", false, nil
	}
	suffix, ok := canonicalSQLiteArtifactSuffix(s.path, clean)
	if !ok || !isSQLiteTransactionSidecarSuffix(suffix) {
		return "", false, fmt.Errorf("refusing non-canonical SQLite transaction artifact %s", filepath.Base(clean))
	}
	// Always key ownership by the retained main path + suffix so dual path
	// strings and SQLite zName variants share one reservation epoch.
	return s.path + suffix, true, nil
}

func canonicalSQLiteArtifactRelativeName(path, name string) (string, error) {
	cleanPath := filepath.Clean(path)
	cleanName := filepath.Clean(name)
	base := filepath.Base(cleanName)
	if base == "." || base == string(filepath.Separator) || base == filepath.Base(cleanPath) {
		return "", fmt.Errorf("invalid canonical SQLite transaction artifact name")
	}
	// Directory identity is enforced by openat/NtCreateFile against the retained
	// directory handle; only require the basename to be a recognized sidecar of
	// the main database name so dual concrete path strings remain accepted.
	recognized, unknown := classifyCanonicalSQLiteArtifact(cleanPath, cleanName)
	if unknown {
		return "", fmt.Errorf("refusing unrecognized canonical SQLite transaction artifact %s", base)
	}
	if !recognized {
		return "", fmt.Errorf("refusing non-canonical SQLite transaction artifact %s", base)
	}
	suffix, ok := canonicalSQLiteArtifactSuffix(cleanPath, cleanName)
	if !ok {
		return "", fmt.Errorf("refusing non-canonical SQLite transaction artifact %s", base)
	}
	return filepath.Base(cleanPath) + suffix, nil
}

func canonicalSQLiteVFSStateFor(pVFS uintptr) *canonicalSQLiteVFSState {
	canonicalSQLiteVFSMu.RLock()
	state := canonicalSQLiteVFSes[pVFS]
	canonicalSQLiteVFSMu.RUnlock()
	return state
}

func canonicalSQLiteVFSFileFor(pFile uintptr) *canonicalSQLiteVFSFile {
	canonicalSQLiteVFSMu.RLock()
	file := canonicalSQLiteVFSFiles[pFile]
	canonicalSQLiteVFSMu.RUnlock()
	return file
}

func canonicalSQLiteVFSOpen(tls *libc.TLS, pVFS, zName, pFile uintptr, flags int32, pOutFlags uintptr) int32 {
	state := canonicalSQLiteVFSStateFor(pVFS)
	if state == nil {
		return sqlite3.SQLITE_CANTOPEN
	}
	name := ""
	if zName != 0 {
		name = libc.GoString(zName)
	}

	var reservation *canonicalSQLiteArtifactReservation
	recognizedArtifact := false
	artifactName := name
	if flags&sqlite3.SQLITE_OPEN_MAIN_DB == 0 {
		resolved, recognized, err := state.resolveCanonicalSQLiteOpenArtifact(name, flags)
		if err != nil {
			_ = state.fail(fmt.Errorf("canonical SQLite VFS open sidecar classification: %w", err))
			return sqlite3.SQLITE_CANTOPEN
		}
		recognizedArtifact = recognized
		if recognized {
			artifactName = resolved
		}
		if err := state.guard(0, artifactName, "open "+filepath.Base(artifactName)); err != nil {
			return sqlite3.SQLITE_CANTOPEN
		}
		if recognizedArtifact {
			reservation, err = state.reserveArtifact(artifactName, flags&sqlite3.SQLITE_OPEN_CREATE != 0)
			if err != nil {
				_ = state.fail(fmt.Errorf("canonical SQLite VFS open sidecar reservation: %w", err))
				return sqlite3.SQLITE_CANTOPEN
			}
		}
	}
	delegateFlags := flags
	if reservation != nil {
		// The wrapper already performed the exclusive creation. SQLite must
		// reopen that exact object: repeating CREATE/EXCL would either reject
		// the reservation or re-enter the base VFS create path (which can
		// re-chmod the live epoch to the main-database mode).
		delegateFlags &^= sqlite3.SQLITE_OPEN_EXCLUSIVE | sqlite3.SQLITE_OPEN_CREATE
	}
	deleteOnClose := recognizedArtifact && flags&sqlite3.SQLITE_OPEN_DELETEONCLOSE != 0
	if deleteOnClose {
		// Keep the name until the wrapper has attested SQLite's exact handle.
		// xClose removes only the bound epoch through the retained directory.
		delegateFlags &^= sqlite3.SQLITE_OPEN_DELETEONCLOSE
	}
	rc := callSQLiteVFSOpen(canonicalSQLiteVFSStruct(state.base).FxOpen, tls, state.base, zName, pFile, delegateFlags, pOutFlags)
	if rc != sqlite3.SQLITE_OK {
		if err := state.releaseArtifactReservation(reservation, true); err != nil {
			_ = state.fail(fmt.Errorf("clean failed canonical SQLite sidecar reservation: %w", err))
		}
		return rc
	}
	original := canonicalSQLiteFileStruct(pFile).FpMethods
	if original == 0 {
		cause := errors.New("canonical SQLite base VFS returned an open file without methods")
		cause = errors.Join(cause, state.releaseArtifactReservation(reservation, true))
		_ = state.fail(cause)
		return sqlite3.SQLITE_CANTOPEN
	}
	entry := &canonicalSQLiteVFSFile{state: state, original: canonicalSQLiteIOMethodsStruct(original), name: artifactName, deleteOnClose: deleteOnClose}
	canonicalSQLiteVFSMu.Lock()
	canonicalSQLiteVFSFiles[pFile] = entry
	canonicalSQLiteVFSMu.Unlock()
	canonicalSQLiteFileStruct(pFile).FpMethods = uintptr(unsafe.Pointer(&canonicalSQLiteIOMethods))

	if flags&sqlite3.SQLITE_OPEN_MAIN_DB != 0 {
		if state.hooks.afterDriverOpen != nil {
			if err := state.hooks.afterDriverOpen(); err != nil {
				cause := closeRefusedCanonicalSQLiteVFSOpen(tls, pFile, entry, fmt.Errorf("after canonical SQLite driver open: %w", err))
				_ = state.fail(cause)
				return sqlite3.SQLITE_CANTOPEN
			}
		}
		if err := attestCanonicalSQLiteVFSFile(pFile, state.bound, state.path); err != nil {
			cause := closeRefusedCanonicalSQLiteVFSOpen(tls, pFile, entry, err)
			_ = state.fail(cause)
			return sqlite3.SQLITE_CANTOPEN
		}
		state.mainFile = pFile
		if state.hooks.afterConnectionAttestation != nil {
			if err := state.hooks.afterConnectionAttestation(); err != nil {
				cause := closeRefusedCanonicalSQLiteVFSOpen(tls, pFile, entry, fmt.Errorf("after canonical SQLite connection attestation: %w", err))
				_ = state.fail(cause)
				return sqlite3.SQLITE_CANTOPEN
			}
		}
	}
	if err := state.guard(pFile, artifactName, "open "+filepath.Base(artifactName)); err != nil {
		cause := closeRefusedCanonicalSQLiteVFSOpen(tls, pFile, entry, err)
		if reservation != nil {
			cause = errors.Join(cause, state.releaseArtifactReservation(reservation, true))
		} else if deleteOnClose {
			cause = errors.Join(cause, state.removeBoundArtifact(artifactName))
		}
		_ = state.fail(cause)
		return sqlite3.SQLITE_CANTOPEN
	}
	// Retain the create-new handle until SQLite closes its matching handle.
	// Besides preserving the exact owned object, this avoids closing a second
	// descriptor while SQLite might hold process-scoped locks on that file.
	entry.reservation = reservation
	return sqlite3.SQLITE_OK
}

func closeRefusedCanonicalSQLiteVFSOpen(tls *libc.TLS, pFile uintptr, file *canonicalSQLiteVFSFile, cause error) error {
	if file == nil || file.state == nil || file.original == nil {
		return cause
	}
	canonicalSQLiteVFSMu.Lock()
	delete(canonicalSQLiteVFSFiles, pFile)
	canonicalSQLiteVFSMu.Unlock()
	if file.state.mainFile == pFile {
		file.state.mainFile = 0
	}
	canonicalSQLiteFileStruct(pFile).FpMethods = uintptr(unsafe.Pointer(file.original))
	if rc := callSQLiteIOClose(file.original.FxClose, tls, pFile); rc != sqlite3.SQLITE_OK {
		cause = errors.Join(cause, fmt.Errorf("close refused canonical SQLite VFS file: result code %d", rc))
	}
	return cause
}

func canonicalSQLiteVFSDelete(tls *libc.TLS, pVFS, zName uintptr, syncDir int32) int32 {
	state := canonicalSQLiteVFSStateFor(pVFS)
	if state == nil {
		return sqlite3.SQLITE_IOERR_DELETE
	}
	name := libc.GoString(zName)
	artifactName, recognized, err := state.resolveCanonicalSQLiteArtifactName(name)
	if err != nil {
		_ = state.fail(fmt.Errorf("canonical SQLite VFS delete classification: %w", err))
		return sqlite3.SQLITE_IOERR_DELETE
	}
	guardName := name
	if recognized {
		guardName = artifactName
	}
	if err := state.guard(0, guardName, "delete "+filepath.Base(guardName)); err != nil {
		return sqlite3.SQLITE_IOERR_DELETE
	}
	if recognized {
		// Never pathname-delete a classified transaction artifact through the
		// base VFS: remove only the exact reserved epoch via the directory-
		// relative capture primitive.
		if err := state.removeBoundArtifact(artifactName); err != nil {
			_ = state.fail(fmt.Errorf("canonical SQLite VFS exact-epoch delete: %w", err))
			return sqlite3.SQLITE_IOERR_DELETE
		}
		if err := state.recordArtifactDeleted(artifactName); err != nil {
			_ = state.fail(fmt.Errorf("canonical SQLite VFS delete postcondition: %w", err))
			return sqlite3.SQLITE_IOERR_DELETE
		}
		return sqlite3.SQLITE_OK
	}
	base := canonicalSQLiteVFSStruct(state.base)
	return callSQLiteVFSDelete(base.FxDelete, tls, state.base, zName, syncDir)
}

func canonicalSQLiteVFSAccess(tls *libc.TLS, pVFS, zName uintptr, flags int32, pResOut uintptr) int32 {
	state := canonicalSQLiteVFSStateFor(pVFS)
	if state == nil {
		return sqlite3.SQLITE_IOERR_ACCESS
	}
	name := libc.GoString(zName)
	artifactName, recognized, err := state.resolveCanonicalSQLiteArtifactName(name)
	if err != nil {
		_ = state.fail(fmt.Errorf("canonical SQLite VFS access classification: %w", err))
		return sqlite3.SQLITE_IOERR_ACCESS
	}
	guardName := name
	if recognized {
		guardName = artifactName
	}
	if err := state.guard(0, guardName, "access "+filepath.Base(guardName)); err != nil {
		return sqlite3.SQLITE_IOERR_ACCESS
	}
	base := canonicalSQLiteVFSStruct(state.base)
	return callSQLiteVFSAccess(base.FxAccess, tls, state.base, zName, flags, pResOut)
}

var canonicalSQLiteIOMethods = sqlite3.Tsqlite3_io_methods{
	FiVersion:               3,
	FxClose:                 ccgoFunctionPointer(canonicalSQLiteIOClose),
	FxRead:                  ccgoFunctionPointer(canonicalSQLiteIORead),
	FxWrite:                 ccgoFunctionPointer(canonicalSQLiteIOWrite),
	FxTruncate:              ccgoFunctionPointer(canonicalSQLiteIOTruncate),
	FxSync:                  ccgoFunctionPointer(canonicalSQLiteIOSync),
	FxFileSize:              ccgoFunctionPointer(canonicalSQLiteIOFileSize),
	FxLock:                  ccgoFunctionPointer(canonicalSQLiteIOLock),
	FxUnlock:                ccgoFunctionPointer(canonicalSQLiteIOUnlock),
	FxCheckReservedLock:     ccgoFunctionPointer(canonicalSQLiteIOCheckReservedLock),
	FxFileControl:           ccgoFunctionPointer(canonicalSQLiteIOFileControl),
	FxSectorSize:            ccgoFunctionPointer(canonicalSQLiteIOSectorSize),
	FxDeviceCharacteristics: ccgoFunctionPointer(canonicalSQLiteIODeviceCharacteristics),
	FxShmMap:                ccgoFunctionPointer(canonicalSQLiteIOShmMap),
	FxShmLock:               ccgoFunctionPointer(canonicalSQLiteIOShmLock),
	FxShmBarrier:            ccgoFunctionPointer(canonicalSQLiteIOShmBarrier),
	FxShmUnmap:              ccgoFunctionPointer(canonicalSQLiteIOShmUnmap),
	FxFetch:                 ccgoFunctionPointer(canonicalSQLiteIOFetch),
	FxUnfetch:               ccgoFunctionPointer(canonicalSQLiteIOUnfetch),
}

func canonicalSQLiteIOGuard(pFile uintptr, operation string) (*canonicalSQLiteVFSFile, int32) {
	file := canonicalSQLiteVFSFileFor(pFile)
	if file == nil || file.original == nil {
		return nil, sqlite3.SQLITE_IOERR
	}
	if err := file.state.guard(pFile, file.name, operation); err != nil {
		return file, sqlite3.SQLITE_IOERR
	}
	return file, sqlite3.SQLITE_OK
}

func canonicalSQLiteIOClose(tls *libc.TLS, pFile uintptr) int32 {
	file := canonicalSQLiteVFSFileFor(pFile)
	if file == nil || file.original == nil {
		return sqlite3.SQLITE_IOERR_CLOSE
	}
	canonicalSQLiteVFSMu.Lock()
	delete(canonicalSQLiteVFSFiles, pFile)
	canonicalSQLiteVFSMu.Unlock()
	if file.state.mainFile == pFile {
		file.state.mainFile = 0
	}
	canonicalSQLiteFileStruct(pFile).FpMethods = uintptr(unsafe.Pointer(file.original))
	rc := callSQLiteIOClose(file.original.FxClose, tls, pFile)
	if err := file.state.releaseArtifactReservation(file.reservation, false); err != nil {
		_ = file.state.fail(fmt.Errorf("release canonical SQLite sidecar reservation after close: %w", err))
		return sqlite3.SQLITE_IOERR_CLOSE
	}
	file.reservation = nil
	if file.deleteOnClose {
		if err := file.state.removeBoundArtifact(file.name); err != nil {
			_ = file.state.fail(fmt.Errorf("delete exact canonical SQLite close artifact: %w", err))
			return sqlite3.SQLITE_IOERR_CLOSE
		}
	}
	return rc
}

func canonicalSQLiteIORead(tls *libc.TLS, pFile, buf uintptr, amount int32, offset int64) int32 {
	file, rc := canonicalSQLiteIOGuard(pFile, "read")
	if rc != sqlite3.SQLITE_OK {
		return sqlite3.SQLITE_IOERR_READ
	}
	return callSQLiteIORead(file.original.FxRead, tls, pFile, buf, amount, offset)
}

func canonicalSQLiteIOWrite(tls *libc.TLS, pFile, buf uintptr, amount int32, offset int64) int32 {
	file, rc := canonicalSQLiteIOGuard(pFile, "write")
	if rc != sqlite3.SQLITE_OK {
		return sqlite3.SQLITE_IOERR_WRITE
	}
	return callSQLiteIOWrite(file.original.FxWrite, tls, pFile, buf, amount, offset)
}

func canonicalSQLiteIOTruncate(tls *libc.TLS, pFile uintptr, size int64) int32 {
	file, rc := canonicalSQLiteIOGuard(pFile, "truncate")
	if rc != sqlite3.SQLITE_OK {
		return sqlite3.SQLITE_IOERR_TRUNCATE
	}
	return callSQLiteIOTruncate(file.original.FxTruncate, tls, pFile, size)
}

func canonicalSQLiteIOSync(tls *libc.TLS, pFile uintptr, flags int32) int32 {
	file, rc := canonicalSQLiteIOGuard(pFile, "sync")
	if rc != sqlite3.SQLITE_OK {
		return sqlite3.SQLITE_IOERR_FSYNC
	}
	return callSQLiteIOSync(file.original.FxSync, tls, pFile, flags)
}

func canonicalSQLiteIOFileSize(tls *libc.TLS, pFile, pSize uintptr) int32 {
	file, rc := canonicalSQLiteIOGuard(pFile, "file-size")
	if rc != sqlite3.SQLITE_OK {
		return sqlite3.SQLITE_IOERR_FSTAT
	}
	return callSQLiteIOFileSize(file.original.FxFileSize, tls, pFile, pSize)
}

func canonicalSQLiteIOLock(tls *libc.TLS, pFile uintptr, lock int32) int32 {
	file, rc := canonicalSQLiteIOGuard(pFile, "lock")
	if rc != sqlite3.SQLITE_OK {
		return sqlite3.SQLITE_IOERR_LOCK
	}
	return callSQLiteIOLock(file.original.FxLock, tls, pFile, lock)
}

func canonicalSQLiteIOUnlock(tls *libc.TLS, pFile uintptr, lock int32) int32 {
	file := canonicalSQLiteVFSFileFor(pFile)
	if file == nil {
		return sqlite3.SQLITE_IOERR_UNLOCK
	}
	return callSQLiteIOUnlock(file.original.FxUnlock, tls, pFile, lock)
}

func canonicalSQLiteIOCheckReservedLock(tls *libc.TLS, pFile, pResult uintptr) int32 {
	file, rc := canonicalSQLiteIOGuard(pFile, "check-lock")
	if rc != sqlite3.SQLITE_OK {
		return sqlite3.SQLITE_IOERR_CHECKRESERVEDLOCK
	}
	return callSQLiteIOCheckReservedLock(file.original.FxCheckReservedLock, tls, pFile, pResult)
}

func canonicalSQLiteIOFileControl(tls *libc.TLS, pFile uintptr, op int32, arg uintptr) int32 {
	file, rc := canonicalSQLiteIOGuard(pFile, "file-control")
	if rc != sqlite3.SQLITE_OK {
		return sqlite3.SQLITE_IOERR
	}
	return callSQLiteIOFileControl(file.original.FxFileControl, tls, pFile, op, arg)
}

func canonicalSQLiteIOSectorSize(tls *libc.TLS, pFile uintptr) int32 {
	file := canonicalSQLiteVFSFileFor(pFile)
	if file == nil {
		return 0
	}
	return callSQLiteIOSectorSize(file.original.FxSectorSize, tls, pFile)
}

func canonicalSQLiteIODeviceCharacteristics(tls *libc.TLS, pFile uintptr) int32 {
	file := canonicalSQLiteVFSFileFor(pFile)
	if file == nil {
		return 0
	}
	return callSQLiteIODeviceCharacteristics(file.original.FxDeviceCharacteristics, tls, pFile)
}

func canonicalSQLiteIOShmMap(tls *libc.TLS, pFile uintptr, page, pageSize, extend int32, pp uintptr) int32 {
	file, rc := canonicalSQLiteIOGuard(pFile, "shared-memory map")
	if rc != sqlite3.SQLITE_OK {
		return sqlite3.SQLITE_IOERR_SHMMAP
	}
	reservation, err := file.state.reserveArtifact(file.state.path+"-shm", true)
	if err != nil {
		_ = file.state.fail(fmt.Errorf("canonical SQLite shared-memory reservation: %w", err))
		return sqlite3.SQLITE_IOERR_SHMMAP
	}
	rc = callSQLiteIOShmMap(file.original.FxShmMap, tls, pFile, page, pageSize, extend, pp)
	if rc != sqlite3.SQLITE_OK {
		if cleanupErr := file.state.releaseArtifactReservation(reservation, true); cleanupErr != nil {
			_ = file.state.fail(fmt.Errorf("clean failed canonical SQLite shared-memory reservation: %w", cleanupErr))
		}
		return rc
	}
	if err := file.state.bindSharedMemory(pFile); err != nil {
		_ = callSQLiteIOShmUnmap(file.original.FxShmUnmap, tls, pFile, 0)
		cleanupErr := file.state.releaseArtifactReservation(reservation, true)
		_ = file.state.fail(errors.Join(fmt.Errorf("canonical SQLite shared-memory binding: %w", err), cleanupErr))
		return sqlite3.SQLITE_IOERR_SHMMAP
	}
	if err := file.state.retainArtifactReservation(reservation); err != nil {
		_ = callSQLiteIOShmUnmap(file.original.FxShmUnmap, tls, pFile, 0)
		cleanupErr := file.state.releaseArtifactReservation(reservation, true)
		_ = file.state.fail(errors.Join(fmt.Errorf("retain canonical SQLite shared-memory reservation: %w", err), cleanupErr))
		return sqlite3.SQLITE_IOERR_SHMMAP
	}
	return sqlite3.SQLITE_OK
}

func canonicalSQLiteIOShmLock(tls *libc.TLS, pFile uintptr, offset, n, flags int32) int32 {
	file, rc := canonicalSQLiteIOGuard(pFile, "shared-memory lock")
	if rc != sqlite3.SQLITE_OK {
		return sqlite3.SQLITE_IOERR_SHMLOCK
	}
	if err := file.state.bindSharedMemory(pFile); err != nil {
		_ = file.state.fail(fmt.Errorf("canonical SQLite shared-memory lock binding: %w", err))
		return sqlite3.SQLITE_IOERR_SHMLOCK
	}
	return callSQLiteIOShmLock(file.original.FxShmLock, tls, pFile, offset, n, flags)
}

func canonicalSQLiteIOShmBarrier(tls *libc.TLS, pFile uintptr) {
	file, rc := canonicalSQLiteIOGuard(pFile, "shared-memory barrier")
	if rc == sqlite3.SQLITE_OK {
		if err := file.state.bindSharedMemory(pFile); err != nil {
			_ = file.state.fail(fmt.Errorf("canonical SQLite shared-memory barrier binding: %w", err))
			return
		}
		callSQLiteIOShmBarrier(file.original.FxShmBarrier, tls, pFile)
	}
}

func canonicalSQLiteIOShmUnmap(tls *libc.TLS, pFile uintptr, deleteFlag int32) int32 {
	file, rc := canonicalSQLiteIOGuard(pFile, "shared-memory unmap")
	if rc != sqlite3.SQLITE_OK {
		return sqlite3.SQLITE_IOERR_SHMMAP
	}
	rc = callSQLiteIOShmUnmap(file.original.FxShmUnmap, tls, pFile, deleteFlag)
	if rc != sqlite3.SQLITE_OK {
		return rc
	}
	path := file.state.path + "-shm"
	if err := file.state.releaseRetainedArtifactReservation(path); err != nil {
		_ = file.state.fail(fmt.Errorf("release canonical SQLite shared-memory reservation after unmap: %w", err))
		return sqlite3.SQLITE_IOERR_SHMMAP
	}
	if deleteFlag != 0 {
		if err := file.state.recordArtifactDeleted(path); err != nil {
			_ = file.state.fail(fmt.Errorf("canonical SQLite shared-memory delete postcondition: %w", err))
			return sqlite3.SQLITE_IOERR_SHMMAP
		}
	}
	return sqlite3.SQLITE_OK
}

func canonicalSQLiteIOFetch(tls *libc.TLS, pFile uintptr, offset int64, amount int32, pp uintptr) int32 {
	file, rc := canonicalSQLiteIOGuard(pFile, "fetch")
	if rc != sqlite3.SQLITE_OK {
		return sqlite3.SQLITE_IOERR
	}
	return callSQLiteIOFetch(file.original.FxFetch, tls, pFile, offset, amount, pp)
}

func canonicalSQLiteIOUnfetch(tls *libc.TLS, pFile uintptr, offset int64, p uintptr) int32 {
	file, rc := canonicalSQLiteIOGuard(pFile, "unfetch")
	if rc != sqlite3.SQLITE_OK {
		return sqlite3.SQLITE_IOERR
	}
	return callSQLiteIOUnfetch(file.original.FxUnfetch, tls, pFile, offset, p)
}

func ccgoFunctionPointer(fn any) uintptr {
	type iface [2]uintptr
	return (*iface)(unsafe.Pointer(&fn))[1]
}

//nolint:govet // The translated SQLite API represents its VFS struct as a uintptr.
func canonicalSQLiteVFSStruct(p uintptr) *sqlite3.Tsqlite3_vfs {
	return (*sqlite3.Tsqlite3_vfs)(unsafe.Pointer(p))
}

//nolint:govet // SQLite supplies this exact sqlite3_file pointer to the VFS.
func canonicalSQLiteFileStruct(p uintptr) *sqlite3.Tsqlite3_file {
	return (*sqlite3.Tsqlite3_file)(unsafe.Pointer(p))
}

//nolint:govet // sqlite3_file stores its translated method table as a uintptr.
func canonicalSQLiteIOMethodsStruct(p uintptr) *sqlite3.Tsqlite3_io_methods {
	return (*sqlite3.Tsqlite3_io_methods)(unsafe.Pointer(p))
}

func callSQLiteVFSOpen(fn uintptr, tls *libc.TLS, vfs, name, file uintptr, flags int32, out uintptr) int32 {
	return (*(*func(*libc.TLS, uintptr, uintptr, uintptr, int32, uintptr) int32)(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, vfs, name, file, flags, out)
}
func callSQLiteVFSDelete(fn uintptr, tls *libc.TLS, vfs, name uintptr, syncDir int32) int32 {
	return (*(*func(*libc.TLS, uintptr, uintptr, int32) int32)(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, vfs, name, syncDir)
}
func callSQLiteVFSAccess(fn uintptr, tls *libc.TLS, vfs, name uintptr, flags int32, out uintptr) int32 {
	return (*(*func(*libc.TLS, uintptr, uintptr, int32, uintptr) int32)(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, vfs, name, flags, out)
}
func callSQLiteIOClose(fn uintptr, tls *libc.TLS, file uintptr) int32 {
	return (*(*func(*libc.TLS, uintptr) int32)(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, file)
}
func callSQLiteIORead(fn uintptr, tls *libc.TLS, file, buf uintptr, amount int32, offset int64) int32 {
	return (*(*func(*libc.TLS, uintptr, uintptr, int32, int64) int32)(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, file, buf, amount, offset)
}
func callSQLiteIOWrite(fn uintptr, tls *libc.TLS, file, buf uintptr, amount int32, offset int64) int32 {
	return (*(*func(*libc.TLS, uintptr, uintptr, int32, int64) int32)(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, file, buf, amount, offset)
}
func callSQLiteIOTruncate(fn uintptr, tls *libc.TLS, file uintptr, size int64) int32 {
	return (*(*func(*libc.TLS, uintptr, int64) int32)(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, file, size)
}
func callSQLiteIOSync(fn uintptr, tls *libc.TLS, file uintptr, flags int32) int32 {
	return (*(*func(*libc.TLS, uintptr, int32) int32)(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, file, flags)
}
func callSQLiteIOFileSize(fn uintptr, tls *libc.TLS, file, size uintptr) int32 {
	return (*(*func(*libc.TLS, uintptr, uintptr) int32)(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, file, size)
}
func callSQLiteIOLock(fn uintptr, tls *libc.TLS, file uintptr, lock int32) int32 {
	return (*(*func(*libc.TLS, uintptr, int32) int32)(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, file, lock)
}
func callSQLiteIOUnlock(fn uintptr, tls *libc.TLS, file uintptr, lock int32) int32 {
	return (*(*func(*libc.TLS, uintptr, int32) int32)(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, file, lock)
}
func callSQLiteIOCheckReservedLock(fn uintptr, tls *libc.TLS, file, out uintptr) int32 {
	return (*(*func(*libc.TLS, uintptr, uintptr) int32)(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, file, out)
}
func callSQLiteIOFileControl(fn uintptr, tls *libc.TLS, file uintptr, op int32, arg uintptr) int32 {
	return (*(*func(*libc.TLS, uintptr, int32, uintptr) int32)(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, file, op, arg)
}
func callSQLiteIOSectorSize(fn uintptr, tls *libc.TLS, file uintptr) int32 {
	return (*(*func(*libc.TLS, uintptr) int32)(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, file)
}
func callSQLiteIODeviceCharacteristics(fn uintptr, tls *libc.TLS, file uintptr) int32 {
	return (*(*func(*libc.TLS, uintptr) int32)(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, file)
}
func callSQLiteIOShmMap(fn uintptr, tls *libc.TLS, file uintptr, page, pageSize, extend int32, pp uintptr) int32 {
	return (*(*func(*libc.TLS, uintptr, int32, int32, int32, uintptr) int32)(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, file, page, pageSize, extend, pp)
}
func callSQLiteIOShmLock(fn uintptr, tls *libc.TLS, file uintptr, offset, n, flags int32) int32 {
	return (*(*func(*libc.TLS, uintptr, int32, int32, int32) int32)(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, file, offset, n, flags)
}
func callSQLiteIOShmBarrier(fn uintptr, tls *libc.TLS, file uintptr) {
	(*(*func(*libc.TLS, uintptr))(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, file)
}
func callSQLiteIOShmUnmap(fn uintptr, tls *libc.TLS, file uintptr, deleteFlag int32) int32 {
	return (*(*func(*libc.TLS, uintptr, int32) int32)(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, file, deleteFlag)
}
func callSQLiteIOFetch(fn uintptr, tls *libc.TLS, file uintptr, offset int64, amount int32, pp uintptr) int32 {
	if fn == 0 {
		return sqlite3.SQLITE_OK
	}
	return (*(*func(*libc.TLS, uintptr, int64, int32, uintptr) int32)(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, file, offset, amount, pp)
}
func callSQLiteIOUnfetch(fn uintptr, tls *libc.TLS, file uintptr, offset int64, p uintptr) int32 {
	if fn == 0 {
		return sqlite3.SQLITE_OK
	}
	return (*(*func(*libc.TLS, uintptr, int64, uintptr) int32)(unsafe.Pointer(&struct{ uintptr }{fn})))(tls, file, offset, p)
}
