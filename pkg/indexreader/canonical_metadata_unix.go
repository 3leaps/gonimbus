//go:build !windows

package indexreader

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func publishCanonicalMetadataPlatform(dir, name string, data []byte, check func() error) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create canonical metadata directory: %w", err)
	}
	dirFD, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("bind canonical metadata directory: %w", err)
	}
	defer func() { _ = unix.Close(dirFD) }()
	if err := unix.Fchmod(dirFD, 0o700); err != nil {
		return fmt.Errorf("chmod canonical metadata directory: %w", err)
	}
	if err := verifyCanonicalMetadataDir(dirFD, dir); err != nil {
		return err
	}
	if err := rejectUnsafeCanonicalMetadataDestination(dirFD, name); err != nil {
		return err
	}
	tmpName, err := canonicalMetadataTempName(name)
	if err != nil {
		return err
	}
	tmpFD, err := unix.Openat(dirFD, tmpName, unix.O_CREAT|unix.O_EXCL|unix.O_WRONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return fmt.Errorf("create metadata transaction temp: %w", err)
	}
	defer func() { _ = unix.Unlinkat(dirFD, tmpName, 0) }()
	tmp := os.NewFile(uintptr(tmpFD), tmpName) // #nosec G115 -- unix.Openat returns a nonnegative descriptor
	if tmp == nil {
		_ = unix.Close(tmpFD)
		return os.ErrInvalid
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write metadata transaction temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync metadata transaction temp: %w", err)
	}
	tmpInfo, err := tmp.Stat()
	if err != nil {
		_ = tmp.Close()
		return fmt.Errorf("inspect metadata transaction temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close metadata transaction temp: %w", err)
	}
	if canonicalMetadataBeforeReplace != nil {
		if err := canonicalMetadataBeforeReplace(filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	if err := check(); err != nil {
		return err
	}
	if err := verifyCanonicalMetadataDir(dirFD, dir); err != nil {
		return err
	}
	if err := rejectUnsafeCanonicalMetadataDestination(dirFD, name); err != nil {
		return err
	}
	if err := unix.Renameat(dirFD, tmpName, dirFD, name); err != nil {
		return fmt.Errorf("atomically replace canonical metadata: %w", err)
	}
	if err := unix.Fsync(dirFD); err != nil {
		return fmt.Errorf("sync canonical metadata directory: %w", err)
	}
	if err := verifyPublishedCanonicalMetadata(dirFD, name, data, tmpInfo); err != nil {
		return err
	}
	if err := verifyCanonicalMetadataDir(dirFD, dir); err != nil {
		return err
	}
	return check()
}

func verifyCanonicalMetadataDir(dirFD int, dir string) error {
	var bound unix.Stat_t
	if err := unix.Fstat(dirFD, &bound); err != nil {
		return fmt.Errorf("inspect bound canonical metadata directory: %w", err)
	}
	var named unix.Stat_t
	if err := unix.Fstatat(unix.AT_FDCWD, dir, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fmt.Errorf("inspect named canonical metadata directory: %w", err)
	}
	if bound.Mode&unix.S_IFMT != unix.S_IFDIR || named.Mode&unix.S_IFMT != unix.S_IFDIR || bound.Dev != named.Dev || bound.Ino != named.Ino {
		return fmt.Errorf("canonical metadata directory binding changed")
	}
	return nil
}

func rejectUnsafeCanonicalMetadataDestination(dirFD int, name string) error {
	var info unix.Stat_t
	err := unix.Fstatat(dirFD, name, &info, unix.AT_SYMLINK_NOFOLLOW)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect canonical metadata destination: %w", err)
	}
	if info.Mode&unix.S_IFMT != unix.S_IFREG {
		return fmt.Errorf("refusing non-regular canonical metadata destination")
	}
	return nil
}

func verifyPublishedCanonicalMetadata(dirFD int, name string, want []byte, tmpInfo os.FileInfo) error {
	fd, err := unix.Openat(dirFD, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("reopen published canonical metadata: %w", err)
	}
	f := os.NewFile(uintptr(fd), name) // #nosec G115 -- unix.Openat returns a nonnegative descriptor
	if f == nil {
		_ = unix.Close(fd)
		return os.ErrInvalid
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || !os.SameFile(tmpInfo, info) {
		return errors.Join(fmt.Errorf("published canonical metadata does not name the committed regular file"), err)
	}
	got, err := io.ReadAll(io.LimitReader(f, int64(len(want))+1))
	if err != nil {
		return fmt.Errorf("read published canonical metadata: %w", err)
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("published canonical metadata content does not match committed payload")
	}
	return nil
}

func canonicalMetadataTempName(name string) (string, error) {
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("generate metadata transaction name: %w", err)
	}
	return "." + name + ".txn-" + hex.EncodeToString(nonce[:]), nil
}
