//go:build windows

package jobregistry

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

func TestWindowsNativeRegistryMutation(t *testing.T) {
	t.Run("first write overwrite and logs", func(t *testing.T) {
		store := NewStore(t.TempDir())
		rec := &JobRecord{JobID: testJobID1, Name: "first", CreatedAt: time.Now().UTC()}
		if err := store.Write(rec); err != nil {
			t.Fatalf("first Store.Write: %v", err)
		}
		rec.Name = "overwritten"
		if err := store.Write(rec); err != nil {
			t.Fatalf("atomic Store.Write overwrite: %v", err)
		}
		got, err := store.Get(testJobID1)
		if err != nil {
			t.Fatal(err)
		}
		if got.Name != "overwritten" {
			t.Fatalf("overwritten name = %q", got.Name)
		}
		log, err := store.OpenLog(testJobID1, "stdout.log", true)
		if err != nil {
			t.Fatalf("create log relative to UUID handle: %v", err)
		}
		if _, err := log.WriteString("native-windows-log"); err != nil {
			t.Fatal(err)
		}
		_ = log.Close()
		log, err = store.OpenLogRead(testJobID1, "stdout.log")
		if err != nil {
			t.Fatalf("read log relative to UUID handle: %v", err)
		}
		content, err := io.ReadAll(log)
		_ = log.Close()
		if err != nil || string(content) != "native-windows-log" {
			t.Fatalf("log read = %q, err=%v", content, err)
		}
	})

	boundaries := []struct {
		name    string
		install func(func())
	}{
		{name: "swap before temp create", install: func(hook func()) { afterJobDirBoundBeforeTempCreate = hook }},
		{name: "swap before atomic replace", install: func(hook func()) { afterRecordTempCreateBeforeReplace = hook }},
	}
	for _, boundary := range boundaries {
		t.Run(boundary.name, func(t *testing.T) {
			store := NewStore(t.TempDir())
			rec := &JobRecord{JobID: testJobID1, Name: "before", CreatedAt: time.Now().UTC()}
			if err := store.Write(rec); err != nil {
				t.Fatal(err)
			}
			jobDir := store.JobDir(testJobID1)
			parked := jobDir + ".parked"
			outside := t.TempDir()
			outsideRecord := filepath.Join(outside, "job.json")
			if err := os.WriteFile(outsideRecord, []byte("outside-marker"), 0o600); err != nil {
				t.Fatal(err)
			}
			var swapErr error
			var junctionOutput []byte
			hook := func() {
				if swapErr = renameJobDirectoryForWindowsTest(store.RootDir(), testJobID1, filepath.Base(parked)); swapErr != nil {
					return
				}
				junctionOutput, swapErr = exec.Command("cmd.exe", "/c", "mklink", "/J", jobDir, outside).CombinedOutput()
			}
			oldTempHook := afterJobDirBoundBeforeTempCreate
			oldReplaceHook := afterRecordTempCreateBeforeReplace
			boundary.install(hook)
			defer func() {
				afterJobDirBoundBeforeTempCreate = oldTempHook
				afterRecordTempCreateBeforeReplace = oldReplaceHook
				_ = os.Remove(jobDir)
				_ = os.Rename(parked, jobDir)
			}()
			rec.Name = "after"
			err := store.Write(rec)
			if swapErr != nil {
				t.Fatalf("create forced junction swap: %v (%s)", swapErr, junctionOutput)
			}
			if err == nil {
				t.Fatal("expected changed UUID directory binding rejection")
			}
			outsideBytes, readErr := os.ReadFile(outsideRecord)
			if readErr != nil || string(outsideBytes) != "outside-marker" {
				t.Fatalf("outside record changed: %q, err=%v", outsideBytes, readErr)
			}
			entries, readErr := os.ReadDir(outside)
			if readErr != nil || len(entries) != 1 {
				t.Fatalf("temp record escaped: entries=%v err=%v", entries, readErr)
			}
		})
	}
}

func renameJobDirectoryForWindowsTest(root, jobID, target string) error {
	rootHandle, err := openDirectoryHandleNoFollow(root, false)
	if err != nil {
		return err
	}
	defer func() { _ = windows.CloseHandle(rootHandle) }()
	jobHandle, err := openRelativeHandleNoFollow(rootHandle, jobID, os.O_RDONLY, true, true)
	if err != nil {
		return err
	}
	defer func() { _ = windows.CloseHandle(jobHandle) }()
	return renameRelativeFileWindows(jobHandle, rootHandle, target)
}
