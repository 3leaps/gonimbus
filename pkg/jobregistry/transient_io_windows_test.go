//go:build windows

package jobregistry

import (
	"fmt"
	"testing"

	"golang.org/x/sys/windows"
)

// The registry's handle-relative opens, replaces and deletes return NTSTATUS
// errors. A sharing violation there must be retried like its Win32 form, or a
// brief open handle on job.json fails the write outright.
func TestTransientRegistryIOErrorRecognizesNTStatus(t *testing.T) {
	for _, status := range []windows.NTStatus{
		windows.STATUS_SHARING_VIOLATION,
		windows.STATUS_ACCESS_DENIED,
		windows.STATUS_LOCK_NOT_GRANTED,
		windows.STATUS_FILE_LOCK_CONFLICT,
	} {
		err := fmt.Errorf("replace bound job record: %w", status)
		if !isTransientRegistryIOError(err) {
			t.Errorf("%#x (%v) not classified transient", uint32(status), status)
		}
	}
	if isTransientRegistryIOError(fmt.Errorf("open: %w", windows.STATUS_OBJECT_NAME_NOT_FOUND)) {
		t.Error("STATUS_OBJECT_NAME_NOT_FOUND classified transient")
	}
}
