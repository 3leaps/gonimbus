package stalledrecovery_test

import (
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// TestSpawnSleepChildHelper is the re-exec entry for SpawnSleepChild.
// No Unix sleep/bash dependency — parks until parent Kill (or soft timeout).
func TestSpawnSleepChildHelper(t *testing.T) {
	if os.Getenv("GONIMBUS_STALLED_SLEEP_HELPER") != "1" {
		t.Skip("sleep child helper entry only")
	}
	if ready := os.Getenv("GONIMBUS_STALLED_SLEEP_READY"); ready != "" {
		if err := os.WriteFile(ready, []byte(runtime.GOOS+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	secs := 120
	if raw := os.Getenv("GONIMBUS_STALLED_SLEEP_SECONDS"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			secs = n
		}
	}
	// Soft upper bound; parent normally SIGKILLs earlier.
	time.Sleep(time.Duration(secs) * time.Second)
}
