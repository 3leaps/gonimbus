//go:build linux

package reflow

import (
	"os"
	"strconv"
	"strings"
)

func defaultPlatformMemoryLimitBytes() (int64, string, error) {
	for _, candidate := range []struct {
		path   string
		source string
	}{
		{path: "/sys/fs/cgroup/memory.max", source: "cgroup_v2"},
		{path: "/sys/fs/cgroup/memory/memory.limit_in_bytes", source: "cgroup_v1"},
	} {
		raw, err := os.ReadFile(candidate.path)
		if err != nil {
			continue
		}
		text := strings.TrimSpace(string(raw))
		if text == "" || text == "max" {
			continue
		}
		limit, err := strconv.ParseInt(text, 10, 64)
		if err != nil || limit <= 0 || limit >= 1<<60 {
			continue
		}
		return limit, candidate.source, nil
	}
	return 0, "", nil
}

func defaultPhysicalMemoryBytes() (int64, error) {
	raw, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	return parseMemInfoTotalBytes(raw), nil
}

// parseMemInfoTotalBytes extracts MemTotal from /proc/meminfo content
// ("MemTotal:       65536000 kB"). Returns 0 when absent or malformed.
func parseMemInfoTotalBytes(raw []byte) int64 {
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(line, "MemTotal:"))
		if len(fields) < 1 {
			return 0
		}
		kb, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil || kb <= 0 || kb >= (1<<60)/1024 {
			return 0
		}
		if len(fields) > 1 && fields[1] != "kB" {
			return 0
		}
		return kb * 1024
	}
	return 0
}
