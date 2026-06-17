//go:build linux

package cmd

import (
	"os"
	"strconv"
	"strings"
)

func defaultReflowPlatformMemoryLimitBytes() (int64, string, error) {
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
