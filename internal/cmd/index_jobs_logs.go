package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/3leaps/gonimbus/pkg/jobregistry"
)

type jobsLogLine struct {
	JobID  string `json:"job_id"`
	Stream string `json:"stream"`
	Line   string `json:"line"`
}

func runIndexJobsLogs(cmd *cobra.Command, args []string) error {
	jobID := strings.TrimSpace(args[0])
	if jobID == "" {
		return fmt.Errorf("job_id is required")
	}

	stream, _ := cmd.Flags().GetString("stream")
	stream = strings.TrimSpace(strings.ToLower(stream))
	if stream == "" {
		stream = "stdout"
	}

	tailN, _ := cmd.Flags().GetInt("tail")
	if tailN < 0 {
		tailN = 0
	}

	follow, _ := cmd.Flags().GetBool("follow")
	jsonOutput, _ := cmd.Flags().GetBool("json")

	root, err := indexJobsRootDir()
	if err != nil {
		return err
	}
	store := jobregistry.NewStore(root)

	resolvedID, err := resolveJobID(store, jobID)
	if err != nil {
		return err
	}

	rec, err := store.Get(resolvedID)
	if err != nil {
		return err
	}

	stdoutPath := rec.StdoutPath
	stderrPath := rec.StderrPath
	if stdoutPath == "" {
		stdoutPath = filepath.Join(store.JobDir(rec.JobID), "stdout.log")
	}
	if stderrPath == "" {
		stderrPath = filepath.Join(store.JobDir(rec.JobID), "stderr.log")
	}

	switch stream {
	case "stdout":
		if jsonOutput {
			return printLogJSONL(rec.JobID, "stdout", stdoutPath, tailN, follow)
		}
		if follow {
			return followLog(stdoutPath)
		}
		return printLogTail(stdoutPath, tailN)
	case "stderr":
		if jsonOutput {
			return printLogJSONL(rec.JobID, "stderr", stderrPath, tailN, follow)
		}
		if follow {
			return followLog(stderrPath)
		}
		return printLogTail(stderrPath, tailN)
	case "both":
		if jsonOutput {
			if err := printLogJSONL(rec.JobID, "stdout", stdoutPath, tailN, follow); err != nil {
				return err
			}
			return printLogJSONL(rec.JobID, "stderr", stderrPath, tailN, follow)
		}
		if follow {
			if err := followLog(stdoutPath); err != nil {
				return err
			}
			return followLog(stderrPath)
		}
		if err := printLogTail(stdoutPath, tailN); err != nil {
			return err
		}
		return printLogTail(stderrPath, tailN)
	default:
		return fmt.Errorf("invalid --stream %q (expected stdout, stderr, or both)", stream)
	}
}

func printLogTail(path string, tailN int) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if tailN <= 0 {
		_, err := io.Copy(os.Stdout, f)
		return err
	}

	lines, err := tailLines(f, tailN)
	if err != nil {
		return err
	}
	for _, line := range lines {
		_, _ = fmt.Fprintln(os.Stdout, line)
	}
	return nil
}

func tailLines(r io.Reader, n int) ([]string, error) {
	if n <= 0 {
		return nil, nil
	}

	scanner := bufio.NewScanner(r)
	buf := make([]string, 0, n)

	for scanner.Scan() {
		line := scanner.Text()
		if len(buf) < n {
			buf = append(buf, line)
			continue
		}
		copy(buf, buf[1:])
		buf[n-1] = line
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return buf, nil
}

func printLogJSONL(jobID, stream, path string, tailN int, follow bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	enc := json.NewEncoder(os.Stdout)

	if tailN > 0 {
		lines, err := tailLines(f, tailN)
		if err != nil {
			return err
		}
		for _, line := range lines {
			if err := enc.Encode(jobsLogLine{JobID: jobID, Stream: stream, Line: line}); err != nil {
				return err
			}
		}
	} else {
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			if err := enc.Encode(jobsLogLine{JobID: jobID, Stream: stream, Line: scanner.Text()}); err != nil {
				return err
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}
	}

	if !follow {
		return nil
	}

	// Poll for new content (simple follow for v0.1.4).
	for {
		pos, _ := f.Seek(0, io.SeekCurrent)
		st, err := f.Stat()
		if err != nil {
			return err
		}
		if st.Size() > pos {
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				if err := enc.Encode(jobsLogLine{JobID: jobID, Stream: stream, Line: scanner.Text()}); err != nil {
					return err
				}
			}
			if err := scanner.Err(); err != nil {
				return err
			}
			continue
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func followLog(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		_, _ = fmt.Fprintln(os.Stdout, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	// Poll for new content (simple follow for v0.1.4).
	for {
		pos, _ := f.Seek(0, io.SeekCurrent)
		st, err := f.Stat()
		if err != nil {
			return err
		}
		if st.Size() > pos {
			// Resume scanning from the current position.
			scanner = bufio.NewScanner(f)
			for scanner.Scan() {
				_, _ = fmt.Fprintln(os.Stdout, scanner.Text())
			}
			if err := scanner.Err(); err != nil {
				return err
			}
			continue
		}
		time.Sleep(250 * time.Millisecond)
	}
}
