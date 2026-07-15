package reflowthroughput

import (
	"bytes"
	"strings"
	"testing"
)

func TestTapForwardsAndCounts(t *testing.T) {
	t.Parallel()
	lines := []string{
		`{"type":"gonimbus.reflow.input.v1","data":{"source_key":"a"}}`,
		`{"type":"gonimbus.reflow.input.v1","data":{"source_key":"b"}}`,
	}
	in := strings.Join(lines, "\n") + "\n"
	var out bytes.Buffer
	tap := &Tap{AcceptReflowInputOnly: true}
	if err := tap.Copy(&out, strings.NewReader(in)); err != nil {
		t.Fatal(err)
	}
	if out.String() != in {
		t.Fatalf("forward mismatch")
	}
	st := tap.Stats()
	if st.ValidReflowInputRows != 2 {
		t.Fatalf("rows=%d", st.ValidReflowInputRows)
	}
}

func TestTapRejectsUnexpectedType(t *testing.T) {
	t.Parallel()
	in := `{"type":"gonimbus.other.v1","data":{}}` + "\n"
	var out bytes.Buffer
	tap := &Tap{AcceptReflowInputOnly: true}
	if err := tap.Copy(&out, strings.NewReader(in)); err == nil {
		t.Fatal("expected error")
	}
}

func TestTapRejectsMalformedJSON(t *testing.T) {
	t.Parallel()
	// Substring looks like reflow.input but is not valid JSON.
	in := `{"type":"gonimbus.reflow.input.v1",BROKEN}` + "\n"
	var out bytes.Buffer
	tap := &Tap{AcceptReflowInputOnly: true}
	if err := tap.Copy(&out, strings.NewReader(in)); err == nil {
		t.Fatal("expected malformed JSON error")
	}
}

func TestTapRejectsOversize(t *testing.T) {
	t.Parallel()
	big := `{"type":"gonimbus.reflow.input.v1","data":{"x":"` + strings.Repeat("a", 100) + `"}}` + "\n"
	var out bytes.Buffer
	tap := &Tap{AcceptReflowInputOnly: true, MaxRecordBytes: 40}
	if err := tap.Copy(&out, strings.NewReader(big)); err == nil {
		t.Fatal("expected oversize error")
	}
}

func TestTapRejectsIncompleteFinal(t *testing.T) {
	t.Parallel()
	in := `{"type":"gonimbus.reflow.input.v1","data":{}}` // no newline
	var out bytes.Buffer
	tap := &Tap{AcceptReflowInputOnly: true}
	if err := tap.Copy(&out, strings.NewReader(in)); err == nil {
		t.Fatal("expected incomplete final error")
	}
}
