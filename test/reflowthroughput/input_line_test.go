package reflowthroughput

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMarshalReflowInputLineS3KeepsFullSourceKey(t *testing.T) {
	t.Parallel()
	uri := "s3://bucket/run-prefix/entity=0/object.xml"
	fullKey := "run-prefix/entity=0/object.xml"
	destRel := "entity=0/object.xml"
	line, err := marshalReflowInputLine(uri, fullKey, destRel, 256, "etag")
	if err != nil {
		t.Fatal(err)
	}
	var env struct {
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal([]byte(line), &env); err != nil {
		t.Fatal(err)
	}
	if env.Data["source_key"] != fullKey {
		t.Fatalf("source_key=%v want full key %q", env.Data["source_key"], fullKey)
	}
	if env.Data["dest_rel_key"] != destRel {
		t.Fatalf("dest_rel_key=%v", env.Data["dest_rel_key"])
	}
	if env.Data["source_uri"] != uri {
		t.Fatalf("source_uri=%v", env.Data["source_uri"])
	}
	// Guard: bare destRel must not replace full key (product prefers source_key).
	if strings.HasPrefix(fullKey, destRel) {
		t.Fatal("test fixture broken")
	}
}
