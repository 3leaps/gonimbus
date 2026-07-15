package reflowthroughput

import (
	"strings"
	"testing"
)

func TestParseReflowStdoutMinimal(t *testing.T) {
	t.Parallel()
	stdout := strings.Join([]string{
		`{"type":"gonimbus.reflow.run.v1","data":{"dest_uri":"file:///tmp/out","checkpoint_path":"/tmp/c.db","dry_run":false,"resume":false,"parallel":8,"adaptive_enabled":false,"concurrency_floor":1,"concurrency_initial":8,"concurrency_ceiling_requested":8,"concurrency_ceiling_effective":8,"concurrency_ceiling_reason":"requested","concurrency_final":8,"concurrency_throttle_backoffs":0,"concurrency_additive_increases":0,"concurrency_connection_error_freezes":0,"concurrency_max_active":4}}`,
		`{"type":"gonimbus.reflow.v1","data":{"source_uri":"file://local/a","source_key":"a","dest_uri":"file:///tmp/out/a","dest_key":"a","status":"complete"}}`,
		`{"type":"gonimbus.reflow.summary.v1","data":{"dest_uri":"file:///tmp/out","dry_run":false,"on_collision":"skip-if-duplicate","adaptive_enabled":false,"concurrency_floor":1,"concurrency_initial":8,"concurrency_ceiling_requested":8,"concurrency_ceiling_effective":8,"concurrency_ceiling_reason":"requested","concurrency_final":8,"concurrency_throttle_backoffs":0,"concurrency_additive_increases":0,"concurrency_connection_error_freezes":0,"concurrency_max_active":4,"dest_ifabsent_honored":null,"fallback_active":false,"ifabsent_fallback_objects":0,"statuses":{"complete":1},"errors":0,"invalid_inputs":0}}`,
	}, "\n") + "\n"
	p, err := ParseReflowStdout([]byte(stdout))
	if err != nil {
		t.Fatal(err)
	}
	if p.Requested != 8 || p.Effective != 8 || p.MaxActive != 4 {
		t.Fatalf("parsed %+v", p)
	}
	if p.ObjectComplete != 1 {
		t.Fatalf("complete=%d", p.ObjectComplete)
	}
}

func TestParseRejectsMalformedJSON(t *testing.T) {
	t.Parallel()
	stdout := `{"type":"gonimbus.reflow.run.v1","data":{` + "\n"
	if _, err := ParseReflowStdout([]byte(stdout)); err == nil {
		t.Fatal("expected malformed error")
	}
}

func TestParseRejectsRunSummaryMismatch(t *testing.T) {
	t.Parallel()
	stdout := strings.Join([]string{
		`{"type":"gonimbus.reflow.run.v1","data":{"dest_uri":"x","checkpoint_path":"y","dry_run":false,"resume":false,"parallel":8,"adaptive_enabled":false,"concurrency_floor":1,"concurrency_initial":8,"concurrency_ceiling_requested":8,"concurrency_ceiling_effective":8,"concurrency_ceiling_reason":"requested","concurrency_final":8,"concurrency_throttle_backoffs":0,"concurrency_additive_increases":0,"concurrency_connection_error_freezes":0,"concurrency_max_active":4}}`,
		`{"type":"gonimbus.reflow.summary.v1","data":{"dest_uri":"x","dry_run":false,"on_collision":"skip-if-duplicate","adaptive_enabled":false,"concurrency_floor":1,"concurrency_initial":8,"concurrency_ceiling_requested":8,"concurrency_ceiling_effective":4,"concurrency_ceiling_reason":"requested","concurrency_final":4,"concurrency_throttle_backoffs":0,"concurrency_additive_increases":0,"concurrency_connection_error_freezes":0,"concurrency_max_active":4,"dest_ifabsent_honored":null,"fallback_active":false,"ifabsent_fallback_objects":0,"statuses":{"complete":1},"errors":0,"invalid_inputs":0}}`,
	}, "\n") + "\n"
	if _, err := ParseReflowStdout([]byte(stdout)); err == nil {
		t.Fatal("expected run/summary mismatch")
	}
}

func TestParseRejectsClampDetailMismatch(t *testing.T) {
	t.Parallel()
	// effective 16 in run/summary but warning details say effective 8.
	stdout := strings.Join([]string{
		`{"type":"gonimbus.reflow.run.v1","data":{"dest_uri":"x","checkpoint_path":"y","dry_run":false,"resume":false,"parallel":256,"adaptive_enabled":false,"concurrency_floor":1,"concurrency_initial":16,"concurrency_ceiling_requested":256,"concurrency_ceiling_effective":16,"concurrency_ceiling_reason":"resource_capped:memory:conservative_default","concurrency_final":16,"concurrency_throttle_backoffs":0,"concurrency_additive_increases":0,"concurrency_connection_error_freezes":0,"concurrency_max_active":8}}`,
		`{"type":"gonimbus.warning.v1","data":{"code":"REFLOW_CONCURRENCY_CEILING_CLAMPED","message":"clamped","details":{"concurrency_ceiling_requested":256,"concurrency_ceiling_effective":8,"concurrency_ceiling_reason":"resource_capped:memory:conservative_default","adaptive_enabled":false}}}`,
		`{"type":"gonimbus.reflow.summary.v1","data":{"dest_uri":"x","dry_run":false,"on_collision":"skip-if-duplicate","adaptive_enabled":false,"concurrency_floor":1,"concurrency_initial":16,"concurrency_ceiling_requested":256,"concurrency_ceiling_effective":16,"concurrency_ceiling_reason":"resource_capped:memory:conservative_default","concurrency_final":16,"concurrency_throttle_backoffs":0,"concurrency_additive_increases":0,"concurrency_connection_error_freezes":0,"concurrency_max_active":8,"dest_ifabsent_honored":null,"fallback_active":false,"ifabsent_fallback_objects":0,"statuses":{"complete":1},"errors":0,"invalid_inputs":0}}`,
	}, "\n") + "\n"
	if _, err := ParseReflowStdout([]byte(stdout)); err == nil {
		t.Fatal("expected clamp detail mismatch")
	}
}

func TestParseRejectsClampMissingAdaptive(t *testing.T) {
	t.Parallel()
	stdout := strings.Join([]string{
		`{"type":"gonimbus.reflow.run.v1","data":{"dest_uri":"x","checkpoint_path":"y","dry_run":false,"resume":false,"parallel":256,"adaptive_enabled":false,"concurrency_floor":1,"concurrency_initial":16,"concurrency_ceiling_requested":256,"concurrency_ceiling_effective":16,"concurrency_ceiling_reason":"resource_capped:memory:conservative_default","concurrency_final":16,"concurrency_throttle_backoffs":0,"concurrency_additive_increases":0,"concurrency_connection_error_freezes":0,"concurrency_max_active":8}}`,
		`{"type":"gonimbus.warning.v1","data":{"code":"REFLOW_CONCURRENCY_CEILING_CLAMPED","message":"clamped","details":{"concurrency_ceiling_requested":256,"concurrency_ceiling_effective":16,"concurrency_ceiling_reason":"resource_capped:memory:conservative_default"}}}`,
		`{"type":"gonimbus.reflow.summary.v1","data":{"dest_uri":"x","dry_run":false,"on_collision":"skip-if-duplicate","adaptive_enabled":false,"concurrency_floor":1,"concurrency_initial":16,"concurrency_ceiling_requested":256,"concurrency_ceiling_effective":16,"concurrency_ceiling_reason":"resource_capped:memory:conservative_default","concurrency_final":16,"concurrency_throttle_backoffs":0,"concurrency_additive_increases":0,"concurrency_connection_error_freezes":0,"concurrency_max_active":8,"dest_ifabsent_honored":null,"fallback_active":false,"ifabsent_fallback_objects":0,"statuses":{"complete":1},"errors":0,"invalid_inputs":0}}`,
	}, "\n") + "\n"
	if _, err := ParseReflowStdout([]byte(stdout)); err == nil {
		t.Fatal("expected missing adaptive_enabled detail to hard-fail")
	}
}

func TestParseRejectsUnknownType(t *testing.T) {
	t.Parallel()
	stdout := strings.Join([]string{
		`{"type":"gonimbus.reflow.run.v1","data":{"dest_uri":"x","checkpoint_path":"y","dry_run":false,"resume":false,"parallel":1,"adaptive_enabled":false,"concurrency_floor":1,"concurrency_initial":1,"concurrency_ceiling_requested":1,"concurrency_ceiling_effective":1,"concurrency_ceiling_reason":"requested","concurrency_final":1,"concurrency_throttle_backoffs":0,"concurrency_additive_increases":0,"concurrency_connection_error_freezes":0,"concurrency_max_active":1}}`,
		`{"type":"gonimbus.unexpected.v1","data":{}}`,
		`{"type":"gonimbus.reflow.summary.v1","data":{"dest_uri":"x","dry_run":false,"on_collision":"skip-if-duplicate","adaptive_enabled":false,"concurrency_floor":1,"concurrency_initial":1,"concurrency_ceiling_requested":1,"concurrency_ceiling_effective":1,"concurrency_ceiling_reason":"requested","concurrency_final":1,"concurrency_throttle_backoffs":0,"concurrency_additive_increases":0,"concurrency_connection_error_freezes":0,"concurrency_max_active":1,"dest_ifabsent_honored":null,"fallback_active":false,"ifabsent_fallback_objects":0,"statuses":{"complete":0},"errors":0,"invalid_inputs":0}}`,
	}, "\n") + "\n"
	if _, err := ParseReflowStdout([]byte(stdout)); err == nil {
		t.Fatal("expected unknown type error")
	}
}
