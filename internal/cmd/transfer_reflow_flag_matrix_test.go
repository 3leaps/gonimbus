package cmd

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

// Flag-coverage parity matrix for `transfer reflow` (standing behavioral parity
// gate, item 3). Every flag declares its disposition on each execution path:
//
//   - honored:          the path applies the flag's semantics
//   - routes-cli-pool:  setting the flag (or a non-migrated value of it) makes
//     the dispatch select the CLI pool, where it is honored;
//     the engine never sees it, so nothing is silently dropped
//   - rejected-loud:    validation refuses the combination before any run
//
// "silently-ignored" is deliberately not a legal category: a flag whose value
// a selected path would ignore must either route dispatch or reject loudly.
// The test below fails when a registered flag lacks a row (new flags added by
// later slices must declare their disposition) or when a row goes stale.
type reflowFlagDisposition struct {
	engine  string
	cliPool string
	note    string
}

const (
	flagHonored       = "honored"
	flagRoutesCLIPool = "routes-cli-pool"
	flagRejectedLoud  = "rejected-loud"
)

var reflowFlagMatrix = map[string]reflowFlagDisposition{
	"stdin":                       {engine: flagHonored, cliPool: flagHonored, note: "engine path requires stdin record streams; positional sources route to the pool"},
	"dest":                        {engine: flagHonored, cliPool: flagHonored},
	"rewrite-from":                {engine: flagHonored, cliPool: flagHonored},
	"rewrite-to":                  {engine: flagHonored, cliPool: flagHonored},
	"parallel":                    {engine: flagHonored, cliPool: flagHonored, note: "engine worker pool honors the resolved ceiling; dual-path max-in-flight harness enforces"},
	"no-adaptive":                 {engine: flagHonored, cliPool: flagHonored, note: "concurrency resolved before dispatch, shared by both paths"},
	"dry-run":                     {engine: flagHonored, cliPool: flagHonored, note: "migrated stdin shapes plan on the engine; non-migrated shapes dry-run on the pool"},
	"resume":                      {engine: flagHonored, cliPool: flagHonored},
	"resume-run":                  {engine: flagRoutesCLIPool, cliPool: flagHonored, note: "run-id resume is a positional-input shape"},
	"checkpoint":                  {engine: flagHonored, cliPool: flagHonored},
	"overwrite":                   {engine: flagRoutesCLIPool, cliPool: flagHonored, note: "overwrite collision semantics not migrated"},
	"on-collision":                {engine: flagHonored, cliPool: flagHonored, note: "engine executes skip-if-duplicate|fail; other modes route to the pool"},
	"collision-quarantine-prefix": {engine: flagRoutesCLIPool, cliPool: flagHonored, note: "quarantine mode not migrated"},

	"provenance":                {engine: flagRoutesCLIPool, cliPool: flagHonored, note: "provenance sidecars not migrated"},
	"provenance-sidecar-root":   {engine: flagRoutesCLIPool, cliPool: flagHonored},
	"provenance-suffix":         {engine: flagRoutesCLIPool, cliPool: flagHonored},
	"provenance-on-write-error": {engine: flagRoutesCLIPool, cliPool: flagHonored},
	"allow-unsafe-suffix":       {engine: flagRoutesCLIPool, cliPool: flagHonored},

	"metadata-policy":                  {engine: flagHonored, cliPool: flagHonored},
	"metadata-set":                     {engine: flagHonored, cliPool: flagHonored},
	"metadata-set-from-source-key":     {engine: flagHonored, cliPool: flagHonored},
	"metadata-set-from-source-derived": {engine: flagHonored, cliPool: flagHonored},
	"metadata-on-missing-source":       {engine: flagHonored, cliPool: flagHonored},
	"preserve-content-type":            {engine: flagHonored, cliPool: flagHonored},
	"destination-storage-class":        {engine: flagHonored, cliPool: flagHonored},
	"metadata-sidecar-suffix":          {engine: flagRoutesCLIPool, cliPool: flagHonored, note: "file destinations route to the pool"},

	"symlinks":          {engine: flagRoutesCLIPool, cliPool: flagHonored, note: "file-source policy; positional sources route to the pool"},
	"hidden":            {engine: flagRoutesCLIPool, cliPool: flagHonored, note: "file-source policy; positional sources route to the pool"},
	"exclude":           {engine: flagRoutesCLIPool, cliPool: flagHonored, note: "file-source policy; positional sources route to the pool"},
	"preserve-mode":     {engine: flagRoutesCLIPool, cliPool: flagHonored, note: "preserve-mode not migrated"},
	"on-source-failure": {engine: flagHonored, cliPool: flagHonored, note: "engine executes skip (default); fail routes to the pool"},

	"src-region":       {engine: flagHonored, cliPool: flagHonored, note: "source provider construction shared by both paths"},
	"src-profile":      {engine: flagHonored, cliPool: flagHonored},
	"src-endpoint":     {engine: flagHonored, cliPool: flagHonored},
	"src-gcp-project":  {engine: flagHonored, cliPool: flagHonored},
	"dest-region":      {engine: flagHonored, cliPool: flagHonored, note: "destination provider constructed before dispatch"},
	"dest-profile":     {engine: flagHonored, cliPool: flagHonored},
	"dest-endpoint":    {engine: flagHonored, cliPool: flagHonored},
	"dest-gcp-project": {engine: flagHonored, cliPool: flagHonored},
}

func TestTransferReflowFlagCoverageParityMatrix(t *testing.T) {
	valid := map[string]bool{flagHonored: true, flagRoutesCLIPool: true, flagRejectedLoud: true}

	registered := map[string]bool{}
	transferReflowCmd.Flags().VisitAll(func(f *pflag.Flag) {
		registered[f.Name] = true
		row, ok := reflowFlagMatrix[f.Name]
		require.True(t, ok,
			"flag --%s has no parity-matrix row: declare its {engine, cli-pool} disposition (silently-ignored is not an option)", f.Name)
		require.True(t, valid[row.engine], "flag --%s engine disposition %q invalid", f.Name, row.engine)
		require.True(t, valid[row.cliPool], "flag --%s cli-pool disposition %q invalid", f.Name, row.cliPool)
	})

	for name := range reflowFlagMatrix {
		require.True(t, registered[name], "parity-matrix row %q does not match any registered flag (stale row)", name)
	}
}
