package reflowthroughput

import (
	"strings"
	"testing"
)

// The two children do not accept the same provider flags: probe takes
// --region/--profile/--endpoint and has no destination, while transfer takes
// the paired --src-*/--dest-* set. These pin the separation without executing a
// child or reaching a provider, because lifting the cloud guards while handing
// either child the other's flags would produce an instrument that looks
// runnable and fails at the first real point.

func byoFixture() BYOS3Config {
	return BYOS3Config{
		Bucket:   "bucket-under-test",
		Endpoint: "https://endpoint.invalid",
		Region:   "auto",
		Profile:  "profile-under-test",
	}
}

func argvHas(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

func anyWithPrefix(args []string, prefix string) string {
	for _, a := range args {
		if strings.HasPrefix(a, prefix) {
			return a
		}
	}
	return ""
}

func TestProbeProviderFlagsAreProbeShaped(t *testing.T) {
	got := CLIProbeProviderFlags(byoFixture())

	for _, want := range []string{"--region", "--profile", "--endpoint"} {
		if !argvHas(got, want) {
			t.Fatalf("probe provider flags %v missing %s", got, want)
		}
	}
	// The transfer command's paired flags must never reach probe: it would
	// refuse them outright.
	if bad := anyWithPrefix(got, "--src-"); bad != "" {
		t.Fatalf("probe provider flags carry transfer flag %q", bad)
	}
	if bad := anyWithPrefix(got, "--dest-"); bad != "" {
		t.Fatalf("probe provider flags carry transfer flag %q", bad)
	}
}

func TestTransferProviderFlagsAreTransferShaped(t *testing.T) {
	got := CLIProviderFlags(byoFixture())

	for _, want := range []string{"--src-region", "--src-profile", "--src-endpoint", "--dest-region", "--dest-profile", "--dest-endpoint"} {
		if !argvHas(got, want) {
			t.Fatalf("transfer provider flags %v missing %s", got, want)
		}
	}
	// Bare probe-shaped flags would be ambiguous on the transfer command, which
	// addresses two endpoints.
	for _, bad := range []string{"--region", "--profile", "--endpoint"} {
		if argvHas(got, bad) {
			t.Fatalf("transfer provider flags carry bare %s", bad)
		}
	}
}

func TestProbeArgvCarriesOnlyProbeFlags(t *testing.T) {
	cases := []struct {
		name     string
		source   string
		extra    []string
		wantHead string
	}{
		{
			name:     "file backend takes no provider flags",
			source:   "file:///corpus/root/",
			extra:    nil,
			wantHead: "file:///corpus/root/",
		},
		{
			name:     "cloud backend takes probe-shaped flags and an object URI",
			source:   "s3://bucket-under-test/prefix/",
			extra:    CLIProbeProviderFlags(byoFixture()),
			wantHead: "s3://bucket-under-test/prefix/",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := ProbeArgs(tc.source, "/probe/config.yaml", 8, tc.extra)

			if args[0] != "content" || args[1] != "probe" {
				t.Fatalf("argv does not invoke content probe: %v", args[:2])
			}
			if args[2] != tc.wantHead {
				t.Fatalf("source is %q, want %q", args[2], tc.wantHead)
			}
			if !argvHas(args, "--emit") || !argvHas(args, "reflow-input") {
				t.Fatalf("probe argv must emit reflow-input: %v", args)
			}
			if bad := anyWithPrefix(args, "--src-"); bad != "" {
				t.Fatalf("probe argv carries transfer flag %q", bad)
			}
			if bad := anyWithPrefix(args, "--dest"); bad != "" {
				t.Fatalf("probe argv carries a destination flag %q; probe has no destination", bad)
			}
			if len(tc.extra) == 0 {
				for _, bad := range []string{"--region", "--profile", "--endpoint"} {
					if argvHas(args, bad) {
						t.Fatalf("local probe argv carries provider flag %s", bad)
					}
				}
			}
		})
	}
}

func TestReflowArgvCarriesOnlyTransferFlags(t *testing.T) {
	opts := FullPipeOpts{
		DestURI:         "s3://bucket-under-test/dest/",
		ReflowParallel:  8,
		CheckpointPath:  "/run/ckpt.db",
		ReflowExtraArgs: CLIProviderFlags(byoFixture()),
	}
	args := FullPipeReflowArgs(opts)

	if args[0] != "transfer" || args[1] != "reflow" {
		t.Fatalf("argv does not invoke transfer reflow: %v", args[:2])
	}
	if !argvHas(args, "--src-endpoint") || !argvHas(args, "--dest-endpoint") {
		t.Fatalf("reflow argv missing transfer provider flags: %v", args)
	}
	for _, bad := range []string{"--region", "--profile", "--endpoint"} {
		if argvHas(args, bad) {
			t.Fatalf("reflow argv carries bare probe flag %s", bad)
		}
	}
	if argvHas(args, "--emit") {
		t.Fatal("reflow argv carries a probe-only flag")
	}
}

// The rewrite template is not backend-independent: a local corpus yields bare
// hive keys, an object store prefixes them with the minted run prefix. A fixed
// template matches one and rejects every record on the other.
func TestReflowRewriteFollowsTheSourceKeyShape(t *testing.T) {
	local := FullPipeReflowArgs(FullPipeOpts{DestURI: "file:///dest/"})
	for i, a := range local {
		if a == "--rewrite-from" && local[i+1] != defaultHiveRewrite {
			t.Fatalf("local rewrite-from %q, want the bare hive identity", local[i+1])
		}
	}

	cloud := FullPipeReflowArgs(FullPipeOpts{
		DestURI:     "s3://bucket-under-test/dest/",
		RewriteFrom: "root/src-1234/" + defaultHiveRewrite,
	})
	var from string
	for i, a := range cloud {
		if a == "--rewrite-from" {
			from = cloud[i+1]
		}
	}
	if !strings.HasPrefix(from, "root/src-1234/") {
		t.Fatalf("cloud rewrite-from %q does not carry the minted source prefix", from)
	}
	if !strings.HasSuffix(from, defaultHiveRewrite) {
		t.Fatalf("cloud rewrite-from %q does not end in the hive captures", from)
	}
}

// Static credentials belong only to a backend that requires them. A real BYO
// backend must stay ambient or profile-based, so no key material is placed in a
// child environment or anywhere it could reach a report.
func TestChildCredentialEnvIsBackendScoped(t *testing.T) {
	real := byoFixture()
	if env := real.ChildAWSEnv(); len(env) != 0 {
		t.Fatalf("real BYO backend supplied child credentials: %v", env)
	}

	moto := byoFixture()
	moto.AccessKeyID = "test-key"
	moto.SecretAccessKey = "test-secret"
	env := moto.ChildAWSEnv()
	if len(env) == 0 {
		t.Fatal("a backend requiring static credentials supplied none")
	}
	if anyWithPrefix(env, "AWS_ACCESS_KEY_ID=") == "" {
		t.Fatalf("child env missing access key: %v", env)
	}
}

// An unset prefix must never widen to the whole bucket. The delete path already
// refuses it; the snapshot compared unrelated objects and reported a parity
// mismatch that had nothing to do with the run.
func TestSnapshotRefusesAnEmptyPrefix(t *testing.T) {
	for _, prefix := range []string{"", "   ", "/"} {
		if _, err := SnapshotS3DestPrefix(t.Context(), nil, prefix); err == nil {
			t.Fatalf("snapshot accepted prefix %q; it would list the entire bucket", prefix)
		}
	}
}
