package reflowthroughput

import (
	"testing"

	"github.com/3leaps/gonimbus/test/cloudtest"
)

func TestLoadBYOS3ConfigOptIn(t *testing.T) {
	// Cannot t.Parallel with t.Setenv.
	t.Setenv(cloudtest.RealS3BucketEnv, "byo-test-bucket-example")
	t.Setenv(cloudtest.RealS3EndpointEnv, "https://s3.example.invalid")
	t.Setenv(cloudtest.RealS3RegionEnv, "us-east-1")
	t.Setenv(cloudtest.RealS3ProfileEnv, "")
	t.Setenv(cloudtest.RealS3PrefixEnv, "")
	t.Setenv(cloudtest.RealS3ForcePathStyleEnv, "")
	cfg, ok := LoadBYOS3Config()
	if !ok {
		t.Fatal("expected ok when bucket set")
	}
	if cfg.Bucket != "byo-test-bucket-example" {
		t.Fatalf("bucket=%s", cfg.Bucket)
	}
	if !cfg.ForcePathStyle {
		t.Fatal("endpoint set should default force path style")
	}
	uri := cfg.ObjectURI("pref/a.xml")
	if uri != "s3://byo-test-bucket-example/pref/a.xml" {
		t.Fatalf("uri=%s", uri)
	}
	pref := cfg.MintUniquePrefix("smoke")
	if pref == "" || pref[len(pref)-1] != '/' {
		t.Fatalf("prefix=%q", pref)
	}
}

func TestResolveProviderClass(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"":              ProviderFile,
		"file":          ProviderFile,
		"s3":            ProviderS3Compatible,
		"s3-compatible": ProviderS3Compatible,
		"byo-s3":        ProviderS3Compatible,
		"moto":          ProviderMoto,
		"gcs":           ProviderGCS,
	}
	for in, want := range cases {
		got, err := ResolveProviderClass(in)
		if err != nil || got != want {
			t.Fatalf("in=%q got=%q err=%v want=%q", in, got, err, want)
		}
	}
	if _, err := ResolveProviderClass("nope"); err == nil {
		t.Fatal("expected error")
	}
}

func TestCLIProviderFlagsOmitEmpty(t *testing.T) {
	t.Parallel()
	cfg := BYOS3Config{Region: "us-west-2", Endpoint: "http://localhost:5555"}
	args := CLIProviderFlags(cfg)
	joined := ""
	for _, a := range args {
		joined += a + " "
	}
	if !contains(joined, "--src-region") || !contains(joined, "us-west-2") {
		t.Fatalf("args=%v", args)
	}
	if contains(joined, "--src-profile") {
		t.Fatalf("empty profile should be omitted: %v", args)
	}
}
