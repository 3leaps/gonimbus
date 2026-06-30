package reflow

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/3leaps/gonimbus/pkg/provider"
)

// sentinelProvider is a provider handle whose String/GoString deliberately leak a
// secret, so the test fails if Config/Destination formatting ever recurses into
// the injected handle.
type sentinelProvider struct{}

const providerSecretSentinel = "SECRET-TOKEN-aws_secret_access_key-LEAKED"

func (sentinelProvider) String() string   { return providerSecretSentinel }
func (sentinelProvider) GoString() string { return providerSecretSentinel }
func (sentinelProvider) List(context.Context, provider.ListOptions) (*provider.ListResult, error) {
	return nil, nil
}
func (sentinelProvider) Head(context.Context, string) (*provider.ObjectMeta, error) {
	return nil, nil
}
func (sentinelProvider) Close() error { return nil }

func TestDestinationStringRedactsProviderHandle(t *testing.T) {
	d := Destination{Provider: sentinelProvider{}, ProviderID: "s3", BaseURI: "s3://bucket/base/"}
	for _, got := range []string{d.String(), d.GoString(), fmt.Sprintf("%v", d), fmt.Sprintf("%#v", d)} {
		if strings.Contains(got, providerSecretSentinel) {
			t.Fatalf("Destination formatting leaked provider handle: %q", got)
		}
		if !strings.Contains(got, "<redacted>") {
			t.Fatalf("Destination formatting should mark the handle redacted: %q", got)
		}
		if !strings.Contains(got, "s3://bucket/base/") {
			t.Fatalf("Destination formatting should keep the non-secret BaseURI: %q", got)
		}
	}
}

func TestConfigStringRedactsInjectedHandles(t *testing.T) {
	cfg := Config{
		Destination: Destination{Provider: sentinelProvider{}, ProviderID: "s3", BaseURI: "s3://bucket/base/"},
		Rewrite:     RewriteConfig{From: "{a}/{file}", To: "{file}"},
		Collision:   CollisionPolicy{Mode: "skip-if-duplicate"},
		Checkpoint:  nil,
		Events:      nil,
	}
	for _, got := range []string{cfg.String(), cfg.GoString(), fmt.Sprintf("%v", cfg), fmt.Sprintf("%#v", cfg)} {
		if strings.Contains(got, providerSecretSentinel) {
			t.Fatalf("Config formatting leaked provider handle: %q", got)
		}
	}
	// A nil sink renders <nil>; a set sink renders <set>, never the value.
	cfg.Events = noopSink{}
	if got := cfg.String(); !strings.Contains(got, "Events:<set>") {
		t.Fatalf("Config.String should mark a set EventSink as <set>: %q", got)
	}
}

func TestConfigStringRedactsMetadataValues(t *testing.T) {
	const metadataSecretSentinel = "SECRET-metadata-token-value"
	cfg := Config{
		Destination: Destination{Provider: sentinelProvider{}, ProviderID: "s3", BaseURI: "s3://bucket/base/"},
		Metadata: MetadataPlan{
			Policy: MetadataPolicyMerge,
			Set: map[string]string{
				"public-name": metadataSecretSentinel,
			},
			SourceKeyRules:          []MetadataSourceKeyRule{{DestKey: "from-source", SourceKey: "source-key", Raw: "from-source=source-key"}},
			DerivedRules:            []MetadataDerivedRule{{DestKey: "derived", Expression: "system.etag", Raw: "derived=system.etag"}},
			OnMissingSource:         MetadataOnMissingFail,
			PreserveContentType:     true,
			DestinationStorageClass: "STANDARD_IA",
			MetadataSidecarSuffix:   ".metadata.json",
		},
	}
	for _, got := range []string{cfg.String(), cfg.GoString(), fmt.Sprintf("%v", cfg), fmt.Sprintf("%#v", cfg)} {
		if strings.Contains(got, metadataSecretSentinel) || strings.Contains(got, "system.etag") || strings.Contains(got, ".metadata.json") {
			t.Fatalf("Config formatting leaked metadata configuration values: %q", got)
		}
		if !strings.Contains(got, "Policy:\"merge\"") || !strings.Contains(got, "Set:1") || !strings.Contains(got, "DerivedRules:1") {
			t.Fatalf("Config formatting should keep value-free metadata shape: %q", got)
		}
	}
}

func TestSourceStringRedactsProviderHandle(t *testing.T) {
	sources := []Source{
		ObjectSource{Provider: sentinelProvider{}, URI: "s3://bucket/key"},
		PrefixSource{Provider: sentinelProvider{}, URI: "s3://bucket/prefix/"},
		RecordStreamSource{Records: strings.NewReader("x"), Resolve: func(context.Context, string) (provider.Provider, error) { return sentinelProvider{}, nil }},
	}
	for _, src := range sources {
		for _, got := range []string{fmt.Sprintf("%v", src), fmt.Sprintf("%+v", src), fmt.Sprintf("%#v", src), fmt.Sprintf("%s", src)} {
			if strings.Contains(got, providerSecretSentinel) {
				t.Fatalf("%T formatting leaked provider handle: %q", src, got)
			}
		}
	}
	// Object/Prefix mark the handle redacted; RecordStream renders presence only.
	if got := fmt.Sprintf("%v", ObjectSource{Provider: sentinelProvider{}, URI: "s3://bucket/key"}); !strings.Contains(got, "<redacted>") || !strings.Contains(got, "s3://bucket/key") {
		t.Fatalf("ObjectSource.String should redact handle and keep URI: %q", got)
	}
	if got := fmt.Sprintf("%v", RecordStreamSource{Records: strings.NewReader("x"), Resolve: func(context.Context, string) (provider.Provider, error) { return nil, nil }}); !strings.Contains(got, "Records:<set>") || !strings.Contains(got, "Resolve:<set>") {
		t.Fatalf("RecordStreamSource.String should render presence only: %q", got)
	}
}

func TestFileTreeSourceStringRedactsLocalRoot(t *testing.T) {
	const secretRoot = "/Users/someone/private/exfil-staging/root"
	src := FileTreeSource{Root: secretRoot}
	for _, got := range []string{src.String(), src.GoString(), fmt.Sprintf("%v", src), fmt.Sprintf("%+v", src), fmt.Sprintf("%#v", src)} {
		if strings.Contains(got, secretRoot) {
			t.Fatalf("FileTreeSource formatting leaked local root path: %q", got)
		}
		if !strings.Contains(got, "Root:<set>") {
			t.Fatalf("FileTreeSource formatting should render root presence only: %q", got)
		}
	}
	if got := fmt.Sprintf("%v", FileTreeSource{}); !strings.Contains(got, "Root:<empty>") {
		t.Fatalf("empty FileTreeSource should render Root:<empty>: %q", got)
	}
}

type noopSink struct{}

func (noopSink) OnRun(context.Context, RunRecord) error          { return nil }
func (noopSink) OnSource(context.Context, SourceRunRecord) error { return nil }
func (noopSink) OnRecord(context.Context, Record) error          { return nil }
func (noopSink) OnWarning(context.Context, Warning) error        { return nil }
func (noopSink) OnError(context.Context, ErrorEvent) error       { return nil }
func (noopSink) OnSummary(context.Context, SummaryRecord) error  { return nil }
