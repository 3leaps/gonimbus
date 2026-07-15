package reflowthroughput

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/3leaps/gonimbus/pkg/provider"
	providers3 "github.com/3leaps/gonimbus/pkg/provider/s3"
	"github.com/3leaps/gonimbus/test/cloudtest"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Provider class names used in Options and sanitized reports.
const (
	ProviderFile         = "file"
	ProviderMoto         = "moto"
	ProviderS3Compatible = "s3-compatible"
	ProviderGCS          = "gcs"
)

// BYOS3Config is the harness view of the existing bring-your-own S3-compatible
// lane (same env vars as test/cloudtest and make test-cloud-real).
// Bucket/endpoint/profile never enter the sanitized report.
type BYOS3Config struct {
	Bucket         string
	Endpoint       string
	Region         string
	Profile        string
	RootPrefix     string
	ForcePathStyle bool
	// Optional static keys for moto only (cloudtest.TestAccessKeyID pattern).
	// Never used for real BYO — real-cloud uses ambient/profile chain only.
	AccessKeyID     string
	SecretAccessKey string
}

// LoadBYOS3Config reads GONIMBUS_S3_TEST_* (cloudtest constants). Returns
// ok=false when the opt-in bucket is unset — callers skip, they do not fail.
func LoadBYOS3Config() (cfg BYOS3Config, ok bool) {
	bucket := strings.TrimSpace(os.Getenv(cloudtest.RealS3BucketEnv))
	if bucket == "" {
		return BYOS3Config{}, false
	}
	endpoint := strings.TrimSpace(os.Getenv(cloudtest.RealS3EndpointEnv))
	forcePathStyle := endpoint != ""
	if raw := strings.TrimSpace(os.Getenv(cloudtest.RealS3ForcePathStyleEnv)); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err == nil {
			forcePathStyle = parsed
		}
	}
	return BYOS3Config{
		Bucket:         bucket,
		Endpoint:       endpoint,
		Region:         strings.TrimSpace(os.Getenv(cloudtest.RealS3RegionEnv)),
		Profile:        strings.TrimSpace(os.Getenv(cloudtest.RealS3ProfileEnv)),
		RootPrefix:     strings.Trim(strings.TrimSpace(os.Getenv(cloudtest.RealS3PrefixEnv)), "/"),
		ForcePathStyle: forcePathStyle,
	}, true
}

// ProviderConfig maps to the S3 provider constructor (ambient credential chain
// for real BYO; optional static keys for moto only).
func (c BYOS3Config) ProviderConfig() providers3.Config {
	return providers3.Config{
		Bucket:          c.Bucket,
		Endpoint:        c.Endpoint,
		Region:          c.Region,
		Profile:         c.Profile,
		ForcePathStyle:  c.ForcePathStyle,
		AccessKeyID:     c.AccessKeyID,
		SecretAccessKey: c.SecretAccessKey,
	}
}

// ChildAWSEnv returns extra KEY=value pairs for the gonimbus child when static
// moto credentials are required. Empty for real BYO (ambient chain / profile).
func (c BYOS3Config) ChildAWSEnv() []string {
	if c.AccessKeyID == "" {
		return nil
	}
	return []string{
		"AWS_ACCESS_KEY_ID=" + c.AccessKeyID,
		"AWS_SECRET_ACCESS_KEY=" + c.SecretAccessKey,
		"AWS_EC2_METADATA_DISABLED=true",
	}
}

// ObjectURI builds s3://bucket/key (not written into reports).
func (c BYOS3Config) ObjectURI(key string) string {
	return "s3://" + c.Bucket + "/" + strings.TrimPrefix(key, "/")
}

// MintUniquePrefix returns a unique object prefix under the operator root,
// matching cloudtest.CreateS3ObjectPrefix naming shape without testing.T.
func (c BYOS3Config) MintUniquePrefix(slug string) string {
	root := c.RootPrefix
	if root == "" {
		root = "gonimbus-reflow-throughput"
	}
	slug = strings.ToLower(slug)
	slug = strings.NewReplacer("/", "-", "_", "-", " ", "-").Replace(slug)
	if slug == "" {
		slug = "run"
	}
	return fmt.Sprintf("%s/%s-%d/", root, slug, time.Now().UnixNano())
}

// OpenS3Provider constructs an S3-compatible provider from BYO config.
func OpenS3Provider(ctx context.Context, cfg BYOS3Config) (*providers3.Provider, error) {
	return providers3.New(ctx, cfg.ProviderConfig())
}

// UploadCorpusToS3 puts each local corpus object under sourcePrefix and returns
// a rewritten reflow.input.jsonl path using s3:// source URIs + dest_rel_key.
func UploadCorpusToS3(ctx context.Context, p *providers3.Provider, cfg BYOS3Config, corpus GeneratedCorpus, sourcePrefix string, outInputPath string) error {
	var lines []string
	for _, e := range corpus.Manifest.Entries {
		abs := filepath.Join(corpus.Root, filepath.FromSlash(e.RelativeKey))
		body, err := os.ReadFile(abs) // #nosec G304 -- harness-owned corpus path
		if err != nil {
			return err
		}
		key := sourcePrefix + e.RelativeKey
		if err := p.PutObjectWithOptions(ctx, key, bytes.NewReader(body), int64(len(body)), provider.PutOptions{
			ContentType: "application/xml",
		}); err != nil {
			return fmt.Errorf("put source object: %w", err)
		}
		meta, err := p.Head(ctx, key)
		if err != nil {
			return fmt.Errorf("head source object: %w", err)
		}
		line, err := marshalReflowInputLine(cfg.ObjectURI(key), e.RelativeKey, meta.Size, meta.ETag)
		if err != nil {
			return err
		}
		// Prefer content size from local if head omits.
		if meta.Size == 0 {
			line, err = marshalReflowInputLine(cfg.ObjectURI(key), e.RelativeKey, e.SizeBytes, meta.ETag)
			if err != nil {
				return err
			}
		}
		lines = append(lines, line)
	}
	return os.WriteFile(outInputPath, []byte(strings.Join(lines, "\n")+"\n"), 0o644)
}

// CountS3Prefix counts objects under prefix (for post-run dest object count).
func CountS3Prefix(ctx context.Context, p *providers3.Provider, prefix string) (int64, error) {
	var n int64
	token := ""
	for {
		res, err := p.List(ctx, provider.ListOptions{Prefix: prefix, ContinuationToken: token})
		if err != nil {
			return 0, err
		}
		n += int64(len(res.Objects))
		if !res.IsTruncated || res.ContinuationToken == "" {
			return n, nil
		}
		token = res.ContinuationToken
	}
}

// DeleteS3PrefixVerified deletes all objects under prefix, then lists again and
// fails if any remain. Stricter than cloudtest's log-on-cleanup-failure helper
// (measurement harness AC: verified cleanup).
func DeleteS3PrefixVerified(ctx context.Context, p *providers3.Provider, prefix string) error {
	if prefix == "" || prefix == "/" {
		return fmt.Errorf("refusing to delete empty or root prefix")
	}
	token := ""
	for {
		res, err := p.List(ctx, provider.ListOptions{Prefix: prefix, ContinuationToken: token})
		if err != nil {
			return fmt.Errorf("list for delete: %w", err)
		}
		for _, obj := range res.Objects {
			if err := p.DeleteObject(ctx, obj.Key); err != nil {
				return fmt.Errorf("delete %s: %w", obj.Key, err)
			}
		}
		if !res.IsTruncated || res.ContinuationToken == "" {
			break
		}
		token = res.ContinuationToken
	}
	left, err := CountS3Prefix(ctx, p, prefix)
	if err != nil {
		return fmt.Errorf("post-delete list: %w", err)
	}
	if left != 0 {
		return fmt.Errorf("cleanup incomplete: %d objects remain under minted prefix", left)
	}
	return nil
}

// CLIProviderFlags returns transfer reflow --src-* / --dest-* args for BYO S3.
// Empty strings are omitted (same as release-stress real-cloud pattern).
func CLIProviderFlags(cfg BYOS3Config) []string {
	var args []string
	add := func(flag, val string) {
		if strings.TrimSpace(val) == "" {
			return
		}
		args = append(args, flag, val)
	}
	add("--src-region", cfg.Region)
	add("--src-profile", cfg.Profile)
	add("--src-endpoint", cfg.Endpoint)
	add("--dest-region", cfg.Region)
	add("--dest-profile", cfg.Profile)
	add("--dest-endpoint", cfg.Endpoint)
	return args
}

// ResolveProviderClass normalizes Options.Provider / env.
// Empty → file. Unknown → error. s3/byo-s3 → s3-compatible.
func ResolveProviderClass(name string) (string, error) {
	name = strings.TrimSpace(strings.ToLower(name))
	if name == "" {
		name = ProviderFile
	}
	switch name {
	case ProviderFile, "local":
		return ProviderFile, nil
	case ProviderMoto:
		return ProviderMoto, nil
	case ProviderS3Compatible, "s3", "byo-s3", "real-s3":
		return ProviderS3Compatible, nil
	case ProviderGCS, "byo-gcs", "real-gcs":
		// Accepted name so docs stay honest; Run returns a clear "not implemented"
		// only after verifying BYO env is present (avoids silent false advertising).
		return ProviderGCS, nil
	default:
		return "", fmt.Errorf("unknown provider class %q (file|moto|s3-compatible|gcs)", name)
	}
}

// MotoAvailable reports whether a local moto endpoint is reachable (cloudtest).
func MotoAvailable() bool {
	return cloudtest.Available()
}

// CreateMotoBucket creates a unique moto bucket using the existing cloudtest
// S3 client (same credentials/endpoint as the moto integration lane).
func CreateMotoBucket(ctx context.Context, name string) error {
	c, err := cloudtest.Client()
	if err != nil {
		return err
	}
	_, err = c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(name)})
	return err
}
