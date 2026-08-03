package reflowthroughput

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
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
//
// The connection pool follows the same public policy as product constructions:
// provider.ResolveConnectionPool(admittedN) with admittedN = corpus upload
// fan-out. For N >= 2 both fields equal N so concurrent staging is not bound
// by net/http.DefaultMaxIdleConnsPerHost (2). For N < 2 the zero policy leaves
// SDK / transport defaults (same as product). Staging must not invent a second
// transport policy pattern.
func (c BYOS3Config) ProviderConfig() providers3.Config {
	n := CorpusUploadConcurrency()
	pool, err := provider.ResolveConnectionPool(n)
	if err != nil {
		// CorpusUploadConcurrency is always >= 1; refuse inventing knobs on error.
		pool = provider.ConnectionPoolPolicy{}
	}
	return providers3.Config{
		Bucket:              c.Bucket,
		Endpoint:            c.Endpoint,
		Region:              c.Region,
		Profile:             c.Profile,
		ForcePathStyle:      c.ForcePathStyle,
		AccessKeyID:         c.AccessKeyID,
		SecretAccessKey:     c.SecretAccessKey,
		MaxIdleConnsPerHost: pool.MaxIdleConnsPerHost,
		MaxConnsPerHost:     pool.MaxConnsPerHost,
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

// DefaultCorpusUploadConcurrency bounds the corpus upload fan-out.
//
// Corpus upload is setup, not measurement, but it is priced in round trips: a
// serial Put+Head loop costs two round trips per object and cannot exceed
// 1/(2*RTT) no matter how much capacity the lane has. Against a real S3
// endpoint that measured ~4.5 objects/s while the same lane sustained ~85/s at
// concurrency 16 -- so a 50k corpus spent roughly three hours uploading before
// the first measurement point, and a run budget sized for the measurement
// expired during setup.
//
// Concurrency here changes only how fast the fixture is staged. It does not
// touch the measured child, which is a separate process invoked afterwards.
const DefaultCorpusUploadConcurrency = 16

// CorpusUploadConcurrency resolves the upload fan-out, allowing an operator
// override for lanes that want a gentler staging rate.
func CorpusUploadConcurrency() int {
	raw := strings.TrimSpace(os.Getenv("GONIMBUS_THROUGHPUT_UPLOAD_CONCURRENCY"))
	if raw == "" {
		return DefaultCorpusUploadConcurrency
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 {
		return DefaultCorpusUploadConcurrency
	}
	return v
}

// UploadCorpusToS3 puts each local corpus object under sourcePrefix and returns
// a rewritten reflow.input.jsonl path using s3:// source URIs + dest_rel_key.
//
// Uploads run concurrently; the emitted input file stays in manifest order so
// the reflow input is byte-identical to what the serial form produced. Order is
// preserved by writing each result into its own slot rather than appending, so
// completion order cannot leak into the fixture.
func UploadCorpusToS3(ctx context.Context, p *providers3.Provider, cfg BYOS3Config, corpus GeneratedCorpus, sourcePrefix string, outInputPath string) error {
	return uploadCorpus(ctx, s3Stager{p: p}, cfg, corpus, sourcePrefix, outInputPath, CorpusUploadConcurrency())
}

// corpusStager is the one operation corpus staging needs from a backend. It
// exists so the fan-out and ordering discipline below can be driven by controls
// without a provider or a network -- a control that reimplements the loop would
// still pass if this function were reverted to its serial form, and so would
// prove nothing.
type corpusStager interface {
	Stage(ctx context.Context, key string, body []byte) (size int64, etag string, err error)
}

type s3Stager struct{ p *providers3.Provider }

func (s s3Stager) Stage(ctx context.Context, key string, body []byte) (int64, string, error) {
	if err := s.p.PutObjectWithOptions(ctx, key, bytes.NewReader(body), int64(len(body)), provider.PutOptions{
		ContentType: "application/xml",
	}); err != nil {
		return 0, "", fmt.Errorf("put source object: %w", err)
	}
	meta, err := s.p.Head(ctx, key)
	if err != nil {
		return 0, "", fmt.Errorf("head source object: %w", err)
	}
	return meta.Size, meta.ETag, nil
}

func uploadCorpus(ctx context.Context, stager corpusStager, cfg BYOS3Config, corpus GeneratedCorpus, sourcePrefix string, outInputPath string, conc int) error {
	entries := corpus.Manifest.Entries
	lines := make([]string, len(entries))

	if conc > len(entries) {
		conc = len(entries)
	}
	if conc < 1 {
		conc = 1
	}

	// A failing upload cancels its siblings: the first error is what the caller
	// reports, and letting the rest run would only add residue to clean up.
	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		errMu   sync.Mutex
		firstEr error
	)
	fail := func(err error) {
		errMu.Lock()
		if firstEr == nil {
			firstEr = err
			cancel()
		}
		errMu.Unlock()
	}

	work := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				e := entries[i]
				abs := filepath.Join(corpus.Root, filepath.FromSlash(e.RelativeKey))
				body, err := os.ReadFile(abs) // #nosec G304 -- harness-owned corpus path
				if err != nil {
					fail(err)
					return
				}
				key := sourcePrefix + e.RelativeKey
				size, etag, err := stager.Stage(uploadCtx, key, body)
				if err != nil {
					fail(err)
					return
				}
				// Prefer content size from local if head omits.
				if size == 0 {
					size = e.SizeBytes
				}
				line, err := marshalReflowInputLine(cfg.ObjectURI(key), e.RelativeKey, size, etag)
				if err != nil {
					fail(err)
					return
				}
				lines[i] = line
			}
		}()
	}

dispatch:
	for i := range entries {
		select {
		case work <- i:
		case <-uploadCtx.Done():
			// Stop dispatching. Continuing the loop here would drain the
			// range without staging anything and then fall through to a
			// successful return with empty slots.
			break dispatch
		}
	}
	close(work)
	wg.Wait()

	if firstEr != nil {
		return firstEr
	}
	// Cancellation that did not come from a staging failure — a parent budget
	// expiring mid-staging — must not be reported as success.
	if err := uploadCtx.Err(); err != nil {
		return fmt.Errorf("corpus staging interrupted: %w", err)
	}
	// Defensive invariant: the emitted fixture is what the measured child
	// consumes, so a gap in it would silently change the measurement rather
	// than fail the run.
	for i, l := range lines {
		if l == "" {
			return fmt.Errorf("corpus staging incomplete: no input line for entry %d of %d", i, len(lines))
		}
	}
	return writeFileAtomic(outInputPath, []byte(strings.Join(lines, "\n")+"\n"))
}

// writeFileAtomic publishes the input file by rename so a failed or partial
// write cannot leave a truncated fixture in place of a complete one.
func writeFileAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		if tmpName != "" {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	tmpName = ""
	return nil
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
	return deletePrefixVerified(ctx, prefixLister{p}, prefix, CorpusUploadConcurrency())
}

// prefixDeleter is the pair of operations teardown needs, extracted so the
// concurrency and verification discipline can be driven by controls without a
// provider.
type prefixDeleter interface {
	ListPage(ctx context.Context, prefix, token string) (keys []string, next string, err error)
	Delete(ctx context.Context, key string) error
	Count(ctx context.Context, prefix string) (int64, error)
}

type prefixLister struct{ p *providers3.Provider }

func (l prefixLister) ListPage(ctx context.Context, prefix, token string) ([]string, string, error) {
	res, err := l.p.List(ctx, provider.ListOptions{Prefix: prefix, ContinuationToken: token})
	if err != nil {
		return nil, "", fmt.Errorf("list for delete: %w", err)
	}
	keys := make([]string, 0, len(res.Objects))
	for _, o := range res.Objects {
		keys = append(keys, o.Key)
	}
	next := ""
	if res.IsTruncated {
		next = res.ContinuationToken
	}
	return keys, next, nil
}

func (l prefixLister) Delete(ctx context.Context, key string) error {
	if err := l.p.DeleteObject(ctx, key); err != nil {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}

func (l prefixLister) Count(ctx context.Context, prefix string) (int64, error) {
	return CountS3Prefix(ctx, l.p, prefix)
}

// deletePrefixVerified removes a prefix with a bounded worker pool.
//
// Teardown was one DeleteObject at a time, which is priced the same way staging
// was: a measured 50k run spent about two and a half hours deleting roughly 94k
// objects after the measurement had already finished. That cost is paid on
// every run, including failed ones, so it bounds how often the experiment can
// be repeated.
func deletePrefixVerified(ctx context.Context, d prefixDeleter, prefix string, conc int) error {
	if conc < 1 {
		conc = 1
	}
	delCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		errMu   sync.Mutex
		firstEr error
	)
	fail := func(err error) {
		errMu.Lock()
		if firstEr == nil {
			firstEr = err
			cancel()
		}
		errMu.Unlock()
	}

	work := make(chan string)
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range work {
				if err := d.Delete(delCtx, k); err != nil {
					fail(err)
					return
				}
			}
		}()
	}

	listErr := func() error {
		token := ""
		for {
			keys, next, err := d.ListPage(delCtx, prefix, token)
			if err != nil {
				return err
			}
			for _, k := range keys {
				select {
				case work <- k:
				case <-delCtx.Done():
					return nil
				}
			}
			if next == "" {
				return nil
			}
			token = next
		}
	}()
	close(work)
	wg.Wait()

	if firstEr != nil {
		return firstEr
	}
	if listErr != nil {
		return listErr
	}
	if err := delCtx.Err(); err != nil {
		return fmt.Errorf("prefix teardown interrupted: %w", err)
	}

	// The verification is the point of this helper: a teardown that reports
	// success while objects remain would let a run claim zero residue it never
	// achieved.
	left, err := d.Count(ctx, prefix)
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

// CLIProbeProviderFlags returns the provider flags `content probe` accepts.
//
// Deliberately separate from CLIProviderFlags rather than reusing it. That
// helper emits the transfer command's paired --src-*/--dest-* flags, which
// probe does not accept: probe reads one source and has no destination. Handing
// the transfer set to a probe child would fail on an unknown flag, and handing
// it a union would give it flags that mean nothing there.
func CLIProbeProviderFlags(cfg BYOS3Config) []string {
	var args []string
	add := func(flag, val string) {
		if strings.TrimSpace(val) == "" {
			return
		}
		args = append(args, flag, val)
	}
	add("--region", cfg.Region)
	add("--profile", cfg.Profile)
	add("--endpoint", cfg.Endpoint)
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

// SnapshotS3DestPrefix returns a sorted content-identity multiset for objects
// under prefix, matching SnapshotDestTree's shape.
//
// It exists so the fullpipe A/B parity check keeps its meaning on an object
// store. The local check walks a destination directory, which a cloud point
// does not have; skipping it there would quietly drop the guarantee the "ab"
// in the profile name refers to, on exactly the runs the attribution depends on.
//
// Identity is the provider ETag rather than a recomputed content hash. Both
// arms are listed the same way and compared only with each other, so the ETag
// is a sufficient identity for parity without re-downloading the corpus twice.
func SnapshotS3DestPrefix(ctx context.Context, p *providers3.Provider, prefix string) ([]LandedObjectID, error) {
	// Same refusal as DeleteS3PrefixVerified, for the same reason: an unset
	// prefix would silently widen to the entire bucket. On a delete that is
	// destruction; here it is a parity check that compares unrelated objects
	// and reports a mismatch that has nothing to do with the run.
	if strings.TrimSpace(prefix) == "" || prefix == "/" {
		return nil, fmt.Errorf("refusing to snapshot an empty or root prefix")
	}
	var out []LandedObjectID
	token := ""
	for {
		res, err := p.List(ctx, provider.ListOptions{Prefix: prefix, ContinuationToken: token})
		if err != nil {
			return nil, fmt.Errorf("list dest prefix: %w", err)
		}
		for _, obj := range res.Objects {
			out = append(out, LandedObjectID{
				RelKey: strings.TrimPrefix(strings.TrimPrefix(obj.Key, prefix), "/"),
				Size:   obj.Size,
				Digest: strings.Trim(obj.ETag, "\""),
			})
		}
		if !res.IsTruncated || res.ContinuationToken == "" {
			break
		}
		token = res.ContinuationToken
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RelKey != out[j].RelKey {
			return out[i].RelKey < out[j].RelKey
		}
		if out[i].Size != out[j].Size {
			return out[i].Size < out[j].Size
		}
		return out[i].Digest < out[j].Digest
	})
	return out, nil
}
