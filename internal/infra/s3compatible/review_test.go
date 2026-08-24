package s3compatible

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Rationale: provider bucket and prefix inputs participate in request
// authority, so only canonical DNS buckets and canonical bounded key prefixes
// may reach the SDK.
func TestConfigRejectsNonCanonicalBucketAndPrefix(t *testing.T) {
	for _, bucket := range []string{
		"192.168.1.1", "999.999.999.999", "bucket.-name", "bucket-.name", "bucket..name", "Bucket",
		"xn--bucket", "sthree-bucket", "amzn-s3-demo-bucket", "bucket-s3alias", "bucket--ol-s3",
		"bucket.mrap", "bucket--x-s3", "bucket--table-s3",
	} {
		config := testConfig()
		config.Bucket = bucket
		if kind, ok := errs.KindOf(validateConfig(config)); !ok || kind != errs.KindValidationFailed {
			t.Errorf("bucket %q validation kind = %v, %t", bucket, kind, ok)
		}
	}
	for _, prefix := range []string{
		"double//slash/", "back\\slash/", "nul\x00byte/", string([]byte{0xff, '/'}),
		strings.Repeat("a", 1024) + "/",
	} {
		config := testConfig()
		config.Prefix = prefix
		if kind, ok := errs.KindOf(validateConfig(config)); !ok || kind != errs.KindValidationFailed {
			t.Errorf("prefix %q validation kind = %v, %t", prefix, kind, ok)
		}
	}
}

// Rationale: endpoint and region values enter HTTP/signing state, so their
// closed transport limits are byte-exact and independent of ambient SDK rules.
func TestEndpointAndRegionTransportBoundaries(t *testing.T) {
	config := testConfig()
	config.Endpoint = "https://" + strings.Repeat("a", endpointMaxBytes-len("https://"))
	config.Region = strings.Repeat("r", regionMaxBytes)
	if err := validateConfig(config); err != nil {
		t.Fatalf("maximum endpoint/region Validate() error = %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*Config)
		valid  bool
	}{
		{"endpoint 2049 bytes", func(value *Config) {
			value.Endpoint = "https://" + strings.Repeat("a", endpointMaxBytes-len("https://")+1)
		}, false},
		{"endpoint invalid UTF-8", func(value *Config) {
			value.Endpoint = "https://example" + string([]byte{0xff}) + ".test"
		}, false},
		{"region 1 byte", func(value *Config) { value.Region = "r" }, true},
		{"region 0 bytes", func(value *Config) { value.Region = "" }, false},
		{"region 65 bytes", func(value *Config) { value.Region = strings.Repeat("r", regionMaxBytes+1) }, false},
		{"region non-ASCII", func(value *Config) { value.Region = "eu-west-ł" }, false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			candidate := testConfig()
			test.mutate(&candidate)
			err := validateConfig(candidate)
			if test.valid && err != nil {
				t.Fatalf("validateConfig() error = %v", err)
			}
			if !test.valid {
				if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed {
					t.Fatalf("validateConfig() kind = %v, %t", kind, ok)
				}
			}
		})
	}
}

// Rationale: invalid SDK-returned evidence is a provider contract rejection;
// a present invalid VersionId must not fall back to an otherwise valid ETag.
func TestProviderDiscriminatorEvidenceFailsClosed(t *testing.T) {
	invalidUTF8 := string([]byte{0xff})
	for _, test := range []struct {
		name      string
		versionID *string
		etag      *string
	}{
		{"invalid VersionId precedence", &invalidUTF8, aws.String(`"valid-etag"`)},
		{"invalid ETag", nil, &invalidUTF8},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := outputDiscriminator(test.versionID, test.etag)
			if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed ||
				!strings.Contains(err.Error(), "provider contract was rejected") {
				t.Fatalf("outputDiscriminator() error = %v, kind = %v, %t", err, kind, ok)
			}
		})
	}
}

// Rationale: the pinned SDK retries timeout and throttle API codes even when
// providers return HTTP 400, so exhausted attempts must retain unavailable
// taxonomy instead of becoming deterministic provider rejections.
func TestTransientAPICodesPrecedeGenericHTTPStatus(t *testing.T) {
	codes := []string{
		"InternalError", "RequestTimeout", "RequestTimeoutException", "ServiceUnavailable", "Throttling",
		"ThrottlingException", "ThrottledException", "RequestThrottledException", "TooManyRequestsException",
		"ProvisionedThroughputExceededException", "TransactionInProgressException", "RequestLimitExceeded",
		"BandwidthLimitExceeded", "LimitExceededException", "RequestThrottled", "SlowDown",
		"PriorRequestNotComplete", "EC2ThrottledException",
	}
	for _, code := range codes {
		err := providerError(providerTestError{status: 400, code: code, text: "private=LEAK"})
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStorageUnavailable || strings.Contains(err.Error(), "LEAK") {
			t.Errorf("providerError(%q) = %v, kind = %v, %t", code, err, kind, ok)
		}
	}
}

// Rationale: a nil successful Put response is a provider-contract rejection,
// not a panic or an opportunity to leak SDK diagnostics.
func TestPutExactRejectsNilProviderOutput(t *testing.T) {
	body := []byte("stored artifact")
	fake := &fakeS3{putObject: func(*s3.PutObjectInput, []func(*s3.Options)) (*s3.PutObjectOutput, error) {
		return nil, nil
	}}
	_, err := mustAdapter(t, fake).PutExact(
		context.Background(), testArtifact(body, "none"), bytes.NewReader(body),
	)
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed ||
		!strings.Contains(err.Error(), "provider contract was rejected") {
		t.Fatalf("PutExact() error = %v, kind = %v, %t", err, kind, ok)
	}
}

// Rationale: every ambiguous Create attempt can leave a server-side upload,
// so both attempts require list/abort/re-list proof before returning.
func TestCreateMultipartSecondAmbiguityAlsoProvesNoUploads(t *testing.T) {
	createCalls := 0
	listCalls := 0
	fake := &fakeS3{
		createMultipartUpload: func(*s3.CreateMultipartUploadInput, []func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
			createCalls++
			return nil, timeoutTestError{}
		},
		listMultipartUploads: func(*s3.ListMultipartUploadsInput) (*s3.ListMultipartUploadsOutput, error) {
			listCalls++
			return &s3.ListMultipartUploadsOutput{}, nil
		},
	}
	_, err := mustAdapter(t, fake).createMultipart(
		context.Background(), testArtifact(nil, "none"),
	)
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStorageUnavailable {
		t.Fatalf("createMultipart() error = %v, kind = %v, %t", err, kind, ok)
	}
	if createCalls != 2 || listCalls != 4 {
		t.Fatalf("calls = create %d list %d, want 2/4", createCalls, listCalls)
	}
}

// Rationale: hashing large parts must observe cancellation between bounded
// reads instead of completing an uninterruptible whole-part copy.
func TestDigestRangeChecksContextBetweenBoundedReads(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := &cancelingReaderAt{cancel: cancel}
	_, _, err := digestRange(ctx, reader, 0, 128*1024, nil)
	if !errors.Is(err, context.Canceled) || reader.reads != 1 {
		t.Fatalf("digestRange() error = %v, reads = %d", err, reader.reads)
	}
}

// Rationale: response cleanup is part of the Get operation; cancellation is
// preserved and Close is still attempted through the ctx-first helper.
func TestCloseObjectBodyPreservesContextAndCloses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	body := &recordingCloser{}
	err := closeObjectBody(ctx, body, nil)
	if !body.closed || !errors.Is(err, context.Canceled) {
		t.Fatalf("closeObjectBody() error = %v, closed = %t", err, body.closed)
	}
}

type cancelingReaderAt struct {
	cancel context.CancelFunc
	reads  int
}

func (reader *cancelingReaderAt) ReadAt(buffer []byte, _ int64) (int, error) {
	reader.reads++
	for index := range buffer {
		buffer[index] = 0
	}
	reader.cancel()
	return len(buffer), nil
}

type recordingCloser struct {
	closed bool
}

func (closer *recordingCloser) Close() error {
	closer.closed = true
	return nil
}

var _ io.ReaderAt = (*cancelingReaderAt)(nil)
