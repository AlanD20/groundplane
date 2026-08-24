package s3compatible

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"maps"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Rationale: the Connector must be reproducible and must not inherit ambient
// credentials, proxy behavior, redirect behavior, checksum modes, or retries.
func TestExplicitSDKConfiguration(t *testing.T) {
	config := testConfig()
	awsConfig := newAWSConfig(config)
	if awsConfig.Region != config.Region || aws.ToString(awsConfig.BaseEndpoint) != config.Endpoint {
		t.Fatalf("AWS config identity = region %q endpoint %q", awsConfig.Region, aws.ToString(awsConfig.BaseEndpoint))
	}
	credentials, err := awsConfig.Credentials.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Retrieve() error = %v", err)
	}
	if credentials.AccessKeyID != config.AccessKey || credentials.SecretAccessKey != config.SecretKey {
		t.Fatal("static credentials were not preserved")
	}
	if awsConfig.RequestChecksumCalculation != aws.RequestChecksumCalculationWhenRequired ||
		awsConfig.ResponseChecksumValidation != aws.ResponseChecksumValidationWhenRequired {
		t.Fatal("checksum modes are not WHEN_REQUIRED")
	}
	if attempts := awsConfig.Retryer().MaxAttempts(); attempts != 3 {
		t.Fatalf("retry attempts = %d, want 3", attempts)
	}
	client := awsConfig.HTTPClient.(*http.Client)
	transport := client.Transport.(*http.Transport)
	if transport.Proxy != nil {
		t.Fatal("HTTP proxy discovery is enabled")
	}
	if transport.TLSClientConfig == nil || transport.TLSClientConfig.RootCAs != nil {
		t.Fatal("TLS must use the system trust roots without a custom CA")
	}
	if err := client.CheckRedirect(&http.Request{}, nil); err == nil {
		t.Fatal("CheckRedirect() unexpectedly allowed a redirect")
	} else if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed {
		t.Fatalf("CheckRedirect() error = %v, kind = %v, %t", err, kind, ok)
	}
}

// Rationale: a small immutable upload must carry the exact body MD5, metadata,
// size, and no-overwrite condition while preserving an absent VersionId as ETag.
func TestPutExactSmallObject(t *testing.T) {
	body := []byte("stored artifact")
	artifact := testArtifact(body, backupobject.EncryptionNone)
	fake := &fakeS3{
		putObject: func(input *s3.PutObjectInput, options []func(*s3.Options)) (*s3.PutObjectOutput, error) {
			actual, err := io.ReadAll(input.Body)
			if err != nil {
				t.Fatalf("ReadAll(body) error = %v", err)
			}
			md5Sum := md5.Sum(body)
			if !bytes.Equal(actual, body) || aws.ToString(input.IfNoneMatch) != "*" ||
				aws.ToInt64(input.ContentLength) != int64(len(body)) ||
				aws.ToString(input.ContentMD5) != base64.StdEncoding.EncodeToString(md5Sum[:]) ||
				!maps.Equal(input.Metadata, artifact.Metadata()) || len(options) != 0 {
				t.Fatalf("PutObject input = %#v options=%d body=%q", input, len(options), actual)
			}
			return &s3.PutObjectOutput{ETag: aws.String("\"etag\"")}, nil
		},
	}
	adapter := mustAdapter(t, fake)
	object, err := adapter.PutExact(context.Background(), artifact, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("PutExact() error = %v", err)
	}
	if object.Discriminator != (backupobject.Discriminator{Kind: backupobject.DiscriminatorETag, Value: "\"etag\""}) {
		t.Fatalf("discriminator = %#v", object.Discriminator)
	}
}

// Rationale: multipart sizing must remain deterministic at the threshold and
// at 5 TiB and never exceed the provider's 10,000-part limit.
func TestMultipartPartSizeFormula(t *testing.T) {
	cases := []struct {
		size uint64
		want uint64
	}{
		{singlePutMaximum + 1, singlePutMaximum},
		{backupobject.MaxObjectSize, 525 * partQuantum},
	}
	for _, test := range cases {
		got := multipartPartSize(test.size)
		if got != test.want {
			t.Errorf("multipartPartSize(%d) = %d, want %d", test.size, got, test.want)
		}
		if parts := (test.size + got - 1) / got; parts > maximumParts {
			t.Errorf("part count = %d, want <= %d", parts, maximumParts)
		}
	}
}

// Rationale: crossing 100 MiB selects sequential multipart upload with exact
// formula-derived parts, per-part MD5, and one-attempt create/complete calls.
func TestPutExactMultipartIsSequentialAndConditional(t *testing.T) {
	size := singlePutMaximum + 1
	digest := hashZeroBytes(t, size)
	artifact := testArtifact(nil, backupobject.EncryptionNone)
	artifact.Evidence = backupobject.Evidence{
		SourceSizeBytes: size,
		SourceSHA256:    digest,
		StoredSizeBytes: size,
		StoredSHA256:    digest,
	}
	uploadedParts := 0
	fake := &fakeS3{
		createMultipartUpload: func(
			input *s3.CreateMultipartUploadInput,
			options []func(*s3.Options),
		) (*s3.CreateMultipartUploadOutput, error) {
			if aws.ToString(input.Key) != artifact.Key || !maps.Equal(input.Metadata, artifact.Metadata()) ||
				mutationAttempts(options) != 1 {
				t.Fatalf("CreateMultipartUpload input = %#v attempts=%d", input, mutationAttempts(options))
			}
			return &s3.CreateMultipartUploadOutput{UploadId: aws.String("upload-1")}, nil
		},
		uploadPart: func(input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
			uploadedParts++
			wantLength := int64(singlePutMaximum)
			if uploadedParts == 2 {
				wantLength = 1
			}
			hasher := md5.New()
			written, err := io.Copy(hasher, input.Body)
			if err != nil {
				t.Fatalf("copy UploadPart body: %v", err)
			}
			if aws.ToInt32(input.PartNumber) != int32(uploadedParts) || written != wantLength ||
				aws.ToInt64(input.ContentLength) != wantLength ||
				aws.ToString(input.ContentMD5) != base64.StdEncoding.EncodeToString(hasher.Sum(nil)) {
				t.Fatalf("UploadPart %d = number %d length %d/%d md5 %q", uploadedParts,
					aws.ToInt32(input.PartNumber), written, aws.ToInt64(input.ContentLength), aws.ToString(input.ContentMD5))
			}
			return &s3.UploadPartOutput{ETag: aws.String("part-etag")}, nil
		},
		completeMultipart: func(
			input *s3.CompleteMultipartUploadInput,
			options []func(*s3.Options),
		) (*s3.CompleteMultipartUploadOutput, error) {
			if uploadedParts != 2 || len(input.MultipartUpload.Parts) != 2 ||
				aws.ToString(input.IfNoneMatch) != "*" || aws.ToInt64(input.MpuObjectSize) != int64(size) ||
				mutationAttempts(options) != 1 {
				t.Fatalf("CompleteMultipartUpload input = %#v attempts=%d uploaded=%d",
					input, mutationAttempts(options), uploadedParts)
			}
			return &s3.CompleteMultipartUploadOutput{VersionId: aws.String("null")}, nil
		},
	}
	adapter := mustAdapter(t, fake)
	object, err := adapter.PutExact(context.Background(), artifact, zeroReaderAt{size: size})
	if err != nil {
		t.Fatalf("PutExact() error = %v", err)
	}
	if object.Discriminator != (backupobject.Discriminator{
		Kind: backupobject.DiscriminatorVersionID, Value: "null",
	}) {
		t.Fatalf("discriminator = %#v", object.Discriminator)
	}
}

// Rationale: an ambiguous Complete is resolved only by exact Head evidence;
// this internal reconciliation read cannot infer success from listing.
func TestAmbiguousCompleteReconcilesWithExactHead(t *testing.T) {
	body := []byte("stored artifact")
	artifact := testArtifact(body, backupobject.EncryptionNone)
	completeCalls := 0
	headCalls := 0
	fake := &fakeS3{
		completeMultipart: func(
			input *s3.CompleteMultipartUploadInput,
			options []func(*s3.Options),
		) (*s3.CompleteMultipartUploadOutput, error) {
			completeCalls++
			if aws.ToString(input.IfNoneMatch) != "*" || mutationAttempts(options) != 1 {
				t.Fatalf("Complete input = %#v attempts=%d", input, mutationAttempts(options))
			}
			return nil, timeoutTestError{}
		},
		headObject: func(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			headCalls++
			if input.IfMatch != nil || input.VersionId != nil {
				t.Fatalf("reconciliation Head was incorrectly conditional: %#v", input)
			}
			return &s3.HeadObjectOutput{
				ContentLength: aws.Int64(int64(len(body))), Metadata: artifact.Metadata(),
				VersionId: aws.String("null"),
			}, nil
		},
		listMultipartUploads: func(*s3.ListMultipartUploadsInput) (*s3.ListMultipartUploadsOutput, error) {
			return &s3.ListMultipartUploadsOutput{}, nil
		},
	}
	adapter := mustAdapter(t, fake)
	object, restart, err := adapter.completeMultipart(
		context.Background(), artifact, bytes.NewReader(body), "upload-1", nil,
	)
	if err != nil || restart || completeCalls != 1 || headCalls != 1 {
		t.Fatalf("completeMultipart() = %#v restart=%t err=%v calls=%d/%d",
			object, restart, err, completeCalls, headCalls)
	}
	if object.Discriminator.Kind != backupobject.DiscriminatorVersionID || object.Discriminator.Value != "null" {
		t.Fatalf("discriminator = %#v", object.Discriminator)
	}
}

// Rationale: cleanup authority is limited to one reserved complete key and
// must fully paginate, abort every exact-key upload, and verify none remain.
func TestCleanupExactUploadsFullyPaginatesAndMatchesCompleteKey(t *testing.T) {
	artifact := testArtifact([]byte("stored artifact"), backupobject.EncryptionNone)
	listCalls := 0
	var aborted []string
	fake := &fakeS3{
		listMultipartUploads: func(input *s3.ListMultipartUploadsInput) (*s3.ListMultipartUploadsOutput, error) {
			listCalls++
			if aws.ToString(input.Prefix) != artifact.Key {
				t.Fatalf("list prefix = %q", aws.ToString(input.Prefix))
			}
			switch listCalls {
			case 1:
				return &s3.ListMultipartUploadsOutput{
					Uploads: []types.MultipartUpload{
						{Key: aws.String(artifact.Key), UploadId: aws.String("upload-1")},
						{Key: aws.String(artifact.Key + ".operator"), UploadId: aws.String("do-not-abort")},
					},
					IsTruncated:        aws.Bool(true),
					NextKeyMarker:      aws.String(artifact.Key),
					NextUploadIdMarker: aws.String("marker-1"),
				}, nil
			case 2:
				if aws.ToString(input.KeyMarker) != artifact.Key || aws.ToString(input.UploadIdMarker) != "marker-1" {
					t.Fatalf("pagination markers = %q/%q", aws.ToString(input.KeyMarker), aws.ToString(input.UploadIdMarker))
				}
				return &s3.ListMultipartUploadsOutput{
					Uploads:     []types.MultipartUpload{{Key: aws.String(artifact.Key), UploadId: aws.String("upload-2")}},
					IsTruncated: aws.Bool(false),
				}, nil
			case 3:
				return &s3.ListMultipartUploadsOutput{IsTruncated: aws.Bool(false)}, nil
			default:
				t.Fatalf("unexpected list call %d", listCalls)
				return nil, nil
			}
		},
		abortMultipart: func(input *s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error) {
			if aws.ToString(input.Key) != artifact.Key {
				t.Fatalf("abort key = %q", aws.ToString(input.Key))
			}
			aborted = append(aborted, aws.ToString(input.UploadId))
			return &s3.AbortMultipartUploadOutput{}, nil
		},
	}
	adapter := mustAdapter(t, fake)
	if err := adapter.cleanupExactUploads(context.Background(), artifact.Key); err != nil {
		t.Fatalf("cleanupExactUploads() error = %v", err)
	}
	if strings.Join(aborted, ",") != "upload-1,upload-2" || listCalls != 3 {
		t.Fatalf("aborted = %v, list calls = %d", aborted, listCalls)
	}
}

// Rationale: a present literal VersionId "null" is the authoritative identity
// and must be sent literally on exact reads rather than falling back to ETag.
func TestGetExactPreservesLiteralNullVersion(t *testing.T) {
	body := []byte("stored artifact")
	artifact := testArtifact(body, backupobject.EncryptionNone)
	object := backupobject.Object{
		Artifact: artifact,
		Discriminator: backupobject.Discriminator{
			Kind: backupobject.DiscriminatorVersionID, Value: "null",
		},
	}
	fake := &fakeS3{
		getObject: func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			if aws.ToString(input.VersionId) != "null" || input.IfMatch != nil {
				t.Fatalf("GetObject identity = version %q if-match %v", aws.ToString(input.VersionId), input.IfMatch)
			}
			return &s3.GetObjectOutput{
				Body:          io.NopCloser(bytes.NewReader(body)),
				ContentLength: aws.Int64(int64(len(body))),
				Metadata:      artifact.Metadata(),
				VersionId:     aws.String("null"),
				ETag:          aws.String("ignored"),
			}, nil
		},
	}
	adapter := mustAdapter(t, fake)
	var destination bytes.Buffer
	if err := adapter.GetExact(context.Background(), object, &destination); err != nil {
		t.Fatalf("GetExact() error = %v", err)
	}
	if !bytes.Equal(destination.Bytes(), body) {
		t.Fatalf("downloaded body = %q", destination.Bytes())
	}
}

// Rationale: exact Head is the authoritative reconciliation read: absence is
// typed, while metadata drift is an immutable state conflict.
func TestHeadExactDistinguishesAbsenceAndMismatch(t *testing.T) {
	body := []byte("stored artifact")
	artifact := testArtifact(body, backupobject.EncryptionNone)
	fake := &fakeS3{}
	adapter := mustAdapter(t, fake)
	fake.headObject = func(*s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
		return nil, providerTestError{status: http.StatusNotFound, code: "NoSuchKey", text: "private key path"}
	}
	head, err := adapter.HeadExact(context.Background(), artifact, nil)
	if err != nil || head.Present {
		t.Fatalf("absent HeadExact() = %#v, %v", head, err)
	}
	fake.headObject = func(*s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
		metadata := artifact.Metadata()
		metadata["groundplane-stored-size-bytes"] = "999"
		return &s3.HeadObjectOutput{
			ContentLength: aws.Int64(int64(len(body))), Metadata: metadata, ETag: aws.String("etag"),
		}, nil
	}
	_, err = adapter.HeadExact(context.Background(), artifact, nil)
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStateConflict {
		t.Fatalf("mismatch error kind = %v, %t; error = %v", kind, ok, err)
	}
}

// Rationale: an ambiguous conditional delete may be repeated only after Head
// proves that the exact same immutable object still exists, and success still
// requires authoritative absence.
func TestDeleteExactReconcilesBeforeOneRetry(t *testing.T) {
	body := []byte("stored artifact")
	artifact := testArtifact(body, backupobject.EncryptionNone)
	object := backupobject.Object{
		Artifact: artifact,
		Discriminator: backupobject.Discriminator{
			Kind: backupobject.DiscriminatorETag, Value: "\"etag\"",
		},
	}
	headCalls := 0
	deleteCalls := 0
	fake := &fakeS3{
		headObject: func(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			headCalls++
			if aws.ToString(input.IfMatch) != object.Discriminator.Value {
				t.Fatalf("Head IfMatch = %q", aws.ToString(input.IfMatch))
			}
			if headCalls == 3 {
				return nil, providerTestError{status: http.StatusNotFound, code: "NoSuchKey"}
			}
			return &s3.HeadObjectOutput{
				ContentLength: aws.Int64(int64(len(body))), Metadata: artifact.Metadata(), ETag: aws.String("\"etag\""),
			}, nil
		},
		deleteObject: func(input *s3.DeleteObjectInput, options []func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
			deleteCalls++
			if aws.ToString(input.IfMatch) != object.Discriminator.Value || mutationAttempts(options) != 1 {
				t.Fatalf("Delete input = %#v attempts=%d", input, mutationAttempts(options))
			}
			if deleteCalls == 1 {
				return nil, timeoutTestError{}
			}
			return &s3.DeleteObjectOutput{}, nil
		},
	}
	adapter := mustAdapter(t, fake)
	if err := adapter.DeleteExact(context.Background(), object); err != nil {
		t.Fatalf("DeleteExact() error = %v", err)
	}
	if deleteCalls != 2 || headCalls != 3 {
		t.Fatalf("calls = delete %d head %d", deleteCalls, headCalls)
	}
}

// Rationale: provider diagnostics can contain credentials, request ids, and
// signed paths, so all provider failures must map to fixed safe taxonomy text.
func TestProviderErrorsAreClassifiedAndRedacted(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		kind   errs.Kind
		detail string
	}{
		{"authentication", providerTestError{status: 403, code: "SignatureDoesNotMatch", text: "secret=LEAK"},
			errs.KindValidationFailed, "s3 connector authentication, authorization, or provider contract was rejected"},
		{"collision", providerTestError{status: 412, code: "PreconditionFailed", text: "request-id=LEAK"},
			errs.KindStateConflict, "s3 backup object conflicts with immutable evidence"},
		{"throttle", providerTestError{status: 429, code: "SlowDown", text: "endpoint=LEAK"},
			errs.KindStorageUnavailable, "s3 connector is temporarily unavailable"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			err := providerError(test.err)
			kind, ok := errs.KindOf(err)
			if !ok || kind != test.kind || !strings.Contains(err.Error(), test.detail) || strings.Contains(err.Error(), "LEAK") {
				t.Fatalf("providerError() = %v, kind %v, %t", err, kind, ok)
			}
		})
	}
}

type fakeS3 struct {
	putObject             func(*s3.PutObjectInput, []func(*s3.Options)) (*s3.PutObjectOutput, error)
	createMultipartUpload func(*s3.CreateMultipartUploadInput, []func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	uploadPart            func(*s3.UploadPartInput) (*s3.UploadPartOutput, error)
	completeMultipart     func(*s3.CompleteMultipartUploadInput, []func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	abortMultipart        func(*s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error)
	listMultipartUploads  func(*s3.ListMultipartUploadsInput) (*s3.ListMultipartUploadsOutput, error)
	headObject            func(*s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
	getObject             func(*s3.GetObjectInput) (*s3.GetObjectOutput, error)
	deleteObject          func(*s3.DeleteObjectInput, []func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

func (fake *fakeS3) PutObject(
	_ context.Context, input *s3.PutObjectInput, options ...func(*s3.Options),
) (*s3.PutObjectOutput, error) {
	return fake.putObject(input, options)
}

func (fake *fakeS3) CreateMultipartUpload(
	_ context.Context, input *s3.CreateMultipartUploadInput, options ...func(*s3.Options),
) (*s3.CreateMultipartUploadOutput, error) {
	return fake.createMultipartUpload(input, options)
}

func (fake *fakeS3) UploadPart(
	_ context.Context, input *s3.UploadPartInput, _ ...func(*s3.Options),
) (*s3.UploadPartOutput, error) {
	return fake.uploadPart(input)
}

func (fake *fakeS3) CompleteMultipartUpload(
	_ context.Context, input *s3.CompleteMultipartUploadInput, options ...func(*s3.Options),
) (*s3.CompleteMultipartUploadOutput, error) {
	return fake.completeMultipart(input, options)
}

func (fake *fakeS3) AbortMultipartUpload(
	_ context.Context, input *s3.AbortMultipartUploadInput, _ ...func(*s3.Options),
) (*s3.AbortMultipartUploadOutput, error) {
	return fake.abortMultipart(input)
}

func (fake *fakeS3) ListMultipartUploads(
	_ context.Context, input *s3.ListMultipartUploadsInput, _ ...func(*s3.Options),
) (*s3.ListMultipartUploadsOutput, error) {
	return fake.listMultipartUploads(input)
}

func (fake *fakeS3) HeadObject(
	_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options),
) (*s3.HeadObjectOutput, error) {
	return fake.headObject(input)
}

func (fake *fakeS3) GetObject(
	_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	return fake.getObject(input)
}

func (fake *fakeS3) DeleteObject(
	_ context.Context, input *s3.DeleteObjectInput, options ...func(*s3.Options),
) (*s3.DeleteObjectOutput, error) {
	return fake.deleteObject(input, options)
}

func testConfig() Config {
	return Config{
		Endpoint: "https://objects.example.test", Bucket: "groundplane-backups", Prefix: "groundplane/",
		Region: "auto", PathStyle: true, AccessKey: "access", SecretKey: "secret",
	}
}

func testArtifact(body []byte, encryption backupobject.Encryption) backupobject.Artifact {
	digest := sha256.Sum256(body)
	artifact := backupobject.Artifact{
		EnvironmentID:   ids.NewAt(ids.KindEnvironment, testInstant, 1),
		SourceID:        ids.NewAt(ids.KindBackupSource, testInstant, 2),
		RecoveryPointID: ids.NewAt(ids.KindRecoveryPoint, testInstant, 3),
		SourceFormat:    backupobject.SourceFormatPostgresCustom,
		Encryption:      encryption,
		Evidence: backupobject.Evidence{
			SourceSizeBytes: uint64(len(body)), SourceSHA256: digest,
			StoredSizeBytes: uint64(len(body)), StoredSHA256: digest,
		},
	}
	if encryption == backupobject.EncryptionAge {
		era := uint64(1)
		artifact.KeyEra = &era
	}
	artifact.Key = artifact.ExpectedKey(testConfig().Prefix)
	return artifact
}

func mustAdapter(t *testing.T, fake *fakeS3) *Adapter {
	t.Helper()
	adapter, err := newWithClient(testConfig(), fake)
	if err != nil {
		t.Fatalf("newWithClient() error = %v", err)
	}
	return adapter
}

func mutationAttempts(options []func(*s3.Options)) int {
	configured := &s3.Options{}
	for _, option := range options {
		option(configured)
	}
	if configured.Retryer == nil {
		return 0
	}
	return configured.Retryer.MaxAttempts()
}

type providerTestError struct {
	status int
	code   string
	text   string
}

func (err providerTestError) Error() string       { return err.text }
func (err providerTestError) HTTPStatusCode() int { return err.status }
func (err providerTestError) ErrorCode() string   { return err.code }

type timeoutTestError struct{}

func (timeoutTestError) Error() string   { return "signed endpoint credential leak" }
func (timeoutTestError) Timeout() bool   { return true }
func (timeoutTestError) Temporary() bool { return true }

var testInstant = time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)

type zeroReaderAt struct {
	size uint64
}

func (reader zeroReaderAt) ReadAt(destination []byte, offset int64) (int, error) {
	if offset < 0 || uint64(offset) >= reader.size {
		return 0, io.EOF
	}
	length := min(uint64(len(destination)), reader.size-uint64(offset))
	clear(destination[:length])
	if length != uint64(len(destination)) {
		return int(length), io.EOF
	}
	return int(length), nil
}

func hashZeroBytes(t *testing.T, size uint64) [sha256.Size]byte {
	t.Helper()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, io.NewSectionReader(zeroReaderAt{size: size}, 0, int64(size))); err != nil {
		t.Fatalf("hash zero bytes: %v", err)
	}
	var digest [sha256.Size]byte
	copy(digest[:], hasher.Sum(nil))
	return digest
}
