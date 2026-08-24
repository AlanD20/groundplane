package s3compatible

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	credentialMaxBytes = 256 * 1024
	endpointMaxBytes   = 2048
	regionMaxBytes     = 64
	maxBackoff         = 20 * time.Second
)

// Config is the complete explicit Connector execution input. New never reads
// environment variables, shared AWS files, proxy variables, or custom roots.
type Config struct {
	Endpoint  string
	Bucket    string
	Prefix    string
	Region    string
	PathStyle bool
	AccessKey string
	SecretKey string
}

type staticCredentials struct {
	accessKey string
	secretKey string
}

func (provider staticCredentials) Retrieve(context.Context) (aws.Credentials, error) {
	return aws.Credentials{
		AccessKeyID:     provider.accessKey,
		SecretAccessKey: provider.secretKey,
		Source:          "groundplane-s3-compatible",
	}, nil
}

func validateConfig(config Config) error {
	parsed, err := url.Parse(config.Endpoint)
	if !utf8.ValidString(config.Endpoint) || len(config.Endpoint) > endpointMaxBytes || err != nil || parsed == nil ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		parsed.Opaque != "" || parsed.Path != "" || parsed.RawPath != "" {
		return errs.New(errs.KindValidationFailed, "s3 connector endpoint is invalid")
	}
	if !validBucket(config.Bucket) {
		return errs.New(errs.KindValidationFailed, "s3 connector bucket is invalid")
	}
	if config.Region == "" || len(config.Region) > regionMaxBytes || !ascii(config.Region) {
		return errs.New(errs.KindValidationFailed, "s3 connector region is invalid")
	}
	if !validPrefix(config.Prefix) {
		return errs.New(errs.KindValidationFailed, "s3 connector prefix is invalid")
	}
	if len(config.AccessKey) == 0 || len(config.AccessKey) > credentialMaxBytes ||
		len(config.SecretKey) == 0 || len(config.SecretKey) > credentialMaxBytes {
		return errs.New(errs.KindValidationFailed, "s3 connector credentials are invalid")
	}
	return nil
}

func validBucket(bucket string) bool {
	if len(bucket) < 3 || len(bucket) > 63 || ipv4Shape(bucket) ||
		hasAnyPrefix(bucket, "xn--", "sthree-", "amzn-s3-demo-") ||
		hasAnySuffix(bucket, "-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3") {
		return false
	}
	for _, label := range strings.Split(bucket, ".") {
		if label == "" || !lowerAlphaNumeric(label[0]) || !lowerAlphaNumeric(label[len(label)-1]) {
			return false
		}
		for index := range len(label) {
			character := label[index]
			if !lowerAlphaNumeric(character) && character != '-' {
				return false
			}
		}
	}
	return true
}

func hasAnyPrefix(value string, prefixes ...string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	return false
}

func hasAnySuffix(value string, suffixes ...string) bool {
	for _, suffix := range suffixes {
		if strings.HasSuffix(value, suffix) {
			return true
		}
	}
	return false
}

func ipv4Shape(value string) bool {
	parts := strings.Split(value, ".")
	if len(parts) != 4 {
		return false
	}
	for _, part := range parts {
		if len(part) == 0 || len(part) > 3 {
			return false
		}
		for index := range len(part) {
			if part[index] < '0' || part[index] > '9' {
				return false
			}
		}
	}
	return true
}

func lowerAlphaNumeric(character byte) bool {
	return character >= 'a' && character <= 'z' || character >= '0' && character <= '9'
}

func ascii(value string) bool {
	for index := range len(value) {
		if value[index] > 0x7f {
			return false
		}
	}
	return true
}

func validPrefix(prefix string) bool {
	if prefix == "" {
		return true
	}
	if !utf8.ValidString(prefix) || len(prefix) > 1024 || strings.ContainsRune(prefix, '\x00') ||
		strings.ContainsRune(prefix, '\\') || strings.HasPrefix(prefix, "/") || !strings.HasSuffix(prefix, "/") {
		return false
	}
	for _, component := range strings.Split(strings.TrimSuffix(prefix, "/"), "/") {
		if component == "" || component == "." || component == ".." {
			return false
		}
	}
	return true
}

func newAWSConfig(config Config) aws.Config {
	return aws.Config{
		Region:      config.Region,
		Credentials: aws.NewCredentialsCache(staticCredentials{config.AccessKey, config.SecretKey}),
		HTTPClient:  newHTTPClient(),
		Retryer: func() aws.Retryer {
			return retry.AddWithMaxBackoffDelay(retry.NewStandard(func(options *retry.StandardOptions) {
				options.MaxAttempts = 3
			}), maxBackoff)
		},
		BaseEndpoint:               aws.String(config.Endpoint),
		ClientLogMode:              aws.ClientLogMode(0),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	}
}

func newHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return rejectedError()
		},
	}
}

func New(config Config) (*Adapter, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(newAWSConfig(config), func(options *s3.Options) {
		options.BaseEndpoint = aws.String(config.Endpoint)
		options.UsePathStyle = config.PathStyle
	})
	return &Adapter{config: config, client: client}, nil
}

func newWithClient(config Config, client s3API) (*Adapter, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	if client == nil {
		return nil, errs.New(errs.KindValidationFailed, "s3 connector client is required")
	}
	return &Adapter{config: config, client: client}, nil
}

type providerFailure uint8

const (
	providerFailureInternal providerFailure = iota
	providerFailureAbsent
	providerFailureConflict
	providerFailureRejected
	providerFailureUnavailable
)

type responseStatus interface {
	HTTPStatusCode() int
}

type responseCode interface {
	ErrorCode() string
}

func classifyProviderFailure(err error) providerFailure {
	if err == nil {
		return providerFailureInternal
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return providerFailureInternal
	}
	if kind, ok := errs.KindOf(err); ok {
		switch kind {
		case errs.KindValidationFailed:
			return providerFailureRejected
		case errs.KindStateConflict:
			return providerFailureConflict
		case errs.KindStorageUnavailable:
			return providerFailureUnavailable
		default:
			return providerFailureInternal
		}
	}

	status := 0
	var statusValue responseStatus
	if errors.As(err, &statusValue) {
		status = statusValue.HTTPStatusCode()
	}
	code := ""
	var codeValue responseCode
	if errors.As(err, &codeValue) {
		code = codeValue.ErrorCode()
	}
	switch code {
	case "NoSuchKey", "NoSuchUpload", "NotFound":
		return providerFailureAbsent
	case "ConditionalRequestConflict", "OperationAborted", "PreconditionFailed":
		return providerFailureConflict
	case "AccessDenied", "ExpiredToken", "ExpiredTokenException", "InvalidAccessKeyId", "InvalidToken",
		"SignatureDoesNotMatch", "TokenRefreshRequired", "UnrecognizedClientException":
		return providerFailureRejected
	case "InternalError", "RequestTimeout", "RequestTimeoutException", "ServiceUnavailable", "Throttling",
		"ThrottlingException", "ThrottledException", "RequestThrottledException", "TooManyRequestsException",
		"ProvisionedThroughputExceededException", "TransactionInProgressException", "RequestLimitExceeded",
		"BandwidthLimitExceeded", "LimitExceededException", "RequestThrottled", "SlowDown",
		"PriorRequestNotComplete", "EC2ThrottledException":
		return providerFailureUnavailable
	}
	switch {
	case status == http.StatusNotFound:
		return providerFailureAbsent
	case status == http.StatusConflict || status == http.StatusPreconditionFailed:
		return providerFailureConflict
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return providerFailureRejected
	case status == http.StatusRequestTimeout || status == http.StatusTooEarly ||
		status == http.StatusTooManyRequests || status >= 500:
		return providerFailureUnavailable
	case status >= 300:
		return providerFailureRejected
	}

	if code != "" {
		return providerFailureRejected
	}

	var networkError net.Error
	if errors.As(err, &networkError) {
		return providerFailureUnavailable
	}
	return providerFailureInternal
}

func providerError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return failureError(classifyProviderFailure(err))
}

func failureError(failure providerFailure) error {
	switch failure {
	case providerFailureConflict, providerFailureAbsent:
		return conflictError()
	case providerFailureRejected:
		return rejectedError()
	case providerFailureUnavailable:
		return unavailableError()
	default:
		return internalError()
	}
}

func rejectedError() error {
	return errs.New(
		errs.KindValidationFailed,
		"s3 connector authentication, authorization, or provider contract was rejected",
	)
}

func conflictError() error {
	return errs.New(errs.KindStateConflict, "s3 backup object conflicts with immutable evidence")
}

func unavailableError() error {
	return errs.New(errs.KindStorageUnavailable, "s3 connector is temporarily unavailable")
}

func internalError() error {
	return errs.New(errs.KindInternal, "s3 connector operation failed")
}
