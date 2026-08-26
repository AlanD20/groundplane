package apiclient_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/problemresponse"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a declared oversized non-success response must be rejected
// without reading attacker-controlled bytes, while the generated parser still
// closes the response exactly once.
func TestGeneratedClientRejectsDeclaredOversizedProblemBeforeRead(t *testing.T) {
	body := &observedResponseBody{Reader: bytes.NewReader(bytes.Repeat([]byte("x"), 32))}
	_, err := callGeneratedHost(
		t,
		generatedResponseDoer(func(request *http.Request) (*http.Response, error) {
			return problemResponse(request, body, problemresponse.MaximumBytes+1), nil
		}),
	)
	assertOverflow(t, err)
	if body.readBytes != 0 || body.closeCalls != 1 {
		t.Fatalf("body reads = %d, closes = %d, want 0 and 1", body.readBytes, body.closeCalls)
	}
}

// Rationale: an unknown-length non-success response must consume at most one
// byte beyond the accepted boundary before returning the one internal Error.
func TestGeneratedClientBoundsUnknownLengthProblem(t *testing.T) {
	body := &observedResponseBody{
		Reader: bytes.NewReader(bytes.Repeat([]byte("x"), int(problemresponse.MaximumBytes)+4096)),
	}
	_, err := callGeneratedHost(
		t,
		generatedResponseDoer(func(request *http.Request) (*http.Response, error) {
			return problemResponse(request, body, -1), nil
		}),
	)
	assertOverflow(t, err)
	if body.readBytes != problemresponse.MaximumBytes+1 || body.closeCalls != 1 {
		t.Fatalf(
			"body reads = %d, closes = %d, want %d and 1",
			body.readBytes,
			body.closeCalls,
			problemresponse.MaximumBytes+1,
		)
	}
}

// Rationale: a real chunked HTTP response has no trusted length declaration;
// the generated client path must still enforce the same exact body ceiling.
func TestGeneratedClientBoundsChunkedProblem(t *testing.T) {
	body := &observedResponseBody{
		Reader: &repeatedChunkReader{remaining: problemresponse.MaximumBytes + 4096},
	}
	_, err := callGeneratedHost(
		t,
		generatedResponseDoer(func(request *http.Request) (*http.Response, error) {
			response := problemResponse(request, body, -1)
			response.TransferEncoding = []string{"chunked"}
			return response, nil
		}),
	)
	assertOverflow(t, err)
	if body.readBytes != problemresponse.MaximumBytes+1 || body.closeCalls != 1 {
		t.Fatalf("chunked body reads = %d, closes = %d", body.readBytes, body.closeCalls)
	}
}

// Rationale: the size limit is inclusive; an exact-bound valid Problem must
// reach the generated response model without changing its fields or body.
func TestGeneratedClientAcceptsExactBoundaryProblem(t *testing.T) {
	payload := exactProblemBody(t, int(problemresponse.MaximumBytes))
	body := &observedResponseBody{Reader: bytes.NewReader(payload)}
	response, err := callGeneratedHost(
		t,
		generatedResponseDoer(func(request *http.Request) (*http.Response, error) {
			return problemResponse(request, body, int64(len(payload))), nil
		}),
	)
	if err != nil {
		t.Fatalf("HostShowWithResponse() error = %v", err)
	}
	if len(response.Body) != len(payload) || response.ApplicationproblemJSONDefault == nil {
		t.Fatalf("generated response = %#v, body bytes = %d", response, len(response.Body))
	}
	if got := response.ApplicationproblemJSONDefault; got.Code != string(errs.CodeStorageUnavailable) ||
		got.Status != http.StatusServiceUnavailable || got.Type != errs.ProblemType {
		t.Fatalf("generated Problem = %#v", got)
	}
	if body.readBytes != problemresponse.MaximumBytes || body.closeCalls != 1 {
		t.Fatalf("body reads = %d, closes = %d", body.readBytes, body.closeCalls)
	}
}

// Rationale: generated error parsing must validate the closed Problem envelope
// before its permissive generated JSON unmarshal can accept unknown members.
func TestGeneratedClientStrictlyDecodesNormalProblem(t *testing.T) {
	valid := []byte(
		`{"type":"about:blank","title":"storage.unavailable","status":503,` +
			`"detail":"storage offline","code":"storage.unavailable"}`,
	)
	response, err := callGeneratedHost(
		t,
		generatedResponseDoer(func(request *http.Request) (*http.Response, error) {
			return problemResponse(request, io.NopCloser(bytes.NewReader(valid)), int64(len(valid))), nil
		}),
	)
	if err != nil || response.ApplicationproblemJSONDefault == nil ||
		response.ApplicationproblemJSONDefault.Detail != "storage offline" {
		t.Fatalf("normal generated Problem = %#v, %v", response, err)
	}

	invalid := bytes.Replace(valid, []byte("}"), []byte(`,"future":"value"}`), 1)
	_, err = callGeneratedHost(
		t,
		generatedResponseDoer(func(request *http.Request) (*http.Response, error) {
			return problemResponse(request, io.NopCloser(bytes.NewReader(invalid)), int64(len(invalid))), nil
		}),
	)
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("unknown Problem member error = %v", err)
	}
}

// Rationale: response-body ownership remains with the generated parser; a
// Close failure is ignored exactly as before and closure still occurs once.
func TestGeneratedClientPreservesCloseErrorSemantics(t *testing.T) {
	payload := []byte(
		`{"type":"about:blank","title":"storage.unavailable","status":503,` +
			`"detail":"storage offline","code":"storage.unavailable"}`,
	)
	body := &observedResponseBody{Reader: bytes.NewReader(payload), closeErr: errors.New("private close failure")}
	response, err := callGeneratedHost(
		t,
		generatedResponseDoer(func(request *http.Request) (*http.Response, error) {
			return problemResponse(request, body, int64(len(payload))), nil
		}),
	)
	if err != nil || response.ApplicationproblemJSONDefault == nil {
		t.Fatalf("HostShowWithResponse() = %#v, %v", response, err)
	}
	if body.closeCalls != 1 {
		t.Fatalf("Close() calls = %d, want 1", body.closeCalls)
	}
}

func callGeneratedHost(
	t *testing.T,
	doer generatedResponseDoer,
) (*generated.HostShowResponse, error) {
	t.Helper()
	client, err := generated.NewClientWithResponses(
		"http://groundplane.test",
		generated.WithHTTPClient(doer),
	)
	if err != nil {
		t.Fatalf("NewClientWithResponses() error = %v", err)
	}
	return client.HostShowWithResponse(context.Background())
}

func problemResponse(
	request *http.Request,
	body io.ReadCloser,
	contentLength int64,
) *http.Response {
	return &http.Response{
		StatusCode:    http.StatusServiceUnavailable,
		Header:        http.Header{"Content-Type": []string{"application/problem+json"}},
		Body:          body,
		ContentLength: contentLength,
		Request:       request,
	}
}

func exactProblemBody(t *testing.T, size int) []byte {
	t.Helper()
	prefix := `{"type":"about:blank","title":"storage.unavailable","status":503,"detail":"`
	suffix := `","code":"storage.unavailable"}`
	if size < len(prefix)+len(suffix) {
		t.Fatalf("Problem size %d is below fixed envelope size", size)
	}
	return []byte(prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix)
}

func assertOverflow(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("overflow error = %v, want %q", err, errs.CodeInternal)
	}
	var domainError *errs.Error
	if !errors.As(err, &domainError) {
		t.Fatalf("overflow error type = %T, want *errs.Error", err)
	}
	problem := domainError.ToProblem()
	if problem.Title != "Internal Server Error" || problem.Detail != "Internal Server Error" ||
		strings.Contains(problem.Detail, "response limit") {
		t.Fatalf("overflow public Problem leaked diagnostics: %#v", problem)
	}
}

type generatedResponseDoer func(*http.Request) (*http.Response, error)

func (doer generatedResponseDoer) Do(request *http.Request) (*http.Response, error) {
	return doer(request)
}

type observedResponseBody struct {
	io.Reader
	readBytes  int64
	closeCalls int
	closeErr   error
}

func (body *observedResponseBody) Read(buffer []byte) (int, error) {
	read, err := body.Reader.Read(buffer)
	body.readBytes += int64(read)
	return read, err
}

func (body *observedResponseBody) Close() error {
	body.closeCalls++
	return body.closeErr
}

type repeatedChunkReader struct {
	remaining int64
}

func (reader *repeatedChunkReader) Read(buffer []byte) (int, error) {
	if reader.remaining == 0 {
		return 0, io.EOF
	}
	read := min(int64(len(buffer)), min(reader.remaining, 4096))
	for index := range int(read) {
		buffer[index] = 'x'
	}
	reader.remaining -= read
	return int(read), nil
}
