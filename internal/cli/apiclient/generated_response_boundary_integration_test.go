package apiclient_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
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
	if body.readBytes != 0 {
		t.Fatalf("body reads = %d, want 0", body.readBytes)
	}
	assertBodyLifecycle(t, body)
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
	if body.readBytes != problemresponse.MaximumBytes+1 {
		t.Fatalf(
			"body reads = %d, want %d",
			body.readBytes,
			problemresponse.MaximumBytes+1,
		)
	}
	assertBodyLifecycle(t, body)
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
	if body.readBytes != problemresponse.MaximumBytes+1 {
		t.Fatalf("chunked body reads = %d, want %d", body.readBytes, problemresponse.MaximumBytes+1)
	}
	assertBodyLifecycle(t, body)
}

// Rationale: an actual HTTP/1.1 chunked response must still be bounded after
// net/http decodes its framing, and the generated parser must close that body.
func TestGeneratedClientBoundsActualChunkedProblem(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/problem+json")
		writer.WriteHeader(http.StatusServiceUnavailable)
		flusher, ok := writer.(http.Flusher)
		if !ok {
			t.Error("httptest ResponseWriter does not support flushing")
			return
		}
		flusher.Flush()
		_, _ = writer.Write(bytes.Repeat([]byte("x"), int(problemresponse.MaximumBytes)+4096))
	}))
	defer server.Close()

	var body *observedResponseBody
	var contentLength int64
	var transferEncoding []string
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		response, err := http.DefaultTransport.RoundTrip(request)
		if err != nil {
			return nil, err
		}
		body = &observedResponseBody{Reader: response.Body}
		response.Body = body
		contentLength = response.ContentLength
		transferEncoding = append([]string(nil), response.TransferEncoding...)
		return response, nil
	})
	client, err := generated.NewClientWithResponses(
		server.URL,
		generated.WithHTTPClient(&http.Client{Transport: transport}),
	)
	if err != nil {
		t.Fatalf("NewClientWithResponses() error = %v", err)
	}
	_, err = client.HostShowWithResponse(context.Background())
	assertOverflow(t, err)
	if body == nil {
		t.Fatal("transport did not return a response body")
	}
	if contentLength >= 0 || len(transferEncoding) != 1 || transferEncoding[0] != "chunked" {
		t.Fatalf("response framing = content-length %d, transfer-encoding %v; want chunked", contentLength, transferEncoding)
	}
	if body.readBytes != problemresponse.MaximumBytes+1 {
		t.Fatalf("actual chunked body reads = %d, want %d", body.readBytes, problemresponse.MaximumBytes+1)
	}
	assertBodyLifecycle(t, body)
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
	if body.readBytes != problemresponse.MaximumBytes {
		t.Fatalf("body reads = %d, want %d", body.readBytes, problemresponse.MaximumBytes)
	}
	assertBodyLifecycle(t, body)
}

// Rationale: generated error parsing must validate the closed Problem envelope
// before its permissive generated JSON unmarshal can accept unknown members.
func TestGeneratedClientStrictlyDecodesNormalProblem(t *testing.T) {
	valid := []byte(
		`{"type":"about:blank","title":"storage.unavailable","status":503,` +
			`"detail":"storage offline","code":"storage.unavailable"}`,
	)
	validBody := &observedResponseBody{Reader: bytes.NewReader(valid)}
	response, err := callGeneratedHost(
		t,
		generatedResponseDoer(func(request *http.Request) (*http.Response, error) {
			return problemResponse(request, validBody, int64(len(valid))), nil
		}),
	)
	if err != nil || response.ApplicationproblemJSONDefault == nil ||
		response.ApplicationproblemJSONDefault.Detail != "storage offline" {
		t.Fatalf("normal generated Problem = %#v, %v", response, err)
	}
	assertBodyLifecycle(t, validBody)

	invalid := bytes.Replace(valid, []byte("}"), []byte(`,"future":"value"}`), 1)
	invalidBody := &observedResponseBody{Reader: bytes.NewReader(invalid)}
	_, err = callGeneratedHost(
		t,
		generatedResponseDoer(func(request *http.Request) (*http.Response, error) {
			return problemResponse(request, invalidBody, int64(len(invalid))), nil
		}),
	)
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("unknown Problem member error = %v", err)
	}
	assertBodyLifecycle(t, invalidBody)
}

// Rationale: malformed media types and mismatched status/code identities are
// untrusted branches, but each must retain the generated parser's one close.
func TestGeneratedClientClosesRejectedProblemBranches(t *testing.T) {
	valid := []byte(
		`{"type":"about:blank","title":"storage.unavailable","status":503,` +
			`"detail":"storage offline","code":"storage.unavailable"}`,
	)
	tests := []struct {
		name        string
		payload     []byte
		contentType string
		status      int
	}{
		{
			name:        "malformed JSON",
			payload:     []byte(`{"`),
			contentType: "application/problem+json",
			status:      http.StatusServiceUnavailable,
		},
		{
			name:        "wrong content type",
			payload:     valid,
			contentType: "application/json",
			status:      http.StatusServiceUnavailable,
		},
		{
			name:        "status mismatch",
			payload:     valid,
			contentType: "application/problem+json",
			status:      http.StatusConflict,
		},
		{
			name: "code mismatch",
			payload: bytes.Replace(
				valid,
				[]byte(`"code":"storage.unavailable"`),
				[]byte(`"code":"validation.failed"`),
				1,
			),
			contentType: "application/problem+json",
			status:      http.StatusServiceUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := &observedResponseBody{Reader: bytes.NewReader(test.payload)}
			_, err := callGeneratedHost(
				t,
				generatedResponseDoer(func(request *http.Request) (*http.Response, error) {
					response := problemResponse(request, body, int64(len(test.payload)))
					response.StatusCode = test.status
					response.Header.Set("Content-Type", test.contentType)
					return response, nil
				}),
			)
			if !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("generated parser error = %v, want %q", err, errs.CodeInternal)
			}
			assertBodyLifecycle(t, body)
		})
	}
}

// Rationale: a transport read failure is wrapped without leaking the body;
// closure remains the parser's responsibility even when no bytes are read.
func TestGeneratedClientClosesUnderlyingReadError(t *testing.T) {
	readErr := errors.New("private transport read failure")
	body := &observedResponseBody{Reader: failingResponseReader{err: readErr}}
	_, err := callGeneratedHost(
		t,
		generatedResponseDoer(func(request *http.Request) (*http.Response, error) {
			return problemResponse(request, body, -1), nil
		}),
	)
	if !errors.Is(err, readErr) || !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("generated parser error = %v, want wrapped read and internal errors", err)
	}
	assertBodyLifecycle(t, body)
}

// Rationale: successful responses remain unrestricted by the Problem ceiling
// while still closing exactly once after generated JSON parsing completes.
func TestGeneratedClientPreservesSuccessResponseLifecycle(t *testing.T) {
	payload := []byte(`{"padding":"` + strings.Repeat("x", int(problemresponse.MaximumBytes)+1) + `"}`)
	body := &observedResponseBody{Reader: bytes.NewReader(payload)}
	response, err := callGeneratedHost(
		t,
		generatedResponseDoer(func(request *http.Request) (*http.Response, error) {
			result := problemResponse(request, body, int64(len(payload)))
			result.StatusCode = http.StatusOK
			result.Header.Set("Content-Type", "application/json")
			return result, nil
		}),
	)
	if err != nil || response.JSON200 == nil || !bytes.Equal(response.Body, payload) {
		t.Fatalf("successful generated response = %#v, %v", response, err)
	}
	if body.readBytes != int64(len(payload)) {
		t.Fatalf("success body reads = %d, want %d", body.readBytes, len(payload))
	}
	assertBodyLifecycle(t, body)
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
	assertBodyLifecycle(t, body)
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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (roundTrip roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

type observedResponseBody struct {
	io.Reader
	readBytes       int64
	readsAfterClose int
	closeCalls      int
	closed          bool
	closeErr        error
}

func (body *observedResponseBody) Read(buffer []byte) (int, error) {
	if body.closed {
		body.readsAfterClose++
	}
	read, err := body.Reader.Read(buffer)
	body.readBytes += int64(read)
	return read, err
}

func (body *observedResponseBody) Close() error {
	body.closeCalls++
	body.closed = true
	return body.closeErr
}

func assertBodyLifecycle(t *testing.T, body *observedResponseBody) {
	t.Helper()
	if body.closeCalls != 1 || body.readsAfterClose != 0 {
		t.Fatalf(
			"body closes = %d, reads after close = %d, want 1 and 0",
			body.closeCalls,
			body.readsAfterClose,
		)
	}
}

type failingResponseReader struct {
	err error
}

func (reader failingResponseReader) Read([]byte) (int, error) {
	return 0, reader.err
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
