package apiclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: CLI retry and exit behavior is derived only after an exact public
// problem tuple reconstructs one closed internal Kind.
// QA: UI-03/05; local error trust boundary, not Controller admission.
func TestResponseProblemReconstructsExactKind(t *testing.T) {
	tests := []struct {
		name   string
		status int
		kind   errs.Kind
	}{
		{name: "malformed", status: 400, kind: errs.KindMalformedRequest},
		{name: "semantic", status: 422, kind: errs.KindValidationFailed},
		{name: "in progress", status: 409, kind: errs.KindIdempotencyInProgress},
		{name: "storage unavailable", status: 503, kind: errs.KindStorageUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := errs.New(test.kind, "detail").ToProblem()
			err := responseProblem(http.MethodPost, "/api/v1/resources", problemHTTPResponse(test.status, source))
			if !errors.Is(err, errs.New(test.kind, "")) {
				t.Fatalf("response error = %v", err)
			}
			var domainError *errs.Error
			if !errors.As(err, &domainError) || domainError.ToProblem() != source {
				t.Fatalf("response problem = %#v, want %#v", domainError, source)
			}
		})
	}
}

// Rationale: a response status that disagrees with its problem body must remain
// untrusted and cannot be reconstructed as a domain error.
// QA: UI-03/05; local error trust boundary, not Controller admission.
func TestResponseProblemRejectsUntrustedTupleMismatch(t *testing.T) {
	tests := []struct {
		name           string
		responseStatus int
		problem        errs.Problem
	}{
		{
			name:           "body status differs from response",
			responseStatus: 400,
			problem:        errs.New(errs.KindValidationFailed, "invalid").ToProblem(),
		},
		{
			name:           "known code with impossible status",
			responseStatus: 409,
			problem: errs.Problem{
				Type: errs.ProblemType, Status: 409, Code: errs.CodeValidationFailed, Detail: "invalid",
			},
		},
		{
			name:           "unknown code",
			responseStatus: 500,
			problem: errs.Problem{
				Type: errs.ProblemType, Status: 500, Code: errs.Code("future.error"), Detail: "invalid",
			},
		},
		{
			name:           "non canonical problem type",
			responseStatus: 500,
			problem: errs.Problem{
				Type: "https://example.invalid/problem", Status: 500, Code: errs.CodeInternal, Detail: "invalid",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := responseProblem(
				http.MethodGet,
				"/api/v1/resources",
				problemHTTPResponse(test.responseStatus, test.problem),
			)
			if !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("response error = %v", err)
			}
		})
	}
}

func problemHTTPResponse(status int, problem errs.Problem) *http.Response {
	body, err := json.Marshal(problem)
	if err != nil {
		panic(err)
	}
	return &http.Response{
		StatusCode: status,
		Header: http.Header{
			"Content-Type": []string{"application/problem+json"},
		},
		Body: io.NopCloser(bytes.NewReader(body)),
	}
}
