package controller

import (
	"errors"
	"net/http"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: framework statuses must select one closed Kind, including the
// deliberate 400/422 and request-failure distinctions.
// QA: UI-03; local error classification, not all HTTP routes.
func TestRequestProblemUsesClosedKinds(t *testing.T) {
	tests := []struct {
		status     int
		wantKind   errs.Kind
		wantCode   errs.Code
		wantStatus int
	}{
		{http.StatusBadRequest, errs.KindMalformedRequest, errs.CodeValidationFailed, 400},
		{http.StatusUnprocessableEntity, errs.KindValidationFailed, errs.CodeValidationFailed, 422},
		{http.StatusNotFound, errs.KindRequestNotFound, errs.CodeRequestNotFound, 404},
		{http.StatusMethodNotAllowed, errs.KindRequestMethodNotAllowed, errs.CodeRequestMethodNotAllowed, 405},
		{http.StatusNotAcceptable, errs.KindRequestNotAcceptable, errs.CodeRequestNotAcceptable, 406},
		{http.StatusRequestEntityTooLarge, errs.KindRequestTooLarge, errs.CodeRequestFailed, 413},
		{
			http.StatusUnsupportedMediaType,
			errs.KindRequestUnsupportedMediaType,
			errs.CodeRequestUnsupportedMediaType,
			415,
		},
		{http.StatusServiceUnavailable, errs.KindRequestUnavailable, errs.CodeRequestFailed, 503},
		{http.StatusNotImplemented, errs.KindNotImplemented, errs.CodeNotImplemented, 501},
		{http.StatusTeapot, errs.KindRequestFailed, errs.CodeRequestFailed, 500},
	}

	for _, test := range tests {
		domainError := requestProblem(test.status, "request failed", nil)
		problem := domainError.ToProblem()
		if domainError.Kind() != test.wantKind || problem.Code != test.wantCode ||
			domainError.HTTPStatus() != test.wantStatus {
			t.Errorf("requestProblem(%d) = kind %d, code %q, status %d",
				test.status, domainError.Kind(), problem.Code, domainError.HTTPStatus())
		}
	}
}

// Rationale: malformed syntax and semantic validation share a public code but
// must remain different Kinds so transport behavior cannot collapse them.
// QA: UI-03; local 400/422 kind distinction only.
func TestFrameworkValidationKindsRemainDistinct(t *testing.T) {
	malformed := requestProblem(http.StatusBadRequest, "malformed", nil)
	semantic := requestProblem(http.StatusUnprocessableEntity, "invalid", nil)
	if errors.Is(malformed, semantic) {
		t.Fatal("framework 400 and 422 must not share internal Kind")
	}
	if malformed.ToProblem().Type != "about:blank" || semantic.ToProblem().Type != "about:blank" {
		t.Fatal("framework errors must use about:blank")
	}
}

// Rationale: Huma may report body overflow as a nominal bad-request detail;
// the boundary must promote it to the descriptor-owned 413 Kind.
// QA: UI-03; local overflow classification, not an actual Huma upload.
func TestHumaMaxBytesErrorSelectsRequestTooLargeKind(t *testing.T) {
	domainError := requestProblem(
		http.StatusBadRequest,
		"decode failed",
		[]error{&http.MaxBytesError{Limit: 32}},
	)
	problem := domainError.ToProblem()
	if domainError.Kind() != errs.KindRequestTooLarge || problem.Code != errs.CodeRequestFailed ||
		domainError.HTTPStatus() != http.StatusRequestEntityTooLarge {
		t.Fatalf("problem = %#v", domainError)
	}
}
