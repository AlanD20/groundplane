package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
)

type adversarialProblemOutput struct {
	Body struct{}
}

type adversarialRequiredQuery struct {
	Value string `query:"value" required:"true"`
}

// Rationale: a domain error returned through the real Huma adapter must retain
// its canonical tuple while a wrapped storage cause remains private.
func TestHumaResponseDoesNotLeakWrappedCause(t *testing.T) {
	const secret = "etcd-token=private"
	mux, api := adversarialProblemAPI()
	huma.Register(api, huma.Operation{
		OperationID: "adversarial-wrapped-storage",
		Method:      http.MethodGet,
		Path:        "/wrapped-storage",
	}, func(context.Context, *struct{}) (*adversarialProblemOutput, error) {
		return nil, errs.Wrap(errs.KindStorageUnavailable, errors.New(secret))
	})

	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/wrapped-storage", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), secret) {
		t.Fatalf("response leaked wrapped cause: %s", response.Body.String())
	}
	var problem errs.Problem
	if err := json.Unmarshal(response.Body.Bytes(), &problem); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if problem.Code != errs.CodeStorageUnavailable || problem.Status != http.StatusServiceUnavailable {
		t.Fatalf("problem tuple = %#v", problem)
	}
}

// Rationale: opaque 500 diagnostics passed directly to New are as private as
// wrapped causes and must be generic after real Huma serialization.
func TestHumaResponseDoesNotLeakNewInternalMessage(t *testing.T) {
	const secret = "controller-signing-key=private"
	mux, api := adversarialProblemAPI()
	huma.Register(api, huma.Operation{
		OperationID: "adversarial-new-internal",
		Method:      http.MethodGet,
		Path:        "/new-internal",
	}, func(context.Context, *struct{}) (*adversarialProblemOutput, error) {
		return nil, errs.New(errs.KindRequestFailed, secret, errs.WithTitle(secret))
	})

	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/new-internal", nil))
	if response.Code != http.StatusInternalServerError || strings.Contains(response.Body.String(), secret) {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var problem errs.Problem
	if err := json.Unmarshal(response.Body.Bytes(), &problem); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if problem.Title != "Internal Server Error" || problem.Detail != "Internal Server Error" {
		t.Fatalf("problem = %#v", problem)
	}
}

// Rationale: framework-created request errors must serialize the HTTP status
// phrase as title instead of deriving presentation from a public error code.
func TestHumaFrameworkErrorUsesHTTPStatusTitle(t *testing.T) {
	mux, api := adversarialProblemAPI()
	huma.Register(api, huma.Operation{
		OperationID: "adversarial-required-query",
		Method:      http.MethodGet,
		Path:        "/required-query",
	}, func(context.Context, *adversarialRequiredQuery) (*adversarialProblemOutput, error) {
		return &adversarialProblemOutput{}, nil
	})

	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/required-query", nil))
	var problem errs.Problem
	if err := json.Unmarshal(response.Body.Bytes(), &problem); err != nil {
		t.Fatalf("decode response: %v; body = %s", err, response.Body.String())
	}
	if problem.Title != http.StatusText(response.Code) {
		t.Fatalf("title = %q, want %q; problem = %#v", problem.Title, http.StatusText(response.Code), problem)
	}
}

// Rationale: Huma detail errors can contain parser internals or request data;
// requestProblem may classify them but must never append their raw text.
func TestRequestProblemDoesNotLeakRawDetailErrors(t *testing.T) {
	const secret = "authorization=private"
	domainError := requestProblem(http.StatusUnprocessableEntity, "request validation failed", []error{
		errors.New(secret),
	})
	problem := domainError.ToProblem()
	if problem.Title != http.StatusText(http.StatusUnprocessableEntity) {
		t.Fatalf("title = %q", problem.Title)
	}
	if strings.Contains(problem.Detail, secret) {
		t.Fatalf("problem detail leaked framework error: %q", problem.Detail)
	}
}

func adversarialProblemAPI() (*http.ServeMux, huma.API) {
	configureProblemResponses()
	mux := http.NewServeMux()
	api := humago.NewWithPrefix(mux, "/api/v1", huma.DefaultConfig("adversarial", "1.0.0"))
	return mux, api
}
