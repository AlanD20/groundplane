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

// Rationale: an API error is trusted only inside a bounded RFC problem
// envelope containing exactly one JSON document and the closed tuple.
// QA: UI-05; local malformed error rejection only.
func TestResponseProblemRejectsInvalidEnvelope(t *testing.T) {
	problem := errs.New(errs.KindStorageUnavailable, "storage offline").ToProblem()
	valid, err := json.Marshal(problem)
	if err != nil {
		t.Fatalf("marshal problem: %v", err)
	}

	tests := map[string]struct {
		mediaType string
		body      []byte
	}{
		"wrong media type": {
			mediaType: "application/json",
			body:      valid,
		},
		"trailing JSON document": {
			mediaType: "application/problem+json",
			body:      append(append([]byte(nil), valid...), []byte(` {}`)...),
		},
		"trailing garbage": {
			mediaType: "application/problem+json",
			body:      append(append([]byte(nil), valid...), []byte(` garbage`)...),
		},
		"oversized body": {
			mediaType: "application/problem+json",
			body:      bytes.Repeat([]byte("x"), (1<<20)+1),
		},
		"missing member": {
			mediaType: "application/problem+json",
			body:      bytes.Replace(valid, []byte(`,"code":"storage.unavailable"`), nil, 1),
		},
		"null member": {
			mediaType: "application/problem+json",
			body:      bytes.Replace(valid, []byte(`"detail":"storage offline"`), []byte(`"detail":null`), 1),
		},
		"invalid member type": {
			mediaType: "application/problem+json",
			body:      bytes.Replace(valid, []byte(`"status":503`), []byte(`"status":"503"`), 1),
		},
		"unknown member": {
			mediaType: "application/problem+json",
			body:      bytes.Replace(valid, []byte(`}`), []byte(`,"details":{}}`), 1),
		},
		"duplicate member": {
			mediaType: "application/problem+json",
			body:      bytes.Replace(valid, []byte(`"code":`), []byte(`"code":"future.error","code":`), 1),
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			response := &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Header: http.Header{
					"Content-Type": []string{test.mediaType},
				},
				Body: io.NopCloser(bytes.NewReader(test.body)),
			}
			got := responseProblem(http.MethodGet, "/api/v1/resources", response)
			if !errors.Is(got, errs.New(errs.KindInternal, "")) ||
				errors.Is(got, errs.New(errs.KindStorageUnavailable, "")) {
				t.Fatalf("invalid envelope produced trusted problem: %v", got)
			}
		})
	}
}
