package apiclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// QA: BP-03, UI-04. A proxy/server failure does not prove that Apply was not
// accepted. The CLI must retain replay instructions for those HTTP outcomes.
func TestBlueprintApplyPreservesUncertainHTTPOutcomes(t *testing.T) {
	for _, status := range []int{400, 409, 422, 408, 429, 500, 502, 503, 504} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.Method != http.MethodPut || request.Header.Get("Idempotency-Key") != "original-apply-key" {
					t.Errorf("wrong protected request: %s %q", request.Method, request.Header.Get("Idempotency-Key"))
				}
				kinds := map[int]errs.Kind{
					400: errs.KindMalformedRequest, 409: errs.KindStateConflict,
					422: errs.KindValidationFailed, 500: errs.KindInternal, 503: errs.KindStorageUnavailable,
				}
				if kind, known := kinds[status]; known {
					writer.Header().Set("Content-Type", "application/problem+json")
					writer.WriteHeader(status)
					if err := json.NewEncoder(writer).Encode(errs.New(kind, "request failed").ToProblem()); err != nil {
						t.Errorf("write problem: %v", err)
					}
					return
				}
				writer.WriteHeader(status)
			}))
			defer server.Close()
			_, uncertain, err := New(server.URL).ApplyEnvironmentBlueprint(
				context.Background(),
				"env_original", "0", "original-apply-key", nil, "multipart/form-data; boundary=fixture",
			)
			wantUncertain := status == 408 || status == 429 || status >= 500
			if err == nil || uncertain != wantUncertain {
				t.Fatalf("status %d: uncertain=%v, error=%v", status, uncertain, err)
			}
		})
	}
}
