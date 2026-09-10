package controller

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: schema and HTTP decoding must accept exact integer orders and a
// zero reset while rejecting coercions before the mutation service is called.
func TestScriptOrderRoutesEnforceNumericBounds(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodPatch} {
		for _, value := range []string{"0", "10", "65535", "-1", "65536", "1.5", "null", `"10"`} {
			t.Run(method+"/"+value, func(t *testing.T) {
				mutations := &scriptOrderRouteMutations{}
				server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{ScriptMutations: mutations})
				path, body := "/api/v1/scripts", `{"environment_id":"env_01ARZ3NDEKTSV4RRFFQ69G5FAV",`+
					`"service_id":"svc_01ARZ3NDEKTSV4RRFFQ69G5FAV","slug":"migrate","script":"echo migrate",`+
					`"when":"pre-deploy","order":`+value+`}`
				status := http.StatusCreated
				if method == http.MethodPatch {
					path += "/scr_01ARZ3NDEKTSV4RRFFQ69G5FAV"
					body, status = `{"order":`+value+`}`, http.StatusOK
				}
				request := httptest.NewRequest(method, path, strings.NewReader(body))
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Idempotency-Key", "script-order-route-0001")
				response := httptest.NewRecorder()
				server.HTTPHandler().ServeHTTP(response, request)
				valid := value == "0" || value == "10" || value == "65535"
				if !valid {
					if response.Code < 400 || mutations.calls != 0 {
						t.Fatalf("invalid order accepted: %d calls=%d", response.Code, mutations.calls)
					}
					return
				}
				want, err := strconv.ParseUint(value, 10, 16)
				if err != nil {
					t.Fatal(err)
				}
				var got apiTypes.Script
				if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
					t.Fatal(err)
				}
				if response.Code != status || mutations.calls != 1 || uint64(got.Order) != want {
					t.Fatalf("order route = %d, calls=%d, order=%d", response.Code, mutations.calls, got.Order)
				}
			})
		}
	}
}

type scriptOrderRouteMutations struct{ calls int }

func (mutations *scriptOrderRouteMutations) CreateScript(
	_ context.Context, input apiTypes.ScriptCreate, _ string,
) (etcd.IdempotencyResponse, error) {
	mutations.calls++
	return scriptOrderRouteResponse(input.Order, http.StatusCreated)
}

func (mutations *scriptOrderRouteMutations) EditScript(
	_ context.Context, _ string, input apiTypes.ScriptEdit, _ string,
) (etcd.IdempotencyResponse, error) {
	mutations.calls++
	if input.Order == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "missing order")
	}
	return scriptOrderRouteResponse(*input.Order, http.StatusOK)
}

func (*scriptOrderRouteMutations) RemoveScript(context.Context, string, string) (etcd.IdempotencyResponse, error) {
	return etcd.IdempotencyResponse{}, errs.New(errs.KindNotImplemented, "not part of order authoring")
}

func (*scriptOrderRouteMutations) RunScript(context.Context, string, string) (etcd.IdempotencyResponse, error) {
	return etcd.IdempotencyResponse{}, errs.New(errs.KindNotImplemented, "not part of order authoring")
}

func scriptOrderRouteResponse(order uint16, status int) (etcd.IdempotencyResponse, error) {
	body, err := json.Marshal(apiTypes.Script{Order: order, Execution: apiTypes.ScriptExecution{Mode: "inherited"}})
	return etcd.IdempotencyResponse{Status: status, ContentKind: "application/json", Body: body}, err
}
