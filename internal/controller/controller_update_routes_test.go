package controller

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: the update surface accepts only a staged immutable release and
// exact idempotency key; arbitrary paths, extra fields and queries cannot enter
// the host coordinator through Huma's request decoding.
func TestControllerUpdateRouteHasClosedInputAndDurableResponse(t *testing.T) {
	release := "sha256:" + strings.Repeat("a", 64)
	for _, scenario := range []string{"valid", "missing-key", "tag", "path", "duplicate", "query"} {
		t.Run(scenario, func(t *testing.T) {
			updates := &controllerUpdateRoutePublisher{}
			server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{ControllerUpdates: updates})
			body, path := `{"release":"`+release+`"}`, "/api/v1/controller/update"
			switch scenario {
			case "tag":
				body = `{"release":"latest"}`
			case "path":
				body = `{"release":"` + release + `","path":"/other/controller"}`
			case "duplicate":
				body = `{"release":"` + release + `","release":"` + release + `"}`
			case "query":
				path += "?path=/other"
			}
			request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			if scenario != "missing-key" {
				request.Header.Set("Idempotency-Key", "native-route-key-0001")
			}
			response := httptest.NewRecorder()
			server.HTTPHandler().ServeHTTP(response, request)
			if scenario == "valid" {
				if response.Code != http.StatusAccepted || updates.release != release ||
					updates.key != "native-route-key-0001" ||
					response.Body.String() != `{"task_id":"task_native"}` {
					t.Fatalf("update response = %d %s", response.Code, response.Body.String())
				}
			} else if response.Code < 400 || updates.calls != 0 {
				t.Fatalf("invalid input accepted: %d calls %d", response.Code, updates.calls)
			}
		})
	}
}

type controllerUpdateRoutePublisher struct {
	release, key string
	calls        int
}

func (publisher *controllerUpdateRoutePublisher) UpdateController(
	_ context.Context,
	release, key string,
) (etcd.IdempotencyResponse, error) {
	publisher.calls++
	publisher.release, publisher.key = release, key
	return etcd.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        []byte(`{"task_id":"task_native"}`),
	}, nil
}
