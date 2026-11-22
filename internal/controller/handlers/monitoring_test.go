package handlers

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	monitoringcapability "github.com/AlanD20/groundplane/internal/controller/monitoring"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type monitoringReaderFunc func(
	context.Context,
	monitoringcapability.Query,
) (apiTypes.MonitoringResponse, error)

func (reader monitoringReaderFunc) Show(
	ctx context.Context,
	query monitoringcapability.Query,
) (apiTypes.MonitoringResponse, error) {
	return reader(ctx, query)
}

func TestMonitoringRoutePreservesValidatedQueryAndResponse(t *testing.T) {
	t.Parallel()

	wantQuery := monitoringcapability.Query{
		Kind: "backing", ID: "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV", Range: "6h", Points: 120, Limit: 50,
		Cursor: "next-page", ConsumerCursor: "next-consumer",
	}
	want := apiTypes.MonitoringResponse{
		Scope:         apiTypes.MonitoringScope{Kind: "backing", ID: wantQuery.ID, Name: "postgres"},
		Window:        apiTypes.MonitoringWindow{Range: "6h", BucketSeconds: 180},
		HistoryStatus: "unavailable",
		Backing: &apiTypes.BackingMonitoring{
			Runtime: apiTypes.ServiceMonitoring{ContainersStatus: "available"},
		},
	}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		Monitoring: monitoringReaderFunc(
			func(_ context.Context, got monitoringcapability.Query) (apiTypes.MonitoringResponse, error) {
				if !reflect.DeepEqual(got, wantQuery) {
					t.Fatalf("Show() query = %#v, want %#v", got, wantQuery)
				}
				return want, nil
			},
		),
	})
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, httptest.NewRequest(
		http.MethodGet,
		"/api/v1/monitoring/backing/"+wantQuery.ID+"?range=6h&points=120&limit=50&cursor=next-page&consumer_cursor=next-consumer",
		nil,
	))
	if response.Code != http.StatusOK {
		t.Fatalf("GET monitoring status = %d, want %d; body=%s", response.Code, http.StatusOK, response.Body.String())
	}
	var got apiTypes.MonitoringResponse
	if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
		t.Fatalf("decode monitoring response: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GET monitoring response = %#v, want %#v", got, want)
	}
}

func TestMonitoringRouteRejectsUnboundedOrAmbiguousQueriesBeforeReader(t *testing.T) {
	t.Parallel()

	called := false
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		Monitoring: monitoringReaderFunc(
			func(context.Context, monitoringcapability.Query) (apiTypes.MonitoringResponse, error) {
				called = true
				return apiTypes.MonitoringResponse{}, nil
			},
		),
	})
	tests := []struct {
		path   string
		status int
	}{
		{path: "/api/v1/monitoring/unknown/id", status: http.StatusUnprocessableEntity},
		{path: "/api/v1/monitoring/host/local?points=15", status: http.StatusUnprocessableEntity},
		{path: "/api/v1/monitoring/host/local?limit=1&limit=2", status: http.StatusBadRequest},
		{path: "/api/v1/monitoring/host/local?unexpected=true", status: http.StatusBadRequest},
	}
	for _, test := range tests {
		response := httptest.NewRecorder()
		server.Mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, test.path, nil))
		if response.Code != test.status {
			t.Fatalf(
				"GET %s status = %d, want %d; body=%s",
				test.path,
				response.Code,
				test.status,
				response.Body.String(),
			)
		}
	}
	if called {
		t.Fatal("monitoring reader called for rejected query")
	}
}

func TestMonitoringRouteProjectsStorageUnavailable(t *testing.T) {
	t.Parallel()

	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		Monitoring: monitoringReaderFunc(
			func(context.Context, monitoringcapability.Query) (apiTypes.MonitoringResponse, error) {
				return apiTypes.MonitoringResponse{}, errs.New(errs.KindStorageUnavailable, "history unavailable")
			},
		),
	})
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(
		response,
		httptest.NewRequest(http.MethodGet, "/api/v1/monitoring/host/local", nil),
	)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf(
			"GET monitoring status = %d, want %d; body=%s",
			response.Code,
			http.StatusServiceUnavailable,
			response.Body.String(),
		)
	}
	var problem errs.Problem
	if err := json.NewDecoder(response.Body).Decode(&problem); err != nil {
		t.Fatalf("decode monitoring problem: %v", err)
	}
	if problem.Code != errs.CodeStorageUnavailable {
		t.Fatalf("problem code = %q, want %q", problem.Code, errs.CodeStorageUnavailable)
	}
}
