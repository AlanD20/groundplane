package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	testidempotencyowner "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type componentEnableRouteFixture struct {
	calls int
	input apiTypes.ComponentEnableRequest
}

func (fixture *componentEnableRouteFixture) EnableComponent(
	_ context.Context,
	_ string,
	input apiTypes.ComponentEnableRequest,
	_ string,
) (testidempotencyowner.IdempotencyResponse, error) {
	fixture.calls++
	fixture.input = input
	return testidempotencyowner.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        []byte(`{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`),
	}, nil
}

func (*componentEnableRouteFixture) DisableComponent(
	context.Context, string, string,

) (testidempotencyowner.IdempotencyResponse, error) {
	return testidempotencyowner.IdempotencyResponse{}, errs.New(errs.KindInternal, "unexpected disable")
}

func (*componentEnableRouteFixture) UpdateComponent(
	context.Context,
	string,
	string,
) (testidempotencyowner.IdempotencyResponse, error) {
	return testidempotencyowner.IdempotencyResponse{}, errs.New(errs.KindInternal, "unexpected update")
}

func (*componentEnableRouteFixture) SetComponentConfig(
	context.Context, string,

	apiTypes.ComponentConfigMutationRequest, string,

) (testidempotencyowner.IdempotencyResponse, error) {
	return testidempotencyowner.IdempotencyResponse{}, errs.New(errs.KindInternal, "unexpected configure")
}

// Rationale: the public enable endpoint must preserve ordered Zone placement
// and optional body semantics through real request parsing and dispatch.
func TestComponentEnableRouteAcceptsOptionalTypedPlacement(t *testing.T) {
	zones := []string{"net_01ARZ3NDEKTSV4RRFFQ69G5FAX", "net_01ARZ3NDEKTSV4RRFFQ69G5FAW"}
	for _, body := range []string{"", `{}`, `{"config":{"zone_ids":["` + zones[0] + `","` + zones[1] + `"]}}`,
		`{"config":{"zone_ids":["` + zones[0] + `","` + zones[1] + `"],"credential":{"mode":"existing","secret_id":"sec_01ARZ3NDEKTSV4RRFFQ69G5FAV"}}}`} {
		fixture := &componentEnableRouteFixture{}
		server := New(nil, nil, Options{ComponentMutations: fixture})
		request := httptest.NewRequest(
			http.MethodPost,
			"/api/v1/components/cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV/enable",
			strings.NewReader(body),
		)
		request.Header.Set("Idempotency-Key", "component-enable-12345")
		if body != "" {
			request.Header.Set("Content-Type", "application/json")
		}
		response := httptest.NewRecorder()
		server.HTTPHandler().ServeHTTP(response, request)
		if response.Code != http.StatusAccepted || fixture.calls != 1 {
			t.Fatalf("enable body %q: %d %s calls=%d", body, response.Code, response.Body.String(), fixture.calls)
		}
		if body == "" || body == "{}" {
			if fixture.input.Config != nil {
				t.Fatal("bodyless enable invented config")
			}
			continue
		}
		var got []string
		if fixture.input.Config.Caddy != nil {
			got = fixture.input.Config.Caddy.ZoneIDs
		} else {
			got = fixture.input.Config.CloudflareTunnel.ZoneIDs
		}
		if !reflect.DeepEqual(got, zones) {
			t.Fatalf("zone order = %v", got)
		}
	}
}

// Rationale: malformed or legacy placement cannot silently become an enable
// using old config, and mutation-key enforcement remains mandatory.
func TestComponentEnableRouteRejectsInvalidPlacement(t *testing.T) {
	for _, body := range []string{
		`null`, `{"config":null}`, `{"unknown":true}`, `{"config":{"zone_id":"net_01ARZ3NDEKTSV4RRFFQ69G5FAX"}}`,
		`{"config":{},"config":{}}`,
		`{"config":{"zone_ids":[]}}`, `{"config":{"zone_ids":["net_01ARZ3NDEKTSV4RRFFQ69G5FAX","net_01ARZ3NDEKTSV4RRFFQ69G5FAX"]}}`,
	} {
		fixture := &componentEnableRouteFixture{}
		server := New(nil, nil, Options{ComponentMutations: fixture})
		request := httptest.NewRequest(
			http.MethodPost,
			"/api/v1/components/cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV/enable",
			strings.NewReader(body),
		)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", "component-enable-12345")
		response := httptest.NewRecorder()
		server.HTTPHandler().ServeHTTP(response, request)
		if response.Code < 400 || fixture.calls != 0 {
			t.Fatalf("invalid enable %q: %d calls=%d", body, response.Code, fixture.calls)
		}
	}
	fixture := &componentEnableRouteFixture{}
	server := New(nil, nil, Options{ComponentMutations: fixture})
	response := httptest.NewRecorder()
	server.HTTPHandler().
		ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/v1/components/cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV/enable", nil))
	if response.Code < 400 || fixture.calls != 0 {
		t.Fatal("enable accepted missing mutation key")
	}
}
