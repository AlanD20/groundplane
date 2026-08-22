package controller

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

const (
	testAttachRouteID        = "att_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testAttachRouteServiceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testAttachRouteBackingID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
)

type fakeAttachMutator struct {
	response  etcd.IdempotencyResponse
	request   apiTypes.AttachRequest
	attachID  string
	key       string
	createHit bool
	detachHit bool
}

func (mutator *fakeAttachMutator) CreateAttach(
	_ context.Context,
	request apiTypes.AttachRequest,
	key string,
) (etcd.IdempotencyResponse, error) {
	mutator.createHit = true
	mutator.request = request
	mutator.key = key
	return mutator.response, nil
}

func (mutator *fakeAttachMutator) DetachAttach(
	_ context.Context,
	attachID string,
	key string,
) (etcd.IdempotencyResponse, error) {
	mutator.detachHit = true
	mutator.attachID = attachID
	mutator.key = key
	return mutator.response, nil
}

// Rationale: Attach creation must preserve the application service's exact replayable response and
// pass the complete decoded contract plus idempotency key without route-layer rewriting.
func TestAttachCreateRoutePreservesMutationContract(t *testing.T) {
	t.Parallel()
	want := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: []byte(`{"task_id":"task_01"}`),
	}
	mutator := &fakeAttachMutator{response: want}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{AttachMutations: mutator})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/attaches", bytes.NewBufferString(
		`{"service_ids":["`+testAttachRouteServiceID+`"],"backing_service_id":"`+
			testAttachRouteBackingID+`","name":"api-db","grant_attach_ids":[]}`,
	))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(idempotencyKeyHeader, "attach-create-key-0001")
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
	if response.Code != want.Status || response.Header().Get("Content-Type") != want.ContentKind ||
		!bytes.Equal(response.Body.Bytes(), want.Body) {
		t.Fatalf(
			"POST /attaches = %d %q %s",
			response.Code,
			response.Header().Get("Content-Type"),
			response.Body.Bytes(),
		)
	}
	if !mutator.createHit || mutator.key != "attach-create-key-0001" || mutator.request.Name != "api-db" ||
		len(mutator.request.ServiceIDs) != 1 || mutator.request.ServiceIDs[0] != testAttachRouteServiceID ||
		mutator.request.BackingServiceID != testAttachRouteBackingID {
		t.Fatalf("CreateAttach() input/key = %#v/%q", mutator.request, mutator.key)
	}
}

// Rationale: detach is a bodyless idempotent task mutation addressed by stable Attach id and must
// return the exact accepted Task response from the application service.
func TestAttachDetachRouteUsesStableTargetAndIdempotencyKey(t *testing.T) {
	t.Parallel()
	want := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: []byte(`{"task_id":"task_02"}`),
	}
	mutator := &fakeAttachMutator{response: want}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{AttachMutations: mutator})
	request := httptest.NewRequest(http.MethodDelete, "/api/v1/attaches/"+testAttachRouteID, nil)
	request.Header.Set(idempotencyKeyHeader, "attach-detach-key-0001")
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
	if response.Code != want.Status || !bytes.Equal(response.Body.Bytes(), want.Body) || !mutator.detachHit ||
		mutator.attachID != testAttachRouteID || mutator.key != "attach-detach-key-0001" {
		t.Fatalf("DELETE /attaches/{id} = %d %s / %#v", response.Code, response.Body.Bytes(), mutator)
	}
}

// Rationale: duplicate or unknown JSON members make idempotency intent ambiguous and must be rejected
// before the Attach application service is invoked.
func TestAttachCreateRouteRejectsAmbiguousJSON(t *testing.T) {
	t.Parallel()
	for _, body := range []string{
		`{"service_ids":[],"service_ids":[],"backing_service_id":"` + testAttachRouteBackingID + `"}`,
		`{"service_ids":[],"backing_service_id":"` + testAttachRouteBackingID + `","extra":true}`,
	} {
		mutator := &fakeAttachMutator{}
		server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{AttachMutations: mutator})
		request := httptest.NewRequest(http.MethodPost, "/api/v1/attaches", bytes.NewBufferString(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(idempotencyKeyHeader, "attach-create-key-0002")
		response := httptest.NewRecorder()
		server.Mux.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest || mutator.createHit {
			t.Fatalf("POST /attaches ambiguous body = %d, called=%t", response.Code, mutator.createHit)
		}
	}
}
