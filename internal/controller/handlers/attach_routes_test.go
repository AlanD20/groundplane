package handlers

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

const (
	testAttachRouteID        = "att_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testAttachRouteServiceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testAttachRouteBackingID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
)

type fakeAttachMutator struct {
	response  testidempotency.IdempotencyResponse
	request   apiTypes.AttachRequest
	attachID  string
	key       string
	createHit bool
	detachHit bool
}

type fakeAttachFactReader struct {
	attachID string
	grantID  string
	key      string
}

func (reader *fakeAttachFactReader) RevealAttachFact(
	_ context.Context,
	attachID string,
	grantID string,
	key string,
) (string, error) {
	reader.attachID, reader.grantID, reader.key = attachID, grantID, key
	return "postgres://ready", nil
}

func (mutator *fakeAttachMutator) CreateAttach(
	_ context.Context,
	request apiTypes.AttachRequest,
	key string,
) (testidempotency.IdempotencyResponse, error) {
	mutator.createHit = true
	mutator.request = request
	mutator.key = key
	return mutator.response, nil
}

func (mutator *fakeAttachMutator) DetachAttach(
	_ context.Context,
	attachID string,
	key string,
) (testidempotency.IdempotencyResponse, error) {
	mutator.detachHit = true
	mutator.attachID = attachID
	mutator.key = key
	return mutator.response, nil
}

// Rationale: Attach creation must preserve the application service's exact replayable response and
// pass the complete decoded contract plus idempotency key without route-layer rewriting.
func TestAttachCreateRoutePreservesMutationContract(t *testing.T) {
	t.Parallel()
	want := testidempotency.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: []byte(`{"task_id":"task_01"}`),
	}
	mutator := &fakeAttachMutator{response: want}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{AttachMutations: mutator})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/attaches", bytes.NewBufferString(
		`{"service_id":"`+testAttachRouteServiceID+`","backing_service_id":"`+
			testAttachRouteBackingID+`","name":"api-db","credential":{"mode":"new"},"grant_attach_ids":[]}`,
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
		mutator.request.ServiceID != testAttachRouteServiceID ||
		mutator.request.Credential.Mode != apiTypes.AttachCredentialNew ||
		mutator.request.BackingServiceID != testAttachRouteBackingID {
		t.Fatalf("CreateAttach() input/key = %#v/%q", mutator.request, mutator.key)
	}
}

// Rationale: detach is a bodyless idempotent task mutation addressed by stable Attach id and must
// return the exact accepted Task response from the application service.
func TestAttachDetachRouteUsesStableTargetAndIdempotencyKey(t *testing.T) {
	t.Parallel()
	want := testidempotency.IdempotencyResponse{
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

// Rationale: fact plaintext must cross only the explicit read route and must retain the stable
// owning Attach, optional grant Attach, and declared fact key selected by the operator.
func TestAttachFactRevealPreservesExactReference(t *testing.T) {
	t.Parallel()
	reader := &fakeAttachFactReader{}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{AttachFacts: reader})
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/attaches/"+testAttachRouteID+"/facts/pg16_URL?grant_attach_id=att_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		nil,
	)
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK ||
		response.Body.String() != "{\"$schema\":\"https://example.com/api/v1/AttachFactValue.json\",\"value\":\"postgres://ready\"}\n" ||
		reader.attachID != testAttachRouteID || reader.grantID != "att_01ARZ3NDEKTSV4RRFFQ69G5FAW" ||
		reader.key != "pg16_URL" {
		t.Fatalf("GET Attach fact = %d %s / %#v", response.Code, response.Body.Bytes(), reader)
	}
}

// Rationale: duplicate or unknown JSON members make idempotency intent ambiguous and must be rejected
// before the Attach application service is invoked.
func TestAttachCreateRouteRejectsAmbiguousJSON(t *testing.T) {
	t.Parallel()
	for _, body := range []string{
		`{"service_id":"` + testAttachRouteServiceID + `","service_id":"` + testAttachRouteServiceID + `","backing_service_id":"` + testAttachRouteBackingID + `","credential":{"mode":"new"}}`,
		`{"service_id":"` + testAttachRouteServiceID + `","backing_service_id":"` + testAttachRouteBackingID + `","credential":{"mode":"new"},"extra":true}`,
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
