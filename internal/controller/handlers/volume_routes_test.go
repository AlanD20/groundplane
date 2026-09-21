package handlers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testidempotencyowner "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: the prerequisite API must preserve stable Volume ids, labels,
// pagination, and Environment scope.
func TestVolumeListRouteProjectsEnvironmentScopedPage(t *testing.T) {
	reader := &volumeRouteTestReader{}
	server := New(nil, nil, Options{Volumes: reader})
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/volumes?environment=env_01AAAAAAAAAAAAAAAAAAAAAAAA&limit=3&cursor=next",
		nil,
	)
	response := httptest.NewRecorder()
	server.HTTPHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status/body = %d, %q", response.Code, response.Body.String())
	}
	var page apiTypes.Page[apiTypes.Volume]
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != "vol_01AAAAAAAAAAAAAAAAAAAAAAAA" || page.NextCursor != "after" {
		t.Fatalf("body = %q", response.Body.String())
	}
	if reader.environmentID != "env_01AAAAAAAAAAAAAAAAAAAAAAAA" ||
		reader.request != (testkeyvalue.PageRequest{Limit: 3, Cursor: "next"}) {
		t.Fatalf("scope/request = %q, %#v", reader.environmentID, reader.request)
	}
}

func TestVolumeCreateRouteUsesTypedMutationEnvelope(t *testing.T) {
	mutator := &volumeRouteTestMutator{}
	server := New(nil, nil, Options{Volumes: &volumeRouteTestReader{}, VolumeMutations: mutator})
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/volumes",
		io.NopCloser(
			strings.NewReader(
				`{"environment_id":"env_01AAAAAAAAAAAAAAAAAAAAAAAA","slug":"uploads","key":"uploads-data"}`,
			),
		),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "volume-create-test-0001")
	response := httptest.NewRecorder()
	server.HTTPHandler().ServeHTTP(response, request)
	if response.Code != http.StatusCreated || response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf(
			"status/content/body = %d/%q/%q",
			response.Code,
			response.Header().Get("Content-Type"),
			response.Body.String(),
		)
	}
	if response.Body.String() != `{"volume":{"id":"vol_01AAAAAAAAAAAAAAAAAAAAAAAA","environment_id":"env_01AAAAAAAAAAAAAAAAAAAAAAAA","slug":"uploads","key":"uploads-data"},"task_id":"tsk_create"}` {
		t.Fatalf("body = %q", response.Body.String())
	}
	if mutator.input.Slug != "uploads" || mutator.idempotencyKey != "volume-create-test-0001" {
		t.Fatalf("mutation = %#v, idempotency = %q", mutator.input, mutator.idempotencyKey)
	}
}

type volumeRouteTestReader struct {
	environmentID string
	request       testkeyvalue.PageRequest
}

type volumeRouteTestMutator struct {
	input          apiTypes.VolumeCreate
	idempotencyKey string
}

func (mutator *volumeRouteTestMutator) CreateVolume(
	_ context.Context,
	input apiTypes.VolumeCreate,
	idempotencyKey string,
) (testidempotencyowner.IdempotencyResponse, error) {
	mutator.input = input
	mutator.idempotencyKey = idempotencyKey
	return testidempotencyowner.IdempotencyResponse{
		Status: http.StatusCreated, ContentKind: "application/json",
		Body: []byte(
			`{"volume":{"id":"vol_01AAAAAAAAAAAAAAAAAAAAAAAA","environment_id":"env_01AAAAAAAAAAAAAAAAAAAAAAAA","slug":"uploads","key":"uploads-data"},"task_id":"tsk_create"}`,
		),
	}, nil
}

func (mutator *volumeRouteTestMutator) EditVolume(
	context.Context, string,

	apiTypes.VolumeEdit, string,

) (testidempotencyowner.IdempotencyResponse, error) {
	return testidempotencyowner.IdempotencyResponse{}, nil
}

func (mutator *volumeRouteTestMutator) RemoveVolume(
	context.Context, string, string, string, string,

) (testidempotencyowner.IdempotencyResponse, error) {
	return testidempotencyowner.IdempotencyResponse{}, nil
}

func (reader *volumeRouteTestReader) ListVolumes(
	_ context.Context,
	environmentID string,
	request testkeyvalue.PageRequest,
) (testkeyvalue.Page[etcd.VolumeRecord], error) {
	reader.environmentID = environmentID
	reader.request = request
	return testkeyvalue.Page[etcd.VolumeRecord]{
		Items: []testkeyvalue.Versioned[etcd.VolumeRecord]{{Record: etcd.VolumeRecord{
			ID: "vol_01AAAAAAAAAAAAAAAAAAAAAAAA", EnvironmentID: environmentID,
			Slug: "uploads", Key: "uploads-data",
		}}},
		NextCursor: "after",
	}, nil
}
