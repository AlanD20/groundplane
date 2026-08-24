package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
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
		reader.request != (etcd.PageRequest{Limit: 3, Cursor: "next"}) {
		t.Fatalf("scope/request = %q, %#v", reader.environmentID, reader.request)
	}
}

type volumeRouteTestReader struct {
	environmentID string
	request       etcd.PageRequest
}

func (reader *volumeRouteTestReader) ListVolumes(
	_ context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.VolumeRecord], error) {
	reader.environmentID = environmentID
	reader.request = request
	return etcd.Page[etcd.VolumeRecord]{
		Items: []etcd.Versioned[etcd.VolumeRecord]{{Record: etcd.VolumeRecord{
			ID: "vol_01AAAAAAAAAAAAAAAAAAAAAAAA", EnvironmentID: environmentID, Name: "uploads",
		}}},
		NextCursor: "after",
	}, nil
}
