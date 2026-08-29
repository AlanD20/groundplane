package apiclient

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func TestVolumeMutationsUseADR0049Shapes(t *testing.T) {
	t.Parallel()

	server := newVolumeTestServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if request.Header.Get("Idempotency-Key") == "" {
			t.Error("mutation request has no Idempotency-Key")
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/api/v1/volumes":
			var input apiTypes.VolumeCreate
			if err := json.Unmarshal(body, &input); err != nil {
				t.Errorf("decode create: %v", err)
			}
			if input.EnvironmentID != "env_1" || input.Slug != "uploads" || input.Key != "uploads-data" {
				t.Errorf("create input = %#v", input)
			}
			writer.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(writer, `{"volume":{"id":"vol_1","environment_id":"env_1","slug":"uploads","key":"uploads-data"},"task_id":"tsk_create"}`)
		case request.Method == http.MethodPatch && request.URL.Path == "/api/v1/volumes/vol_1":
			var input apiTypes.VolumeEdit
			if err := json.Unmarshal(body, &input); err != nil {
				t.Errorf("decode edit: %v", err)
			}
			if input.Slug != "archive" {
				t.Errorf("edit input = %#v", input)
			}
			writer.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(writer, `{"volume":{"id":"vol_1","environment_id":"env_1","slug":"archive","key":"uploads-data"},"task_id":"tsk_edit"}`)
		case request.Method == http.MethodDelete && request.URL.Path == "/api/v1/volumes/vol_1":
			query := request.URL.Query()
			if query.Get("impact_token") != "impact" || query.Get("confirm_key") != "uploads-data" {
				t.Errorf("delete query = %q", request.URL.RawQuery)
			}
			writer.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(writer, `{"task_id":"tsk_remove"}`)
		default:
			t.Errorf("unexpected request %s %s", request.Method, request.URL.String())
			http.Error(writer, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := New(server.URL)
	created, err := client.CreateVolume(context.Background(), apiTypes.VolumeCreate{
		EnvironmentID: "env_1", Slug: "uploads", Key: "uploads-data",
	})
	if err != nil || created.TaskID != "tsk_create" || created.Volume.Key != "uploads-data" {
		t.Fatalf("create = %#v, %v", created, err)
	}
	edited, err := client.EditVolume(context.Background(), "vol_1", apiTypes.VolumeEdit{Slug: "archive"})
	if err != nil || edited.TaskID != "tsk_edit" || edited.Volume.Slug != "archive" {
		t.Fatalf("edit = %#v, %v", edited, err)
	}
	removed, err := client.RemoveVolume(context.Background(), "vol_1", "impact", "uploads-data")
	if err != nil || removed.TaskID != "tsk_remove" {
		t.Fatalf("remove = %#v, %v", removed, err)
	}
}

// Rationale: The generated create request represents the optional Compose key
// as a pointer, so the mapping must omit it when the public input omits it.
func TestGeneratedVolumeCreateBodyPreservesOptionalKey(t *testing.T) {
	t.Parallel()

	withoutKey := generatedVolumeCreateBody(apiTypes.VolumeCreate{
		EnvironmentID: "env_1",
		Slug:          "uploads",
	})
	if withoutKey.Key != nil {
		t.Fatalf("body without key = %#v, want nil key", withoutKey)
	}
	if withoutKey.EnvironmentId != "env_1" || withoutKey.Slug != "uploads" {
		t.Fatalf("body without key = %#v", withoutKey)
	}

	withKey := generatedVolumeCreateBody(apiTypes.VolumeCreate{
		EnvironmentID: "env_1",
		Slug:          "uploads",
		Key:           "uploads-data",
	})
	if withKey.Key == nil || *withKey.Key != "uploads-data" {
		t.Fatalf("body with key = %#v, want uploads-data", withKey)
	}
}

// Rationale: Volume edits have one mutable field and must map directly to the
// generated request without reserializing through an untyped intermediate.
func TestGeneratedVolumeEditBodyMapsSlug(t *testing.T) {
	t.Parallel()

	body := generatedVolumeEditBody(apiTypes.VolumeEdit{Slug: "archive"})
	if body.Slug != "archive" {
		t.Fatalf("body = %#v, want slug archive", body)
	}
}

func TestGetVolumeDeletionImpactPreservesFixedRevisionPage(t *testing.T) {
	t.Parallel()

	server := newVolumeTestServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/api/v1/volumes/vol_1/deletion-impact" {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		if request.URL.Query().Get("cursor") != "next" || request.URL.Query().Get("limit") != "1" {
			t.Errorf("query = %q", request.URL.RawQuery)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"volume_id":"vol_1","slug":"uploads","key":"uploads-data","environment_id":"env_1","revision":12,"environment_head":"head","items":[],"complete":true,"item_count":0,"rolling_digest":"digest","impact_token":"token","data_handling":"recursive_destroy"}`)
	}))
	defer server.Close()

	page, err := New(server.URL).GetVolumeDeletionImpact(context.Background(), "vol_1", "next", 1)
	if err != nil {
		t.Fatal(err)
	}
	if page.VolumeID != "vol_1" || page.Revision != 12 || page.ImpactToken != "token" || !page.Complete {
		t.Fatalf("page = %#v", page)
	}
}

func newVolumeTestServer(handler http.Handler) *httptest.Server {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	server := &httptest.Server{Listener: listener, Config: &http.Server{Handler: handler}}
	server.Start()
	return server
}
