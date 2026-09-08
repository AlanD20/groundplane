package controller

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/backupkey"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type fakeBackupKeyService struct {
	exported []byte
	rotated  []byte
	exports  int
	rotates  int
}

func (service *fakeBackupKeyService) ExportBackupKey(context.Context, string) (backupkey.Export, error) {
	service.exports++
	service.exported = []byte("AGE-SECRET-KEY-1TEST\n")
	return backupkey.Export{Identity: service.exported, Era: 7}, nil
}

func (service *fakeBackupKeyService) RotateBackupKey(
	context.Context,
	string,
	string,
) (etcd.IdempotencyResponse, error) {
	service.rotates++
	service.rotated = []byte("{\"task_id\":\"task_01ARZ3NDEKTSV4RRFFQ69G5FAV\"}")
	return etcd.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        service.rotated,
	}, nil
}

// Rationale: export must emit the exact no-store attachment and clear the
// service-owned private identity only after Huma has copied it to the response.
func TestBackupKeyExportWritesExactNoStoreAttachmentAndClearsServiceBuffer(t *testing.T) {
	service := &fakeBackupKeyService{}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		BackupKeyExports:   service,
		BackupKeyMutations: service,
	})
	environmentID := "env_01J00000000000000000000000"
	request := httptest.NewRequest(http.MethodPost, "/api/v1/environments/"+environmentID+"/export-key", nil)
	recorder := httptest.NewRecorder()
	server.HTTPHandler().ServeHTTP(recorder, request)
	response := recorder.Result()
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !bytes.Equal(body, []byte("AGE-SECRET-KEY-1TEST\n")) {
		t.Fatalf("response = %d %q", response.StatusCode, body)
	}
	if got := response.Header.Get("Content-Type"); got != "text/plain; charset=utf-8" {
		t.Fatalf("Content-Type = %q", got)
	}
	wantDisposition := "attachment; filename=\"groundplane-" + environmentID + "-age-era-7-identity.txt\""
	if got := response.Header.Get("Content-Disposition"); got != wantDisposition {
		t.Fatalf("Content-Disposition = %q, want %q", got, wantDisposition)
	}
	if got := response.Header.Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q", got)
	}
	if !bytes.Equal(service.exported, make([]byte, len(service.exported))) {
		t.Fatalf("service-owned identity was not cleared: %q", service.exported)
	}
}

// Rationale: rotation must copy TaskAccepted before clearing service-owned
// bytes so the HTTP response never observes a zeroed JSON buffer.
func TestBackupKeyRotationWritesOwnedTaskAcceptedBody(t *testing.T) {
	service := &fakeBackupKeyService{}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		BackupKeyExports:   service,
		BackupKeyMutations: service,
	})
	environmentID := "env_01J00000000000000000000000"
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/environments/"+environmentID+"/rotate-key",
		nil,
	)
	request.Header.Set("Idempotency-Key", "rotate-key-test-0001")
	recorder := httptest.NewRecorder()
	server.HTTPHandler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted ||
		recorder.Body.String() != "{\"task_id\":\"task_01ARZ3NDEKTSV4RRFFQ69G5FAV\"}" {
		t.Fatalf("response = %d %q", recorder.Code, recorder.Body.Bytes())
	}
	if !bytes.Equal(service.rotated, make([]byte, len(service.rotated))) {
		t.Fatalf("service rotation response was not cleared: %q", service.rotated)
	}
}

// Rationale: both bodyless key operations must reject ignored request bytes
// before invoking either secret-bearing service boundary.
func TestBackupKeyMutationRoutesRejectBodies(t *testing.T) {
	service := &fakeBackupKeyService{}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		BackupKeyExports:   service,
		BackupKeyMutations: service,
	})
	environmentID := "env_01J00000000000000000000000"
	for _, route := range []string{"rotate-key", "export-key"} {
		request := httptest.NewRequest(
			http.MethodPost,
			"/api/v1/environments/"+environmentID+"/"+route,
			bytes.NewBufferString("{}"),
		)
		request.Header.Set("Content-Type", "application/json")
		if route == "rotate-key" {
			request.Header.Set("Idempotency-Key", "rotate-key-test-0001")
		}
		recorder := httptest.NewRecorder()
		server.HTTPHandler().ServeHTTP(recorder, request)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400", route, recorder.Code)
		}
	}
	if service.exports != 0 || service.rotates != 0 {
		t.Fatalf("service calls exports/rotates = %d/%d", service.exports, service.rotates)
	}
}
