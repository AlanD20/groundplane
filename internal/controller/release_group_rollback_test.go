package controller

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type fakeReleaseGroupRollbackOperator struct {
	ReleaseOperator
	groupID        string
	request        domain.GroupRollbackInput
	idempotencyKey string
}

func (fake *fakeReleaseGroupRollbackOperator) PreviewReleaseGroupRollback(_ context.Context, groupID string, input domain.GroupRollbackPreviewInput) (domain.GroupRollbackPreview, error) {
	fake.groupID = groupID
	fake.request.Tag = input.Tag
	return domain.GroupRollbackPreview{GroupID: groupID, Revision: 42, Sources: []domain.RollbackSource{{ServiceID: "svc_01J00000000000000000000000", ReleaseID: "dep_01J00000000000000000000000", Tag: "release-1"}}}, nil
}

func (fake *fakeReleaseGroupRollbackOperator) RollbackReleaseGroup(
	_ context.Context,
	groupID string,
	request domain.GroupRollbackInput,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	fake.groupID = groupID
	fake.request = request
	fake.idempotencyKey = idempotencyKey
	return etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: []byte(`{"task_id":"task_group_rollback"}`),
	}, nil
}

func TestReleaseGroupRollbackPreviewReturnsControllerSelection(t *testing.T) {
	groupID := ids.NewAt(ids.KindReleaseGroup, time.Date(2026, time.September, 4, 0, 0, 0, 0, time.UTC), 1)
	operator := &fakeReleaseGroupRollbackOperator{}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{ReleaseOperations: operator})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/release-groups/"+groupID+"/rollback-preview?tag=release-1", nil)
	response := httptest.NewRecorder()
	server.HTTPHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"revision":"42"`) || !strings.Contains(response.Body.String(), `"release_id":"dep_`) {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
	if operator.request.Tag == nil || *operator.request.Tag != "release-1" {
		t.Fatalf("preview tag = %#v", operator.request.Tag)
	}
}

func TestReleaseGroupRollbackRejectsExplicitBlankTag(t *testing.T) {
	groupID := ids.NewAt(ids.KindReleaseGroup, time.Date(2026, time.September, 4, 0, 0, 0, 0, time.UTC), 1)
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{ReleaseOperations: &fakeReleaseGroupRollbackOperator{}})
	for _, target := range []string{"/api/v1/release-groups/" + groupID + "/rollback", "/api/v1/release-groups/" + groupID + "/rollback-preview?tag=%20"} {
		method, body := http.MethodGet, ""
		wantStatus := http.StatusBadRequest
		if !strings.Contains(target, "preview") {
			method, body = http.MethodPost, `{"tag":" "}`
			wantStatus = http.StatusUnprocessableEntity
		}
		request := httptest.NewRequest(method, target, strings.NewReader(body))
		if body != "" {
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set(idempotencyKeyHeader, "release-group-rollback-key")
		}
		response := httptest.NewRecorder()
		server.HTTPHandler().ServeHTTP(response, request)
		if response.Code != wantStatus {
			t.Fatalf("%s response = %d %s", target, response.Code, response.Body.String())
		}
	}
}

func TestReleaseGroupRollbackAcceptsOptionalTagBody(t *testing.T) {
	// Rationale: rollback omission selects each member's automatic candidate,
	// while an explicit tag must survive the native Huma boundary unchanged.
	t.Parallel()
	groupID := ids.NewAt(ids.KindReleaseGroup, time.Date(2026, time.September, 4, 0, 0, 0, 0, time.UTC), 1)
	tests := []struct {
		name string
		body string
		tag  *string
	}{
		{name: "bodyless"},
		{name: "empty object", body: `{}`},
		{name: "explicit tag", body: `{"tag":"release-2026-09-04"}`, tag: stringPointer("release-2026-09-04")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			operator := &fakeReleaseGroupRollbackOperator{}
			server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{ReleaseOperations: operator})
			request := httptest.NewRequest(http.MethodPost, "/api/v1/release-groups/"+groupID+"/rollback", strings.NewReader(test.body))
			request.Header.Set(idempotencyKeyHeader, "release-group-rollback-key")
			if test.body != "" {
				request.Header.Set("Content-Type", "application/json")
			}
			response := httptest.NewRecorder()

			server.HTTPHandler().ServeHTTP(response, request)

			if response.Code != http.StatusAccepted || response.Body.String() != `{"task_id":"task_group_rollback"}` {
				t.Fatalf("response = %d %s", response.Code, response.Body.String())
			}
			if operator.groupID != groupID || !sameStringPointer(operator.request.Tag, test.tag) ||
				operator.idempotencyKey != "release-group-rollback-key" {
				t.Fatalf("rollback call = %#v", operator)
			}
		})
	}
}

func stringPointer(value string) *string { return &value }
func sameStringPointer(left, right *string) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}
