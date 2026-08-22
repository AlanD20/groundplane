package controller

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

type fakeSecretReader struct {
	record      etcd.Versioned[etcd.SecretRecord]
	page        etcd.Page[etcd.SecretRecord]
	revealed    string
	wantScope   core.SecretScope
	wantProject string
	wantPage    etcd.PageRequest
	listCalls   int
}

func (fake *fakeSecretReader) GetSecret(
	_ context.Context,
	_ string,
) (etcd.Versioned[etcd.SecretRecord], error) {
	return fake.record, nil
}

func (fake *fakeSecretReader) ListSecrets(
	_ context.Context,
	scope core.SecretScope,
	projectID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.SecretRecord], error) {
	fake.listCalls++
	if scope != fake.wantScope || projectID != fake.wantProject || request != fake.wantPage {
		return etcd.Page[etcd.SecretRecord]{}, io.ErrUnexpectedEOF
	}
	return fake.page, nil
}

func (fake *fakeSecretReader) RevealSecret(_ context.Context, _ string) (string, error) {
	return fake.revealed, nil
}

// Rationale: reusable Secret metadata must expose the accepted redacted shape and cursor through
// typed routes, while plaintext appears only in the explicit value response.
func TestSecretRoutesKeepMetadataRedactedAndRevealExplicit(t *testing.T) {
	t.Parallel()
	projectID := ids.NewAt(ids.KindProject, secretRouteTestTime(), 1)
	secretID := ids.NewAt(ids.KindSecret, secretRouteTestTime(), 2)
	record := etcd.SecretRecord{Secret: core.Secret{
		ID: secretID, Scope: core.SecretScopeProject, ProjectID: projectID,
		Key: "DATABASE_PASSWORD", Kind: core.SecretKindEnvVar, Ref: "secrets/.env." + projectID,
		UpdatedAt: secretRouteTestTime(),
	}}
	pageRequest := etcd.PageRequest{Limit: 3, Cursor: "opaque"}
	reader := &fakeSecretReader{
		record: etcd.Versioned[etcd.SecretRecord]{Record: record},
		page: etcd.Page[etcd.SecretRecord]{
			Items: []etcd.Versioned[etcd.SecretRecord]{{Record: record}}, NextCursor: "next",
		},
		revealed: "database-password", wantScope: core.SecretScopeProject,
		wantProject: projectID, wantPage: pageRequest,
	}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Secrets: reader})

	list := httptest.NewRecorder()
	server.Mux.ServeHTTP(list, httptest.NewRequest(
		http.MethodGet,
		"/api/v1/secrets?project="+projectID+"&limit=3&cursor=opaque",
		nil,
	))
	var page apiTypes.Page[apiTypes.Secret]
	if list.Code != http.StatusOK || json.Unmarshal(list.Body.Bytes(), &page) != nil ||
		reader.listCalls != 1 || len(page.Items) != 1 || page.NextCursor != "next" ||
		page.Items[0].UpdatedAt != secretRouteTestTime().Format(time.RFC3339Nano) ||
		strings.Contains(list.Body.String(), "database-password") {
		t.Fatalf("list response = %d/%s, calls %d, page %#v", list.Code, list.Body.String(), reader.listCalls, page)
	}

	detail := httptest.NewRecorder()
	server.Mux.ServeHTTP(
		detail,
		httptest.NewRequest(http.MethodGet, "/api/v1/secrets/"+secretID, nil),
	)
	var shown apiTypes.Secret
	if detail.Code != http.StatusOK || json.Unmarshal(detail.Body.Bytes(), &shown) != nil ||
		shown.ID != secretID || shown.Key != record.Secret.Key || strings.Contains(detail.Body.String(), "database-password") {
		t.Fatalf("detail response = %d/%s, shown %#v", detail.Code, detail.Body.String(), shown)
	}

	reveal := httptest.NewRecorder()
	server.Mux.ServeHTTP(
		reveal,
		httptest.NewRequest(http.MethodGet, "/api/v1/secrets/"+secretID+"/value", nil),
	)
	var value apiTypes.SecretValue
	if reveal.Code != http.StatusOK || json.Unmarshal(reveal.Body.Bytes(), &value) != nil ||
		value.Value != "database-password" {
		t.Fatalf("reveal response = %d/%s, value %#v", reveal.Code, reveal.Body.String(), value)
	}
}

// Rationale: owner selection is part of the collection identity, so missing, mixed, false,
// duplicated, or unknown selectors must fail before any repository read.
func TestSecretListRejectsAmbiguousOwnerQueries(t *testing.T) {
	t.Parallel()
	projectID := ids.NewAt(ids.KindProject, secretRouteTestTime(), 3)
	for _, query := range []string{
		"", "?project=" + projectID + "&platform=true", "?platform=false",
		"?platform=true&platform=true", "?platform=true&status=ready",
	} {
		reader := &fakeSecretReader{}
		server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Secrets: reader})
		response := httptest.NewRecorder()
		server.Mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/secrets"+query, nil))
		if response.Code != http.StatusBadRequest || reader.listCalls != 0 {
			t.Fatalf("query %q = %d/%s, list calls %d", query, response.Code, response.Body.String(), reader.listCalls)
		}
	}
}

func secretRouteTestTime() time.Time {
	return time.Date(2026, time.August, 22, 14, 30, 0, 0, time.UTC)
}
