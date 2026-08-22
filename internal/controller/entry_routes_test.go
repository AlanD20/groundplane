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

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

type fakeEntryReader struct {
	record          etcd.Versioned[etcd.EntryRecord]
	page            etcd.Page[etcd.EntryRecord]
	revealed        string
	wantEnvironment string
	wantPage        etcd.PageRequest
	listCalls       int
}

func (fake *fakeEntryReader) GetEntry(
	_ context.Context,
	_ string,
) (etcd.Versioned[etcd.EntryRecord], error) {
	return fake.record, nil
}

func (fake *fakeEntryReader) ListEntries(
	_ context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.EntryRecord], error) {
	fake.listCalls++
	if environmentID != fake.wantEnvironment || request != fake.wantPage {
		return etcd.Page[etcd.EntryRecord]{}, io.ErrUnexpectedEOF
	}
	return fake.page, nil
}

func (fake *fakeEntryReader) RevealEntry(_ context.Context, _ string) (string, error) {
	return fake.revealed, nil
}

// Rationale: Entry metadata must flatten the accepted fact source on list/detail without leaking
// selected bytes, while only the explicit value route may return decrypted plaintext.
func TestEntryRoutesFlattenMetadataAndRevealExplicitly(t *testing.T) {
	t.Parallel()
	environmentID := ids.NewAt(ids.KindEnvironment, secretRouteTestTime(), 31)
	entryID := ids.NewAt(ids.KindEnvEntry, secretRouteTestTime(), 32)
	record := etcd.EntryRecord{
		EnvironmentID: environmentID,
		Entry: core.EnvEntry{
			ID: entryID, Kind: core.EntryKindEnv, Key: "DATABASE_URL",
			Source: core.EntrySource{Kind: core.SourceFact, Fact: &core.FactRef{
				Attach: "att_primary", Grant: "att_reporting", Key: "pg16_URL",
			}},
			Exposure: []string{"api"}, Secret: true,
		},
	}
	pageRequest := etcd.PageRequest{Limit: 5, Cursor: "opaque"}
	reader := &fakeEntryReader{
		record: etcd.Versioned[etcd.EntryRecord]{Record: record},
		page: etcd.Page[etcd.EntryRecord]{
			Items: []etcd.Versioned[etcd.EntryRecord]{{Record: record}}, NextCursor: "next",
		},
		revealed: "postgres://credential", wantEnvironment: environmentID, wantPage: pageRequest,
	}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Entries: reader})

	list := httptest.NewRecorder()
	server.Mux.ServeHTTP(list, httptest.NewRequest(
		http.MethodGet,
		"/api/v1/entries?environment="+environmentID+"&limit=5&cursor=opaque",
		nil,
	))
	var page apiTypes.Page[apiTypes.Entry]
	if list.Code != http.StatusOK || json.Unmarshal(list.Body.Bytes(), &page) != nil ||
		reader.listCalls != 1 || len(page.Items) != 1 || page.NextCursor != "next" ||
		page.Items[0].Source.AttachID != "att_primary" || page.Items[0].Source.GrantAttachID != "att_reporting" ||
		page.Items[0].Source.Fact != "pg16_URL" || strings.Contains(list.Body.String(), "postgres://credential") {
		t.Fatalf("list response = %d/%s, calls %d, page %#v", list.Code, list.Body.String(), reader.listCalls, page)
	}

	detail := httptest.NewRecorder()
	server.Mux.ServeHTTP(detail, httptest.NewRequest(http.MethodGet, "/api/v1/entries/"+entryID, nil))
	var shown apiTypes.Entry
	if detail.Code != http.StatusOK || json.Unmarshal(detail.Body.Bytes(), &shown) != nil ||
		shown.ID != entryID || shown.Key != "DATABASE_URL" || strings.Contains(detail.Body.String(), "postgres://credential") {
		t.Fatalf("detail response = %d/%s, shown %#v", detail.Code, detail.Body.String(), shown)
	}

	reveal := httptest.NewRecorder()
	server.Mux.ServeHTTP(reveal, httptest.NewRequest(http.MethodGet, "/api/v1/entries/"+entryID+"/value", nil))
	var value apiTypes.EntryValue
	if reveal.Code != http.StatusOK || json.Unmarshal(reveal.Body.Bytes(), &value) != nil ||
		value.Value != "postgres://credential" {
		t.Fatalf("reveal response = %d/%s, value %#v", reveal.Code, reveal.Body.String(), value)
	}
}

// Rationale: Environment ownership is the collection identity, so missing, duplicate, and unknown
// query selectors must fail before the Entry repository can be read.
func TestEntryListRejectsInvalidEnvironmentQueries(t *testing.T) {
	t.Parallel()
	environmentID := ids.NewAt(ids.KindEnvironment, secretRouteTestTime(), 33)
	for _, query := range []string{
		"", "?environment=", "?environment=" + environmentID + "&environment=" + environmentID,
		"?environment=" + environmentID + "&status=ready",
	} {
		reader := &fakeEntryReader{}
		server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Entries: reader})
		response := httptest.NewRecorder()
		server.Mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/entries"+query, nil))
		if response.Code != http.StatusBadRequest || reader.listCalls != 0 {
			t.Fatalf("query %q = %d/%s, list calls %d", query, response.Code, response.Body.String(), reader.listCalls)
		}
	}
}
