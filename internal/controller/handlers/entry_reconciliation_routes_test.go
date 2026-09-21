package handlers

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// Rationale: a Script file grant resolves the immutable Blueprint Entry key,
// never a similarly named variable or path, without revealing any Entry value.
func TestEntryReadExposesBlueprintReconciliationKey(t *testing.T) {
	entryID, environmentID := ids.New(ids.KindEnvEntry), ids.New(ids.KindEnvironment)
	record := testentries.Record{EnvironmentID: environmentID, BlueprintKey: "tls-seed", Entry: core.EnvEntry{
		ID: entryID, Kind: core.EntryKindFile, Path: "/etc/tls/seed.pem", Secret: true,
		Source:   core.EntrySource{Kind: core.SourceSecretRef, SecretRef: ids.New(ids.KindSecret)},
		Exposure: []string{"consumer"},
	}}
	reader := &fakeEntryReader{record: testkeyvalue.Versioned[testentries.Record]{Record: record}}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Entries: reader})
	response := httptest.NewRecorder()
	server.HTTPHandler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/entries/"+entryID, nil))
	var got struct {
		ID                string `json:"id"`
		ReconciliationKey string `json:"reconciliation_key"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || got.ID != entryID || got.ReconciliationKey != "tls-seed" {
		t.Fatalf("Entry identity projection = %d, %#v", response.Code, got)
	}
}
