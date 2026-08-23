package controller

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func TestScriptResponseProjectsOnlyThePublicContract(t *testing.T) {
	// Rationale: the stable target Service id is durable metadata but the locked public response exposes its label.
	record, err := etcd.NewScriptRecord(ids.New(ids.KindEnvironment), ids.New(ids.KindService), core.Script{
		ID: ids.New(ids.KindScript), Name: "migrate", ServiceName: "api",
		Body: "php artisan migrate --force", When: core.ScriptHook("manual"),
	})
	if err != nil {
		t.Fatalf("NewScriptRecord(): %v", err)
	}
	response := scriptResponse(record)
	if response.ID != record.Desired.ID || response.Name != "migrate" || response.ServiceName != "api" ||
		response.Body != record.Desired.Body || response.When != "manual" {
		t.Fatalf("scriptResponse() = %#v", response)
	}
}

func TestScriptListRequestRequiresStableEnvironment(t *testing.T) {
	// Rationale: name-scoped reads would make pagination and tenant isolation ambiguous.
	if _, err := scriptListRequest("production", 0, ""); err == nil {
		t.Fatal("scriptListRequest() accepted an Environment label")
	}
	if _, err := scriptListRequest(ids.New(ids.KindEnvironment), 200, ""); err != nil {
		t.Fatalf("scriptListRequest() stable id: %v", err)
	}
}
