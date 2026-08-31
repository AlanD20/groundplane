package desiredrevision

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestReconcileBlueprintScriptsDerivesAndPreservesIdentityByReconciliationKey(t *testing.T) {
	// Rationale: the authored reconciliation key, not a mutable slug or map iteration order,
	// must deterministically own the Script id across first apply and replay.
	at := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	service := desiredRevisionScriptService(t, environmentID, ids.NewAt(ids.KindService, at, 2), "api")
	authority := ids.NewAt(ids.KindTask, at, 3)
	allocate := func(kind ids.Kind, purpose string) string {
		return ids.DeriveAt(kind, at, authority, "test/"+purpose)
	}
	authored := map[string]core.ScriptSpec{
		"migration-hook": {
			Slug: "migrate", Service: "api", When: core.ScriptPreDeploy,
			Script: "php artisan migrate --force",
		},
	}

	first, err := ReconcileBlueprintScripts(environmentID, authored, []etcd.ServiceRecord{service}, nil, allocate)
	if err != nil {
		t.Fatalf("ReconcileBlueprintScripts(first) error = %v", err)
	}
	wantID := ids.DeriveAt(ids.KindScript, at, authority, "test/script/migration-hook")
	if len(first.Current) != 1 || first.Current[0].Desired.ID != wantID ||
		first.Current[0].ReconciliationKey != "migration-hook" || first.Current[0].Origin != "blueprint" ||
		len(first.BodyGenerations) != 1 || first.BodyGenerations[0].ScriptID != wantID {
		t.Fatalf("first reconciliation = %#v", first)
	}

	replayed, err := ReconcileBlueprintScripts(
		environmentID,
		authored,
		[]etcd.ServiceRecord{service},
		first.Current,
		func(ids.Kind, string) string {
			t.Fatal("reapply allocated a new Script id")
			return ""
		},
	)
	if err != nil {
		t.Fatalf("ReconcileBlueprintScripts(reapply) error = %v", err)
	}
	if len(replayed.Current) != 1 || replayed.Current[0] != first.Current[0] ||
		len(replayed.BodyGenerations) != 0 {
		t.Fatalf("reapplied reconciliation = %#v", replayed)
	}
}

func TestReconcileBlueprintScriptsBodyChangeAppendsExactGeneration(t *testing.T) {
	// Rationale: a body edit must advance only the active generation and emit the exact immutable
	// bytes, size, and digest needed for publication and recovery.
	at := time.Date(2026, 8, 30, 13, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 10)
	service := desiredRevisionScriptService(t, environmentID, ids.NewAt(ids.KindService, at, 11), "api")
	previous := desiredRevisionScriptRecord(
		t, environmentID, service, ids.NewAt(ids.KindScript, at, 12),
		"migration-hook", "migrate", "printf old-body", "blueprint",
	)
	const body = "printf exact-new-body"

	reconciled, err := ReconcileBlueprintScripts(
		environmentID,
		map[string]core.ScriptSpec{
			"migration-hook": {Slug: "migrate-renamed", Service: "api", When: core.ScriptPostDeploy, Script: body},
		},
		[]etcd.ServiceRecord{service},
		[]etcd.ScriptRecord{previous},
		func(ids.Kind, string) string {
			t.Fatal("existing reconciliation key allocated a new Script id")
			return ""
		},
	)
	if err != nil {
		t.Fatalf("ReconcileBlueprintScripts() error = %v", err)
	}
	digest := sha256.Sum256([]byte(body))
	wantDigest := hex.EncodeToString(digest[:])
	if len(reconciled.Current) != 1 || reconciled.Current[0].Desired.ID != previous.Desired.ID ||
		reconciled.Current[0].ActiveGeneration != 2 || reconciled.Current[0].Desired.Slug != "migrate-renamed" ||
		reconciled.Current[0].Desired.When != core.ScriptPostDeploy || reconciled.Current[0].Desired.Body != body ||
		len(reconciled.BodyGenerations) != 1 {
		t.Fatalf("body reconciliation = %#v", reconciled)
	}
	generation := reconciled.BodyGenerations[0]
	if generation.ScriptID != previous.Desired.ID || generation.Generation != 2 ||
		generation.Body != body || generation.BodySize != uint32(len(body)) || generation.BodySHA256 != wantDigest {
		t.Fatalf("new body generation = %#v", generation)
	}
}

func TestReconcileBlueprintScriptsCarriesOmittedBlueprintAndAPIRecordsForward(t *testing.T) {
	// Rationale: Blueprint apply is non-destructive, so omission must preserve both a previously
	// authored reconciliation identity and a direct API record without adoption or mutation.
	at := time.Date(2026, 8, 30, 14, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 20)
	service := desiredRevisionScriptService(t, environmentID, ids.NewAt(ids.KindService, at, 21), "api")
	blueprint := desiredRevisionScriptRecord(
		t, environmentID, service, ids.NewAt(ids.KindScript, at, 22),
		"migration-hook", "migrate", "printf blueprint", "blueprint",
	)
	api := desiredRevisionScriptRecord(
		t, environmentID, service, ids.NewAt(ids.KindScript, at, 23),
		"", "maintenance", "printf api", "api",
	)

	reconciled, err := ReconcileBlueprintScripts(
		environmentID,
		map[string]core.ScriptSpec{},
		[]etcd.ServiceRecord{service},
		[]etcd.ScriptRecord{api, blueprint},
		func(ids.Kind, string) string {
			t.Fatal("omission allocated a Script id")
			return ""
		},
	)
	if err != nil {
		t.Fatalf("ReconcileBlueprintScripts() error = %v", err)
	}
	byID := make(map[string]etcd.ScriptRecord, len(reconciled.Current))
	for _, record := range reconciled.Current {
		byID[record.Desired.ID] = record
	}
	if len(byID) != 2 || byID[blueprint.Desired.ID] != blueprint || byID[api.Desired.ID] != api ||
		len(reconciled.BodyGenerations) != 0 {
		t.Fatalf("omitted reconciliation = %#v", reconciled)
	}
}

func TestReconcileBlueprintScriptsRejectsSlugCollisionAndTargetChange(t *testing.T) {
	// Rationale: a new reconciliation key must never adopt a slug owner, and an existing key must
	// never silently retarget the stable Script id to another Service.
	at := time.Date(2026, 8, 30, 15, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 30)
	apiService := desiredRevisionScriptService(t, environmentID, ids.NewAt(ids.KindService, at, 31), "api")
	workerService := desiredRevisionScriptService(t, environmentID, ids.NewAt(ids.KindService, at, 32), "worker")

	t.Run("slug collision", func(t *testing.T) {
		api := desiredRevisionScriptRecord(
			t, environmentID, apiService, ids.NewAt(ids.KindScript, at, 33),
			"", "shared", "printf api", "api",
		)
		_, err := ReconcileBlueprintScripts(
			environmentID,
			map[string]core.ScriptSpec{
				"new-hook": {Slug: "shared", Service: "api", When: core.ScriptManual, Script: "printf new"},
			},
			[]etcd.ServiceRecord{apiService, workerService},
			[]etcd.ScriptRecord{api},
			func(kind ids.Kind, purpose string) string {
				return ids.DeriveAt(kind, at, ids.NewAt(ids.KindTask, at, 34), purpose)
			},
		)
		if !errors.Is(err, errs.New(errs.KindNameConflict, "")) {
			t.Fatalf("ReconcileBlueprintScripts(slug collision) error = %v, want name.conflict", err)
		}
	})

	t.Run("target change", func(t *testing.T) {
		blueprint := desiredRevisionScriptRecord(
			t, environmentID, apiService, ids.NewAt(ids.KindScript, at, 35),
			"migration-hook", "migrate", "printf migrate", "blueprint",
		)
		_, err := ReconcileBlueprintScripts(
			environmentID,
			map[string]core.ScriptSpec{
				"migration-hook": {
					Slug: "migrate", Service: "worker", When: core.ScriptPreDeploy, Script: "printf migrate",
				},
			},
			[]etcd.ServiceRecord{apiService, workerService},
			[]etcd.ScriptRecord{blueprint},
			func(ids.Kind, string) string {
				t.Fatal("target change allocated a Script id")
				return ""
			},
		)
		if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
			t.Fatalf("ReconcileBlueprintScripts(target change) error = %v, want validation.failed", err)
		}
	})
}

func TestReconcileBlueprintScriptsIgnoresReplicaCountOfUntargetedServices(t *testing.T) {
	// Rationale: ADR 0040 constrains only Services selected by a Script; an unrelated
	// horizontally scaled Service must not make an otherwise valid Blueprint fail.
	at := time.Date(2026, 8, 30, 15, 30, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 40)
	target := desiredRevisionScriptService(t, environmentID, ids.NewAt(ids.KindService, at, 41), "api")
	unrelated := desiredRevisionScriptService(t, environmentID, ids.NewAt(ids.KindService, at, 42), "worker")
	unrelated.Desired.Replicas = 3

	result, err := ReconcileBlueprintScripts(
		environmentID,
		map[string]core.ScriptSpec{
			"migration-hook": {Slug: "migrate", Service: "api", When: core.ScriptManual, Script: "printf migrate"},
		},
		[]etcd.ServiceRecord{target, unrelated},
		nil,
		func(kind ids.Kind, purpose string) string {
			return ids.DeriveAt(kind, at, ids.NewAt(ids.KindTask, at, 43), purpose)
		},
	)
	if err != nil || len(result.Current) != 1 || result.Current[0].ServiceID != target.Desired.ID {
		t.Fatalf("ReconcileBlueprintScripts() = %#v, %v", result, err)
	}
}

func desiredRevisionScriptService(
	t *testing.T,
	environmentID string,
	serviceID string,
	name string,
) etcd.ServiceRecord {
	t.Helper()
	record, err := etcd.NewServiceRecord(environmentID, core.Service{
		ID: serviceID, Name: name, Image: "example.invalid/" + name + ":1", Replicas: 1,
	}, "")
	if err != nil {
		t.Fatalf("NewServiceRecord(%s) error = %v", name, err)
	}
	return record
}

func desiredRevisionScriptRecord(
	t *testing.T,
	environmentID string,
	service etcd.ServiceRecord,
	scriptID string,
	reconciliationKey string,
	slug string,
	body string,
	origin string,
) etcd.ScriptRecord {
	t.Helper()
	record, err := etcd.NewScriptRecord(environmentID, service.Desired.ID, core.Script{
		ID: scriptID, Slug: slug, ServiceName: service.Desired.Name, Body: body, When: core.ScriptManual,
	})
	if err != nil {
		t.Fatalf("NewScriptRecord(%s) error = %v", slug, err)
	}
	record.Origin = origin
	record.ReconciliationKey = reconciliationKey
	return record
}
