package app

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: Component authoring reads current decisions; a concurrent desired
// revision change must reject before any claim, staging or Task publication.
func TestApplyComponentBlueprintPreservesAuthoringRevisionGuard(t *testing.T) {
	store := &blueprintPreflightStore{values: map[string]etcd.KeyValue{}, revision: 1}
	hierarchy, err := etcd.NewEnvironmentBlueprintRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	protector, err := secretvalue.NewProtector(attachFactTestCrypt{}, attachFactTestCrypt{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := idempotentintent.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	records, err := etcd.NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := desiredrevision.NewIdempotency(coordinator, records)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 6, 8, 0, 0, 0, time.UTC)
	tenantID, projectID := ids.NewAt(ids.KindTenant, at, 1), ids.NewAt(ids.KindProject, at, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 3)
	repository := &blueprintPreflightRepository{
		durableEnvironmentBlueprintRepository: &durableEnvironmentBlueprintRepository{
			EnvironmentBlueprintRepository: hierarchy,
		},
		tenant: etcd.TenantRecord{ID: tenantID, Slug: "tenant"},
		project: etcd.ProjectRecord{
			ID:       projectID,
			TenantID: tenantID,
			Slug:     "project",
			Kind:     etcd.ProjectKindTenant,
		},
		environment: etcd.EnvironmentRecord{ID: environmentID, ProjectID: projectID, Name: "production"},
	}
	service := &environmentBlueprintService{repository: repository, idempotency: idempotency}
	bundle := core.BlueprintBundle{
		RootPath: "groundplane.yaml", ComposeSources: []string{"groundplane.yaml"},
		Files: []core.BlueprintFile{{Path: "groundplane.yaml", Content: []byte("services: {}\n")}},
	}
	componentID := ids.NewAt(ids.KindComponent, at, 4)
	response, err := service.ApplyComponentBlueprint(t.Context(), environmentID, componentID,
		bundle, ids.NewAt(ids.KindTask, at, 5), "stale-component-authoring")
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStateConflict {
		t.Fatalf("stale Component authoring error = %v", err)
	}
	if store.writes != 0 || response.Status != 0 {
		t.Fatalf("stale authoring wrote state: writes=%d response=%#v", store.writes, response)
	}
	if _, err := service.ApplyComponentBlueprint(t.Context(), environmentID, componentID,
		bundle, "", "missing-component-revision"); err == nil {
		t.Fatal("Component mutation accepted an absent authoring revision")
	}
}
