package blueprint

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: Component authoring reads current decisions; a concurrent desired
// revision change must reject before any claim, staging or Task publication.
func TestDeferredBlueprintCapturePreservesAuthoringRevisionGuard(t *testing.T) {
	store := &blueprintTestStore{values: map[string]testkeyvalue.KeyValue{}, revision: 1}
	hierarchy, err := etcd.NewEnvironmentBlueprintRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	protector, err := secretvalue.NewProtector(blueprintTestCrypt{}, blueprintTestCrypt{})
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
		durableRepository: &durableRepository{
			EnvironmentBlueprintRepository: hierarchy,
		},
		tenant: testhierarchy.TenantRecord{ID: tenantID, Slug: "tenant"},
		project: testhierarchy.ProjectRecord{
			ID:       projectID,
			TenantID: tenantID,
			Slug:     "project",
			Kind:     testhierarchy.ProjectKindTenant,
		},
		environment: testhierarchy.EnvironmentRecord{ID: environmentID, ProjectID: projectID, Name: "production"},
	}
	service := &Service{repository: repository, idempotency: idempotency}
	_, err = service.loadAuthoredApplyBaseline(t.Context(), environmentID, ids.NewAt(ids.KindTask, at, 5))
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStateConflict {
		t.Fatalf("stale Component authoring error = %v", err)
	}
	if store.writes != 0 {
		t.Fatalf("stale authoring wrote state: writes=%d", store.writes)
	}
}
