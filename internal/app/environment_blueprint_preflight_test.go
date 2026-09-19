package app

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/entrygeneration"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	desiredrevisionstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	releasegroupstore "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroup"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type blueprintPreflightResolver struct {
	calls     int
	agentID   string
	selectors []*agentpb.WorkloadImageSelector
}

func (r *blueprintPreflightResolver) ResolveWorkloadImages(
	_ context.Context,
	agentID string,
	selectors []*agentpb.WorkloadImageSelector,
) (*agentpb.WorkloadImageResolutionResult, error) {
	r.calls++
	r.agentID = agentID
	r.selectors = selectors
	return nil, errs.New(errs.KindWorkloadImageResolutionUnavailable, "test Agent unavailable")
}

type blueprintPreflightMaterials struct{}

func (blueprintPreflightMaterials) ResolveTaskMaterializationSource(
	context.Context,
	string,
	etcd.TaskMaterializationSource,
) ([]byte, error) {
	return nil, errors.New("unexpected materialization before image preflight")
}

func (blueprintPreflightMaterials) PinSecretValue(
	context.Context,
	string,
	string,
) (etcd.TaskSecretValueReference, error) {
	return etcd.TaskSecretValueReference{}, errors.New("unexpected secret pin before image preflight")
}

func (blueprintPreflightMaterials) RetainComponentFile(
	context.Context,
	etcd.TaskMaterializationRecord,
	uint64,
	[]byte,
) error {
	return errors.New("unexpected materialization retention before image preflight")
}

// The hierarchy read seam supplies a coherent existing Environment; every other
// read and all claim/staging/publication methods are real persistence wrappers.
type blueprintPreflightRepository struct {
	*durableEnvironmentBlueprintRepository
	tenant      etcd.TenantRecord
	project     etcd.ProjectRecord
	environment etcd.EnvironmentRecord
}

func (r *blueprintPreflightRepository) GetTenant(context.Context, string) (etcd.Versioned[etcd.TenantRecord], error) {
	return etcd.Versioned[etcd.TenantRecord]{Record: r.tenant, Revision: 1, ReadRevision: 1}, nil
}
func (r *blueprintPreflightRepository) GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error) {
	return etcd.Versioned[etcd.ProjectRecord]{Record: r.project, Revision: 1, ReadRevision: 1}, nil
}

func (r *blueprintPreflightRepository) GetEnvironment(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return etcd.Versioned[etcd.EnvironmentRecord]{Record: r.environment, Revision: 1, ReadRevision: 1}, nil
}

// Rationale: a failed host-local image lookup must leave no durable Blueprint
// claim, staged sources, Release ledger writes, Task, or idempotency publication.
func TestApplyBlueprintImageFailurePrecedesEveryDurableWrite(t *testing.T) {
	store := &blueprintTestStore{values: map[string]etcd.KeyValue{}, revision: 1}
	resolver := &blueprintPreflightResolver{}
	hierarchy, err := etcd.NewEnvironmentBlueprintRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	desiredStore, err := desiredrevisionstore.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	zones, err := etcd.NewZoneRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	services, err := etcd.NewServiceRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := etcd.NewRouteRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := etcd.NewEntryRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	values, err := etcd.NewEntryValueGenerationRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	attaches, err := etcd.NewAttachRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	components, err := etcd.NewComponentRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	scripts, err := etcd.NewScriptRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := newDurableEnvironmentBlueprintRepository(
		hierarchy,
		desiredStore,
		zones,
		services,
		routes,
		entries,
		values,
		attaches,
		components,
		scripts,
	)
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
	idempotencyRecords, err := etcd.NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := desiredrevision.NewIdempotency(coordinator, idempotencyRecords)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := etcd.NewTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := etcd.NewReleaseLedger(store, tasks)
	if err != nil {
		t.Fatal(err)
	}
	sources, err := etcd.NewScriptSourceReferenceAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	agents, err := etcd.NewLocalAgentRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := registeredEnvironmentComponentCatalog()
	if err != nil {
		t.Fatal(err)
	}
	plans, err := controller.NewTaskPlanResolver("/var/lib/groundplane/volumes", catalog)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := controller.NewScriptArtifactService(scripts, blueprintPreflightMaterials{})
	if err != nil {
		t.Fatal(err)
	}
	releases, err := blueprintrelease.NewService(ledger, scripts, plans, artifacts, sources, agents, resolver)
	if err != nil {
		t.Fatal(err)
	}
	groups, err := releasegroupstore.New(store)
	if err != nil {
		t.Fatal(err)
	}
	groupPlanner, err := controller.NewReleaseGroupBlueprintPlanner(groups, hierarchy.HierarchyRepository)
	if err != nil {
		t.Fatal(err)
	}
	facts, err := NewAttachFactService(attaches, protector)
	if err != nil {
		t.Fatal(err)
	}
	secretRecords, err := etcd.NewSecretRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	entryGeneration, err := entrygeneration.NewEntryGenerationService(secretRecords, facts, protector)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	agentID := ids.NewAt(ids.KindAgent, now, 1)
	_, err = agents.CreateSingleton(t.Context(), etcd.LocalAgentRecord{
		ID: agentID, EnrollmentTaskID: ids.NewAt(ids.KindTask, now, 2), Image: "example.invalid/agent@sha256:" + strings.Repeat("a", 64),
		Generation: 1, Phase: etcd.LocalAgentPhaseProvisioning,
		Config: etcd.LocalAgentConfig{PullIntervalSeconds: 5, MaxConcurrentTasks: 1},
		EncryptedToken: []byte(
			"encrypted-test-token",
		), TokenDigest: base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("b", 32))), CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	tenantID, projectID, environmentID := ids.NewAt(
		ids.KindTenant,
		now,
		3,
	), ids.NewAt(
		ids.KindProject,
		now,
		4,
	), ids.NewAt(
		ids.KindEnvironment,
		now,
		5,
	)
	reads := &blueprintPreflightRepository{
		durableEnvironmentBlueprintRepository: repository,
		tenant:                                etcd.TenantRecord{ID: tenantID, Slug: "tenant"},
		project: etcd.ProjectRecord{
			ID:       projectID,
			TenantID: tenantID,
			Slug:     "project",
			Kind:     etcd.ProjectKindTenant,
		},
		environment: etcd.EnvironmentRecord{
			ID:        environmentID,
			ProjectID: projectID,
			Name:      "production",
		},
	}
	service, err := newEnvironmentBlueprintService("/var/lib/groundplane/volumes", "10.40.0.0/16", reads, idempotency,
		blueprintPreflightMaterials{}, groupPlanner, releases, entryGeneration, facts, catalog)
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return now }
	store.readOnly, store.writes = true, 0
	bundle := core.BlueprintBundle{
		RootPath:       "compose.yaml",
		ComposeSources: []string{"compose.yaml"},
		Files: []core.BlueprintFile{{
			Path: "compose.yaml", Content: []byte("kind: environment\nschema: 1\nmetadata: {tenant: tenant, project: project, environment: production}\nx-gp-network-pool: 10.40.0.0/16\nservices:\n  api:\n    image: example.invalid/api:1\n    deploy:\n      replicas: 1\n"),
		}},
	}
	response, err := service.ApplyBlueprint(t.Context(), environmentID, bundle, "", "preflight-no-write")
	if !errors.Is(err, errs.New(errs.KindWorkloadImageResolutionUnavailable, "")) {
		t.Fatalf(
			"ApplyBlueprint error = %v, want image_resolution_unavailable (calls=%d,writes=%d)",
			err,
			resolver.calls,
			store.writes,
		)
	}
	if resolver.calls != 1 || resolver.agentID != agentID || len(resolver.selectors) != 1 ||
		resolver.selectors[0].GetRequestedReference() != "example.invalid/api:1" {
		t.Fatalf("image lookup did not reach expected workload: %+v", resolver)
	}
	if store.writes != 0 || response.Status != 0 {
		t.Fatalf("failed preflight wrote state or returned publication: writes=%d response=%+v", store.writes, response)
	}
}
