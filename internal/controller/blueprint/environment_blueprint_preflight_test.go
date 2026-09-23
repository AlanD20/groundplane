package blueprint

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskmaterialization "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	"github.com/AlanD20/groundplane/internal/controller/componentrender"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/entrygeneration"
	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	testreleasegroup "github.com/AlanD20/groundplane/internal/controller/releasegroup"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	agentregistration "github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	desiredrevisionstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	entryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testlocalagents "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	releasegroupstore "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroup"
	scriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
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
	string, testtaskmaterialization.Source,

) ([]byte, error) {
	return nil, errors.New("unexpected materialization before image preflight")
}

func (blueprintPreflightMaterials) PinSecretValue(
	context.Context,
	string,
	string,
) (testtaskmaterialization.SecretValueReference, error) {
	return testtaskmaterialization.SecretValueReference{}, errors.New("unexpected secret pin before image preflight")
}

func (blueprintPreflightMaterials) RetainComponentFile(
	context.Context, testtaskmaterialization.Record,

	uint64,
	[]byte,
) error {
	return errors.New("unexpected materialization retention before image preflight")
}

// The hierarchy read seam supplies a coherent existing Environment; every other
// read and all claim/staging/publication methods are real persistence wrappers.
type blueprintPreflightRepository struct {
	*durableRepository
	tenant      testhierarchy.TenantRecord
	project     testhierarchy.ProjectRecord
	environment testhierarchy.EnvironmentRecord
}

func (r *blueprintPreflightRepository) GetTenant(
	context.Context,
	string,
) (testkeyvalue.Versioned[testhierarchy.TenantRecord], error) {
	return testkeyvalue.Versioned[testhierarchy.TenantRecord]{Record: r.tenant, Revision: 1, ReadRevision: 1}, nil
}

func (r *blueprintPreflightRepository) GetProject(
	context.Context,
	string,
) (testkeyvalue.Versioned[testhierarchy.ProjectRecord], error) {
	return testkeyvalue.Versioned[testhierarchy.ProjectRecord]{Record: r.project, Revision: 1, ReadRevision: 1}, nil
}

func (r *blueprintPreflightRepository) GetEnvironment(
	context.Context,
	string,
) (testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], error) {
	return testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
		Record:       r.environment,
		Revision:     1,
		ReadRevision: 1,
	}, nil
}

// Rationale: a failed host-local image lookup must leave no durable Blueprint
// claim, staged sources, Release ledger writes, Task, or idempotency publication.
func TestApplyBlueprintImageFailurePrecedesEveryDurableWrite(t *testing.T) {
	store := &blueprintTestStore{values: map[string]testkeyvalue.KeyValue{}, revision: 1}
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
	values, err := entryvalues.New(store)
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
	repository, err := NewRepository(
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
		nil,
		nil,
	)
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
	sources, err := scriptsourcepublication.NewAuthority(store)
	if err != nil {
		t.Fatal(err)
	}
	agents, err := agentregistration.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	var catalog []componentrender.EnvironmentComponentRegistration
	plans, err := testtaskplanning.NewTaskPlanResolver("/var/lib/groundplane/volumes", catalog)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := testtaskplanning.NewScriptArtifactService(scripts, blueprintPreflightMaterials{})
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
	groupPlanner, err := testreleasegroup.NewReleaseGroupBlueprintPlanner(groups, hierarchy.HierarchyRepository)
	if err != nil {
		t.Fatal(err)
	}
	facts, err := attachments.NewFactService(attaches, protector)
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
	_, err = agents.CreateSingleton(t.Context(), testlocalagents.LocalAgentRecord{
		ID: agentID, EnrollmentTaskID: ids.NewAt(ids.KindTask, now, 2), Image: "example.invalid/agent@sha256:" + strings.Repeat("a", 64),
		Generation: 1, Phase: testlocalagents.LocalAgentPhaseProvisioning,
		Config: testlocalagents.LocalAgentConfig{PullIntervalSeconds: 5, MaxConcurrentTasks: 1},
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
		durableRepository: repository,
		tenant:            testhierarchy.TenantRecord{ID: tenantID, Slug: "tenant"},
		project: testhierarchy.ProjectRecord{
			ID:       projectID,
			TenantID: tenantID,
			Slug:     "project",
			Kind:     testhierarchy.ProjectKindTenant,
		},
		environment: testhierarchy.EnvironmentRecord{
			ID:        environmentID,
			ProjectID: projectID,
			Name:      "production",
		},
	}
	service, err := NewService(
		"/var/lib/groundplane/volumes",
		"10.40.0.0/16",
		reads,
		idempotency,
		blueprintPreflightMaterials{},
		groupPlanner,
		releases,
		entryGeneration,
		facts,
		catalog,
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return now }
	store.readOnly, store.writes = true, 0
	bundle := core.BlueprintBundle{
		RootPath:       "compose.yaml",
		ComposeSources: []string{"compose.yaml"},
		Files: []core.BlueprintFile{{
			Path: "compose.yaml", Content: []byte("kind: environment\nschema: 1\nmetadata: {tenant: tenant, project: project, environment: production}\nx-gp-network-pool: 10.40.0.0/16\nservices:\n  api:\n    image: example.invalid/api:1\n    deploy:\n      replicas: 1\nnetworks:\n  default:\n    ipam:\n      config:\n        - subnet: 10.40.10.0/24\n"),
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
