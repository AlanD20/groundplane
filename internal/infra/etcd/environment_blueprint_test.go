package etcd

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/netip"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testblueprintplanning "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintplanning"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testcomponentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleasegroups "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	testroutes "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestEnvironmentBlueprintZonePoolPublishesSealedRevisionAndTaskAuthority(t *testing.T) {
	// Rationale: publication must select an already sealed desired revision and
	// advance the head, Task queue, and Environment mutation epoch atomically.
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, repository)
	task := environmentBlueprintTestTask(t, project.Record, environment.Record, 20)
	revision := environmentBlueprintTestRevision(environment.Record.ID, task, "services: {}\n")
	marker := environmentBlueprintTestMarker(task, environment.Record.ID)

	desiredProjection := environmentBlueprintTestProjection(environment.Record.ID, task, 1)
	desiredProjection.DesiredServices[0].Desired.Strategy = core.StrategyBlueGreen
	desiredProjection.DesiredServices[0].Desired.DependsOn = map[string]core.ServiceDependency{
		"migrate": {
			Condition: core.ServiceDependencyCompletedSuccessfully,
			Phases:    []core.ServiceDependencyPhase{core.ServiceDependencyPhaseDeploy},
		},
	}
	zoneChanges := environmentBlueprintTestZoneChanges(t, repository, desiredProjection)
	serviceChanges := environmentBlueprintTestServiceChanges(t, repository, desiredProjection)
	routeChanges := environmentBlueprintTestRouteChanges(t, repository, desiredProjection)
	result := publishEnvironmentBlueprintTestRevision(
		t, repository, project, environment, 0, revision, desiredProjection,
		zoneChanges, serviceChanges, routeChanges, testcomponentplanning.ComponentTaskPreparation{}, task, marker,
	)
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("publication outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}

	head, found, err := repository.GetEnvironmentBlueprintHead(ctx, environment.Record.ID)
	if err != nil || !found || head.Record.RevisionID != task.ID {
		t.Fatalf("GetEnvironmentBlueprintHead() = %#v, %v, %v", head, found, err)
	}
	stored, found, err := repository.GetEnvironmentBlueprintRevision(ctx, environment.Record.ID, task.ID)
	if err != nil || !found || len(stored.Record.Files) != 1 ||
		string(stored.Record.Files[0].Content) != "services: {}\n" {
		t.Fatalf("GetEnvironmentBlueprintRevision() = %#v, %v, %v", stored, found, err)
	}
	if stored.Revision >= head.Revision {
		t.Fatalf("sealed revision MVCC revision = %d, want before head publication %d", stored.Revision, head.Revision)
	}
	assertEnvironmentBlueprintTopologyAuthority(t, store, desiredProjection)
	projection, found, err := repository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil || !found || projection.Record.RevisionID != task.ID ||
		projection.Record.RenderGeneration != 1 {
		t.Fatalf("GetEnvironmentComposeProjection() = %#v, %v, %v", projection, found, err)
	}
	assertEnvironmentBlueprintTaskAuthority(t, store, environment.Record.ID, task, head.Revision)
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	assertEnvironmentBlueprintValue(t, store, markerKey)
}

type environmentBlueprintPublicationAuditStore struct {
	hierarchyStore
	publicationKey   string
	reject           bool
	comparisons      int
	successMutations int
	failureReads     int
}

// Only final publication is rejected; source staging is not public authority.
func (store *environmentBlueprintPublicationAuditStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if !slices.ContainsFunc(mutations, func(mutation testkeyvalue.Mutation) bool {
		return mutation.Key == store.publicationKey
	}) {
		return store.hierarchyStore.Transact(ctx, conditions, mutations)
	}
	store.comparisons = len(conditions)
	store.successMutations = len(mutations)
	if store.reject {
		store.failureReads = len(conditions)
		return testkeyvalue.TransactionResult{
			Revision: 1, FailureReads: make([]*testkeyvalue.KeyValue, len(conditions)),
		}, nil
	}
	return store.hierarchyStore.Transact(ctx, conditions, mutations)
}

// BP-04: final publication stays atomic and bounded independently of topology size.
func TestEnvironmentBlueprintTopologyPublicationHasConstantCompactShape(t *testing.T) {
	// Rationale: the sealed 6-Zone/13-Service/6-Route topology must publish by
	// Environment head without consuming one transaction operation per resource.
	for _, reject := range []bool{false, true} {
		name := "success"
		if reject {
			name = "failure arm"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newMemoryHierarchyStore()
			base, err := newHierarchyRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			project, environment := createEnvironmentBlueprintOwners(t, base)
			task := environmentBlueprintTestTask(t, project.Record, environment.Record, 13100)
			projection := environmentBlueprintSizedTopologyProjection(t, environment.Record.ID, task.ID)
			zones, services, routes := environmentBlueprintTopologyChanges(t, projection)
			marker := environmentBlueprintTestMarker(task, environment.Record.ID)
			claim := stageEnvironmentBlueprintForPublicationTest(
				t,
				base,
				0,
				environmentBlueprintTestRevision(environment.Record.ID, task, "services: {}\n"),
				projection,
				marker,
			)
			audited := &environmentBlueprintPublicationAuditStore{
				hierarchyStore: store,
				publicationKey: testtaskjournal.TaskStorageKey(task.ID),
				reject:         reject,
			}
			repository, err := newHierarchyRepository(audited)
			if err != nil {
				t.Fatal(err)
			}
			result, err := publishEnvironmentBlueprintClaimTest(
				repository,
				ctx,
				project,
				environment,
				0,
				claim,
				testblueprints.EnvironmentDesiredRevisionIdentity{
					EnvironmentID: environment.Record.ID,
					RevisionID:    task.ID,
				},
				projection,
				zones,
				services,
				routes,
				testreleasegroups.ReleaseGroupBlueprintPreparedMutation{},
				testcomponentplanning.ComponentTaskPreparation{},
				testblueprintplanning.BlueprintAttachTaskPreparation{},
				task,
				marker,
			)
			if err != nil {
				t.Fatalf("PublishEnvironmentDesiredRevisionWithTask() error = %v", err)
			}
			// The configuration-head fence adds one comparison, not one per resource.
			if audited.comparisons != 23 || audited.successMutations != 12 {
				t.Fatalf(
					"publication partitions = %d/%d, want 23/12",
					audited.comparisons,
					audited.successMutations,
				)
			}
			outcome, _, conflict, classifyErr := result.Classify()
			if reject {
				if audited.failureReads != 23 {
					t.Fatalf("failure reads = %d, want 23", audited.failureReads)
				}
				if classifyErr != nil || outcome != IdempotencyKnownConflict || conflict == nil {
					t.Fatalf("rejected publication = %v/%v/%v", outcome, conflict, classifyErr)
				}
				return
			}
			if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
				t.Fatalf("publication = %v/%v/%v", outcome, conflict, classifyErr)
			}
			assertEnvironmentBlueprintTopologyAuthority(t, store, projection)
		})
	}
}

func environmentBlueprintSizedTopologyProjection(
	t *testing.T,
	environmentID string,
	revisionID string,
) testenvironmentprojection.EnvironmentComposeProjection {
	t.Helper()
	projection := desiredTopologyProjectionFixture(t)
	projection.EnvironmentID = environmentID
	projection.RevisionID = revisionID
	for index := range projection.DesiredZones {
		projection.DesiredZones[index].EnvironmentID = environmentID
		projection.DesiredZones[index].Desired.OwnerID = environmentID
		projection.DesiredZones[index].Desired.Subnet = fmt.Sprintf("10.40.%d.0/24", index+10)
	}
	for index := range projection.DesiredServices {
		projection.DesiredServices[index].EnvironmentID = environmentID
	}
	for index := range projection.DesiredRoutes {
		projection.DesiredRoutes[index].EnvironmentID = environmentID
	}
	return withTestEnvironmentComposeArtifact(projection)
}

func environmentBlueprintTopologyChanges(
	t *testing.T,
	projection testenvironmentprojection.EnvironmentComposeProjection,
) (
	[]testblueprints.EnvironmentBlueprintZoneChange,
	[]testblueprints.EnvironmentBlueprintServiceChange,
	[]testblueprints.EnvironmentBlueprintRouteChange,
) {
	t.Helper()
	zones := make([]testblueprints.EnvironmentBlueprintZoneChange, len(projection.DesiredZones))
	for index, desired := range projection.DesiredZones {
		record, err := testzones.NewRecord(projection.EnvironmentID, desired.Desired)
		if err != nil {
			t.Fatalf("NewZoneRecord() error = %v", err)
		}
		zones[index] = testblueprints.EnvironmentBlueprintZoneChange{Record: record}
	}
	services := make([]testblueprints.EnvironmentBlueprintServiceChange, len(projection.DesiredServices))
	for index, desired := range projection.DesiredServices {
		record, err := testservices.NewServiceRecord(
			projection.EnvironmentID,
			desired.Desired,
			desired.BackingNetworkID,
		)
		if err != nil {
			t.Fatalf("NewServiceRecord() error = %v", err)
		}
		services[index] = testblueprints.EnvironmentBlueprintServiceChange{Record: record}
	}
	routes := make([]testblueprints.EnvironmentBlueprintRouteChange, len(projection.DesiredRoutes))
	for index, desired := range projection.DesiredRoutes {
		record, err := testroutes.NewRecord(projection.EnvironmentID, desired.Desired)
		if err != nil {
			t.Fatalf("NewRouteRecord() error = %v", err)
		}
		routes[index] = testblueprints.EnvironmentBlueprintRouteChange{Record: record}
	}
	return zones, services, routes
}

func TestEnvironmentBlueprintPublicationPreservesOldRevisionWhenHeadAdvances(t *testing.T) {
	// Rationale: a queued Task must reconstruct its sealed input after restart
	// even when a later successful apply changes current desired state.
	ctx := context.Background()
	repository, err := newHierarchyRepository(newMemoryHierarchyStore())
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, repository)
	firstTask := environmentBlueprintTestTask(t, project.Record, environment.Record, 30)
	first := environmentBlueprintTestRevision(environment.Record.ID, firstTask, "services: {old: {}}\n")
	firstProjection := environmentBlueprintTestProjection(environment.Record.ID, firstTask, 1)
	firstZones := environmentBlueprintTestZoneChanges(t, repository, firstProjection)
	firstServices := environmentBlueprintTestServiceChanges(t, repository, firstProjection)
	firstRoutes := environmentBlueprintTestRouteChanges(t, repository, firstProjection)
	firstResult := publishEnvironmentBlueprintTestRevision(
		t, repository, project, environment, 0, first, firstProjection,
		firstZones, firstServices, firstRoutes, testcomponentplanning.ComponentTaskPreparation{}, firstTask,
		environmentBlueprintTestMarker(firstTask, environment.Record.ID),
	)
	if outcome, _, conflict, classifyErr := firstResult.Classify(); classifyErr != nil || conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("first publication outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	head, found, err := repository.GetEnvironmentBlueprintHead(ctx, environment.Record.ID)
	if err != nil || !found {
		t.Fatalf("first head = %#v, %v, %v", head, found, err)
	}

	secondTask := environmentBlueprintTestTask(t, project.Record, environment.Record, 40)
	secondTask.RenderGeneration = 2
	second := environmentBlueprintTestRevision(environment.Record.ID, secondTask, "services: {new: {}}\n")
	secondProjection := firstProjection
	secondProjection.RevisionID = secondTask.ID
	secondProjection.RenderGeneration = 2
	secondZones := environmentBlueprintTestZoneChanges(t, repository, secondProjection)
	secondServices := environmentBlueprintTestServiceChanges(t, repository, secondProjection)
	secondRoutes := environmentBlueprintTestRouteChanges(t, repository, secondProjection)
	secondResult := publishEnvironmentBlueprintTestRevision(
		t, repository, project, environment, head.Revision, second, secondProjection,
		secondZones, secondServices, secondRoutes, testcomponentplanning.ComponentTaskPreparation{}, secondTask,
		environmentBlueprintTestMarker(secondTask, environment.Record.ID),
	)
	if outcome, _, conflict, classifyErr := secondResult.Classify(); classifyErr != nil || conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("second publication outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}

	old, found, err := repository.GetEnvironmentBlueprintRevision(ctx, environment.Record.ID, firstTask.ID)
	if err != nil || !found || string(old.Record.Files[0].Content) != "services: {old: {}}\n" {
		t.Fatalf("old immutable revision = %#v, %v, %v", old, found, err)
	}
	current, found, err := repository.GetEnvironmentBlueprintHead(ctx, environment.Record.ID)
	if err != nil || !found || current.Record.RevisionID != secondTask.ID {
		t.Fatalf("current head = %#v, %v, %v", current, found, err)
	}
	assertEnvironmentBlueprintTaskAuthority(t, repository.store, environment.Record.ID, secondTask, current.Revision)
}

func TestEnvironmentBlueprintCompletionPromotesAppliedComposeProjection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, hierarchy)
	task := environmentBlueprintTestTask(t, project.Record, environment.Record, 50)
	desired := environmentBlueprintTestProjection(environment.Record.ID, task, 1)
	publishEnvironmentBlueprintTestRevision(
		t,
		hierarchy,
		project,
		environment,
		0,
		environmentBlueprintTestRevision(environment.Record.ID, task, "services: {}\n"),
		desired,
		environmentBlueprintTestZoneChanges(t, hierarchy, desired),
		environmentBlueprintTestServiceChanges(t, hierarchy, desired),
		environmentBlueprintTestRouteChanges(
			t,
			hierarchy,
			desired,
		),
		testcomponentplanning.ComponentTaskPreparation{},
		task,
		environmentBlueprintTestMarker(task, environment.Record.ID),
	)
	if applied, found, err := hierarchy.GetEnvironmentAppliedComposeProjection(ctx, environment.Record.ID); err != nil ||
		found {
		t.Fatalf("applied projection before acknowledgement = %#v, %v, %v", applied, found, err)
	}

	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	agentID := ids.NewAt(ids.KindAgent, task.CreatedAt, 51)
	assignment, found, err := tasks.ClaimNextTask(ctx, agentID, 1, task.CreatedAt.Add(time.Second))
	if err != nil || !found || assignment.Task.Record.ID != task.ID {
		t.Fatalf("ClaimNextTask() = %#v, %v, %v", assignment, found, err)
	}
	if _, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		1,
		task.ID,
		taskAssignmentIDForTest(t, tasks, task.ID), testtaskjournal.TaskStatusCompleted, completedComposeTaskResult(),
		task.CreatedAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeTask() error = %v", err)
	}
	applied, found, err := hierarchy.GetEnvironmentAppliedComposeProjection(ctx, environment.Record.ID)
	if err != nil || !found || !reflect.DeepEqual(applied.Record, desired) {
		t.Fatalf("applied projection after acknowledgement = %#v, %v, %v", applied, found, err)
	}
}

type environmentBlueprintTestTransactionStore struct {
	hierarchyStore
}

func (store environmentBlueprintTestTransactionStore) TransactEnvironmentBlueprint(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	return store.hierarchyStore.Transact(ctx, conditions, mutations)
}

func publishEnvironmentBlueprintClaimTest(
	repository *HierarchyRepository,
	ctx context.Context,
	project testkeyvalue.Versioned[testhierarchy.ProjectRecord],
	environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord],
	expectedHeadRevision int64,
	claim testblueprints.EnvironmentBlueprintStageClaim,
	revision testblueprints.EnvironmentDesiredRevisionIdentity,
	projection testenvironmentprojection.EnvironmentComposeProjection,
	zoneChanges []testblueprints.EnvironmentBlueprintZoneChange,
	serviceChanges []testblueprints.EnvironmentBlueprintServiceChange,
	routeChanges []testblueprints.EnvironmentBlueprintRouteChange,
	releaseGroupPreparation testreleasegroups.ReleaseGroupBlueprintPreparedMutation,
	componentPreparation testcomponentplanning.ComponentTaskPreparation,
	attachPreparation testblueprintplanning.BlueprintAttachTaskPreparation,
	task TaskRecord,
	marker testidempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	final, err := newEnvironmentBlueprintRepository(
		repository.store,
		environmentBlueprintTestTransactionStore{hierarchyStore: repository.store},
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return final.PublishEnvironmentBlueprintDesiredRevision(
		ctx,
		netip.Prefix{},
		environment.Record.NetworkPool,
		project,
		environment,
		expectedHeadRevision,
		claim,
		revision,
		projection,
		zoneChanges,
		serviceChanges,
		routeChanges,
		releaseGroupPreparation,
		componentPreparation,
		attachPreparation, testblueprintplanning.BlueprintBackupPolicyPreparation{}, BlueprintScriptPublication{},
		BlueprintReleasePublication{},
		BlueprintRequirementGate{},
		task,
		marker,
	)
}

func publishEnvironmentBlueprintTestRevision(
	t *testing.T,
	repository *HierarchyRepository,
	project testkeyvalue.Versioned[testhierarchy.ProjectRecord],
	environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord],
	expectedHeadRevision int64,
	revision testblueprints.EnvironmentBlueprintRevision,
	projection testenvironmentprojection.EnvironmentComposeProjection,
	zoneChanges []testblueprints.EnvironmentBlueprintZoneChange,
	serviceChanges []testblueprints.EnvironmentBlueprintServiceChange,
	routeChanges []testblueprints.EnvironmentBlueprintRouteChange,
	componentPreparation testcomponentplanning.ComponentTaskPreparation,
	task TaskRecord,
	marker testidempotency.IdempotencyMarker,
) IdempotencyTransactionResult {
	t.Helper()
	claim := stageEnvironmentBlueprintForPublicationTest(
		t, repository, expectedHeadRevision, revision, projection, marker,
	)
	result, err := publishEnvironmentBlueprintClaimTest(
		repository,
		context.Background(),
		project,
		environment,
		expectedHeadRevision,
		claim,
		testblueprints.EnvironmentDesiredRevisionIdentity{
			EnvironmentID: revision.EnvironmentID,
			RevisionID:    revision.RevisionID,
		},
		projection,
		zoneChanges,
		serviceChanges,
		routeChanges,
		testreleasegroups.ReleaseGroupBlueprintPreparedMutation{},
		componentPreparation,
		testblueprintplanning.BlueprintAttachTaskPreparation{},
		task,
		marker,
	)
	if err != nil {
		t.Fatalf("PublishEnvironmentDesiredRevisionWithTask() error = %v", err)
	}
	return result
}

func assertEnvironmentBlueprintTaskAuthority(
	t *testing.T,
	store hierarchyStore,
	environmentID string,
	task TaskRecord,
	wantRevision int64,
) {
	t.Helper()
	stored, err := store.Get(context.Background(), testtaskjournal.TaskStorageKey(task.ID))
	if err != nil || stored.Entry == nil || stored.Entry.ModRevision != wantRevision {
		t.Fatalf("Task authority = %#v, %v; want revision %d", stored, err, wantRevision)
	}
	decoded, err := DecodeTaskRecord(stored.Entry.Value)
	if err != nil || decoded.ID != task.ID ||
		decoded.Params[testblueprints.EnvironmentDesiredRevisionParam] != task.ID {
		t.Fatalf("decoded Task authority = %#v, %v", decoded, err)
	}
	queued, err := store.Get(context.Background(), testtaskjournal.TaskQueueKey(task.Executor, task.ID))
	if err != nil || queued.Entry == nil || queued.Entry.ModRevision != wantRevision {
		t.Fatalf("Task queue authority = %#v, %v; want revision %d", queued, err, wantRevision)
	}
	epoch, err := store.Get(context.Background(), testhierarchy.EnvironmentMutationEpochKey(environmentID))
	if err != nil || epoch.Entry == nil || epoch.Entry.ModRevision != wantRevision {
		t.Fatalf("Environment mutation epoch = %#v, %v; want revision %d", epoch, err, wantRevision)
	}
	decodedEpoch, err := testbackupruntime.DecodeEnvironmentMutationEpochRecord(epoch.Entry.Value)
	if err != nil || decodedEpoch.EnvironmentID != environmentID {
		t.Fatalf("decoded Environment mutation epoch = %#v, %v", decodedEpoch, err)
	}
}

func createEnvironmentBlueprintOwners(
	t *testing.T,
	repository *HierarchyRepository,
) (testkeyvalue.Versioned[testhierarchy.ProjectRecord], testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]) {
	t.Helper()
	ctx := context.Background()
	at := time.Date(2026, 8, 22, 18, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, at, 1)
	projectID := ids.NewAt(ids.KindProject, at, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 3)
	if _, err := repository.CreateTenant(ctx, testhierarchy.TenantRecord{ID: tenantID, Slug: "acme", Name: "Acme"}); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project, err := repository.CreateProject(ctx, testhierarchy.ProjectRecord{
		ID: projectID, TenantID: tenantID, Slug: "console", Name: "Console", Kind: testhierarchy.ProjectKindTenant,
	})
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	environment, err := repository.CreateEnvironment(ctx, testhierarchy.EnvironmentRecord{NetworkPool: "10.40.0.0/16",
		ID: environmentID, ProjectID: projectID, Name: "production",
		VolumeDir:         "/var/lib/groundplane/vol/" + tenantID + "/" + projectID + "/" + environmentID,
		ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, at, 4),
		CreatedAt:         at,
	})
	if err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	return project, environment
}

func environmentBlueprintTestTask(
	t *testing.T,
	project testhierarchy.ProjectRecord,
	environment testhierarchy.EnvironmentRecord,
	seed int64,
) TaskRecord {
	t.Helper()
	at := time.Date(2026, 8, 22, 19, 0, int(seed), 0, time.UTC)
	taskID := ids.NewAt(ids.KindTask, at, seed)
	owner, err := testtaskjournal.EnvironmentTaskOwner(project, environment)
	if err != nil {
		t.Fatalf("EnvironmentTaskOwner() error = %v", err)
	}
	return TaskRecord{
		ID: taskID, OperationID: ids.NewAt(ids.KindOperation, at, seed+1),
		Owner: owner, Actor: testtaskjournal.TaskActorOperator,
		IdempotencyKey: ids.NewAt(ids.KindOperation, at, seed+2)[3:],
		Executor:       etcdTaskExecutorAgent(), PlanID: ids.NewAt(ids.KindPlan, at, seed+3),
		PlanHash: strings.Repeat("a", 64), RenderGeneration: 1,
		Type: testtaskjournal.TaskUpdate, Target: environment.ID,
		Params: map[string]string{
			testblueprints.EnvironmentDesiredRevisionParam:      taskID,
			testtaskjournal.TaskMaterializationEnvironmentParam: environment.ID,
		},
		Steps: []testtaskjournal.TaskStepRecord{
			{Kind: testtaskjournal.TaskStepOperation, ID: ids.NewAt(ids.KindStep, at, seed+4)},
		},
		TimeoutSeconds: 120, Status: testtaskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: at, UpdatedAt: at,
	}
}

func etcdTaskExecutorAgent() testtaskjournal.TaskExecutor { return testtaskjournal.TaskExecutorAgent }

func environmentBlueprintTestRevision(
	environmentID string,
	task TaskRecord,
	content string,
) testblueprints.EnvironmentBlueprintRevision {
	return testblueprints.EnvironmentBlueprintRevision{
		EnvironmentID: environmentID, RevisionID: task.ID,
		RootPath: "blueprint.yaml", ComposeSources: []string{"blueprint.yaml"},
		Interpolation: map[string]string{"TAG": "v1"},
		Files:         []testblueprints.EnvironmentBlueprintFile{{Path: "blueprint.yaml", Content: []byte(content)}},
		CreatedAt:     task.CreatedAt,
	}
}

func environmentBlueprintTestProjection(
	environmentID string,
	task TaskRecord,
	generation uint64,
) testenvironmentprojection.EnvironmentComposeProjection {
	serviceID := ids.NewAt(ids.KindService, task.CreatedAt, 70)
	zoneID := ids.NewAt(ids.KindNetwork, task.CreatedAt, 71)
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: task.ID, RenderGeneration: generation,
		DesiredServices: []testservices.EnvironmentServiceProjection{{
			EnvironmentID: environmentID,
			Desired: core.Service{
				ID: serviceID, Name: "api", Image: "example/api:1", Zones: []string{"default"},
			},
		}},
		DesiredZones: []testenvironmentprojection.EnvironmentZoneProjection{{
			EnvironmentID: environmentID,
			Desired: core.Zone{
				ID: zoneID, Name: "default", Subnet: "10.40.10.0/24", Internal: true,
				OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
			},
		}},
		DesiredRoutes: []testenvironmentprojection.EnvironmentRouteProjection{{
			EnvironmentID: environmentID,
			Desired: core.Route{
				ID: ids.NewAt(ids.KindRoute, task.CreatedAt, 74), Path: "/internal/*",
				TargetServiceID: serviceID, TargetPort: 8080, Exposure: "internal",
			},
			DesiredGeneration: 1,
		}},
		Volumes: []testenvironmentprojection.EnvironmentVolumeIdentity{{
			ID: ids.NewAt(ids.KindVolume, task.CreatedAt, 72), Slug: "app-data", Key: "app-data",
		}},
	}
	canonicalYAML := []byte("services: {}\n")
	yamlDigest := sha256.Sum256(canonicalYAML)
	artifact := &agentpb.ComposeArtifact{
		ArtifactId:          ids.NewAt(ids.KindConfig, task.CreatedAt, 73),
		OwnerKind:           agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:             environmentID,
		ProjectName:         "groundplane-test",
		CanonicalYaml:       canonicalYAML,
		YamlSha256:          yamlDigest[:],
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/test",
		Services: []*agentpb.ComposeService{{
			ServiceId:   projection.DesiredServices[0].Desired.ID,
			ComposeName: projection.DesiredServices[0].Desired.Name,
		}},
		Volumes: []*agentpb.ComposeVolume{{
			VolumeId: projection.Volumes[0].ID, ComposeName: projection.Volumes[0].Key,
		}},
	}
	value, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		panic(err)
	}
	projection.ComposeArtifact = value
	projection.NormalizedCompose = append([]byte(nil), canonicalYAML...)
	return projection
}

func environmentBlueprintTestMarker(task TaskRecord, environmentID string) testidempotency.IdempotencyMarker {
	marker := pendingTaskMarker(task)
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: http.MethodPut, Route: "/environments/{id}/blueprint", Key: task.IdempotencyKey,
	}
	return marker
}

func environmentBlueprintTestServiceChanges(
	t *testing.T,
	repository *HierarchyRepository,
	projection testenvironmentprojection.EnvironmentComposeProjection,
) []testblueprints.EnvironmentBlueprintServiceChange {
	t.Helper()
	services, err := newServiceRepository(repository.store)
	if err != nil {
		t.Fatalf("newServiceRepository() error = %v", err)
	}
	changes := make([]testblueprints.EnvironmentBlueprintServiceChange, len(projection.DesiredServices))
	for index, desired := range projection.DesiredServices {
		current, err := services.GetService(context.Background(), desired.Desired.ID)
		if err == nil {
			replacement, replaceErr := testservices.ReplaceServiceDesired(current.Record, desired.Desired)
			if replaceErr != nil {
				t.Fatalf("ReplaceServiceDesired() error = %v", replaceErr)
			}
			currentCopy := current
			changes[index] = testblueprints.EnvironmentBlueprintServiceChange{
				Current: &currentCopy,
				Record:  replacement,
			}
			continue
		}
		if !isKind(err, errs.KindServiceNotFound) {
			t.Fatalf("GetService() error = %v", err)
		}
		record, recordErr := testservices.NewServiceRecord(
			projection.EnvironmentID,
			desired.Desired,
			desired.BackingNetworkID,
		)
		if recordErr != nil {
			t.Fatalf("NewServiceRecord() error = %v", recordErr)
		}
		changes[index] = testblueprints.EnvironmentBlueprintServiceChange{Record: record}
	}
	return changes
}

func environmentBlueprintTestRouteChanges(
	t *testing.T,
	repository *HierarchyRepository,
	projection testenvironmentprojection.EnvironmentComposeProjection,
) []testblueprints.EnvironmentBlueprintRouteChange {
	t.Helper()
	routes, err := newRouteRepository(repository.store)
	if err != nil {
		t.Fatalf("newRouteRepository() error = %v", err)
	}
	changes := make([]testblueprints.EnvironmentBlueprintRouteChange, len(projection.DesiredRoutes))
	for index, desired := range projection.DesiredRoutes {
		current, err := routes.GetRoute(context.Background(), desired.Desired.ID)
		if err == nil {
			replacement, replaceErr := testroutes.ReplaceDesired(current.Record, desired.Desired)
			if replaceErr != nil {
				t.Fatalf("ReplaceRouteDesired() error = %v", replaceErr)
			}
			currentCopy := current
			changes[index] = testblueprints.EnvironmentBlueprintRouteChange{Current: &currentCopy, Record: replacement}
			continue
		}
		if !isKind(err, errs.KindRouteNotFound) {
			t.Fatalf("GetRoute() error = %v", err)
		}
		record, recordErr := testroutes.NewRecord(projection.EnvironmentID, desired.Desired)
		if recordErr != nil {
			t.Fatalf("NewRouteRecord() error = %v", recordErr)
		}
		changes[index] = testblueprints.EnvironmentBlueprintRouteChange{Record: record}
	}
	return changes
}

func assertEnvironmentBlueprintValue(t *testing.T, store *memoryHierarchyStore, key string) {
	t.Helper()
	result, err := store.Get(context.Background(), key)
	if err != nil || result.Entry == nil {
		t.Fatalf("durable key %q = %#v, %v", key, result, err)
	}
}
