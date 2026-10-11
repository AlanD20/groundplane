package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testroutes "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: a durable provider pin must retain the provider-neutral origin
// exactly and reject any origin that no longer names its managed Service.
func TestRouteProviderPinClonePreservesCanonicalOrigin(t *testing.T) {
	t.Parallel()
	componentID := ids.New(ids.KindComponent)
	serviceID := ids.New(ids.KindService)
	pinned := testenvironmentchanges.RouteProviderPin{
		ComponentID: componentID, DefinitionDigest: strings.Repeat("a", 64),
		CatalogDigest: strings.Repeat("b", 64), InputRevision: 7, InputGeneration: 9,
		Destination: "components/router/config", ActionID: "activate-config", ServiceID: serviceID,
		Input: componentsdk.HTTPRouterInput{
			ComponentID: componentID, Enabled: true, GeneratedServiceID: serviceID,
			Zones: []componentsdk.HTTPRouterZoneInput{{
				ID: ids.New(ids.KindNetwork), Name: "frontend", StaticIPv4: "10.40.0.2",
			}},
			Origin: componentsdk.HTTPRouterOrigin{ServiceName: "edge-router", URL: "http://edge-router:8080"},
			Routes: []componentsdk.HTTPRoute{{
				ID: "rte_one", Host: "app.example.com", Path: "/", BackendServiceID: "svc_backend",
				BackendServiceName: "backend", TargetPort: 8080,
				Exposure: componentsdk.HTTPRouteExposurePublic,
			}},
		},
	}
	if err := testenvironmentchanges.ValidateRouteProviderPin(&pinned); err != nil {
		t.Fatalf("validateRouteProviderPin() error = %v", err)
	}
	cloned := testenvironmentchanges.CloneRouteProviderPin(pinned)
	cloned.Input.Routes[0].Path = "/changed"
	cloned.Input.Zones[0].Name = "changed"
	if pinned.Input.Routes[0].Path != "/" || pinned.Input.Zones[0].Name != "frontend" ||
		cloned.Input.Origin != pinned.Input.Origin {
		t.Fatalf("cloneRouteProviderPin() source/clone = %#v / %#v", pinned, cloned)
	}
	changed := pinned
	changed.Input.Origin.URL = "http://another-router:8080"
	if err := testenvironmentchanges.ValidateRouteProviderPin(&changed); err == nil {
		t.Fatal("validateRouteProviderPin() accepted a changed origin host")
	}
}

func TestRouteRepositoryRejectsPublicationOmittingDesiredService(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project, target := routeRepositoryTestHierarchy(t)
	fenceKey := "/v1/test/route-service-fences/" + target.Record.Desired.ID
	fence, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: fenceKey, Value: []byte(target.Record.Desired.ID),
	}})
	if err != nil || !fence.Succeeded {
		t.Fatalf("seed Route service fence = %#v, %v", fence, err)
	}
	target = refenceServiceFixture(t, store, target, fenceKey, fence.Revision)
	target.Revision = fence.Revision
	target.ReadRevision = fence.Revision

	currentProjection, found, err := testblueprints.ReadProjectionAtRevision(
		ctx, store, environment.Record.ID, fence.Revision,
	)
	if err != nil || !found {
		t.Fatalf("currentEnvironmentProjectionAtRevision() = %#v/%v/%v", currentProjection, found, err)
	}
	headBefore, err := store.Get(ctx, testblueprints.EnvironmentBlueprintHeadKey(environment.Record.ID))
	if err != nil || headBefore.Entry == nil {
		t.Fatalf("Get(desired head before) = %#v/%v", headBefore, err)
	}

	createdAt := serviceRecordTestTime().Add(4 * time.Hour)
	record := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1300, "/unsafe/*")
	task := validTaskRecord(createdAt)
	task.ID = ids.NewAt(ids.KindTask, createdAt, 1301)
	task.OperationID = ids.NewAt(ids.KindOperation, createdAt, 1302)
	task.PlanID = ids.NewAt(ids.KindPlan, createdAt, 1303)
	task.Owner = mustEnvironmentTaskOwner(t, project.Record, environment.Record)
	task.Executor = testtaskjournal.TaskExecutorController
	task.Type = testtaskjournal.TaskCreate
	task.Target = record.Desired.ID
	task.Status = testtaskjournal.TaskStatusPending
	task.Params = map[string]string{
		testtaskjournal.TaskResourceKindParam:     testtaskjournal.TaskResourceRoute,
		testtaskjournal.TaskRouteEnvironmentParam: environment.Record.ID,
	}
	task.Steps = []testtaskjournal.TaskStepRecord{
		{Kind: testtaskjournal.TaskStepOperation, ID: ids.NewAt(ids.KindStep, createdAt, 1304)},
	}
	task.TimeoutSeconds = 30
	task.RenderGeneration = int32(currentProjection.Record.RenderGeneration + 1)
	task.PlanHash = strings.Repeat("c", 64)
	task.IdempotencyKey = "route-omission-task-key-0001"

	intent, err := testenvironmentchanges.NewRouteMutationIntent(
		task.ID, task.OperationID, environment.Record.ID, record, nil, &currentProjection, createdAt,
	)
	if err != nil {
		t.Fatalf("NewRouteMutationIntent() error = %v", err)
	}
	candidate, err := testenvironmentprojection.ApplyEnvironmentRoute(currentProjection.Record, record)
	if err != nil {
		t.Fatalf("ApplyEnvironmentRoute() error = %v", err)
	}
	candidate.RevisionID = task.ID
	candidate.DesiredServices = nil
	candidate = withTestEnvironmentComposeArtifact(candidate)
	intent.CandidateProjection = &candidate
	intent.Provider = routeRepositoryTestProviderPin(environment.Record.ID, target.Record.Desired.ID)
	sealRouteRepositoryTestExecutionPlan(t, &task, &intent.Plan, intent.Provider, false)

	body, err := json.Marshal(struct {
		Route struct {
			ID string `json:"id"`
		} `json:"route"`
		TaskID string `json:"task_id"`
	}{Route: struct {
		ID string `json:"id"`
	}{record.Desired.ID}, TaskID: task.ID})
	if err != nil {
		t.Fatalf("marshal acceptance: %v", err)
	}
	marker, err := testidempotency.NewCompletedDirectIdempotencyMarker(testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: environment.Record.ID,
		Method: http.MethodPost, Route: "/routes", Key: task.IdempotencyKey,
	}, testDirectMarker().Intent, testidempotency.IdempotencyResponse{Status: http.StatusAccepted, ContentKind: "application/json", Body: body}, createdAt,
	)
	if err != nil {
		t.Fatalf("NewCompletedDirectIdempotencyMarker() error = %v", err)
	}

	_, mutationErr := repository.BeginRouteMutationWithTask(
		ctx, environment, project, target, nil, record, intent, task, marker,
	)
	if kind, ok := errs.KindOf(mutationErr); !ok || kind != errs.KindStateConflict {
		t.Errorf(
			"BeginRouteMutationWithTask() error kind = %v/%v, want %v (error %v)",
			kind,
			ok,
			errs.KindStateConflict,
			mutationErr,
		)
	}
	headAfter, err := store.Get(ctx, testblueprints.EnvironmentBlueprintHeadKey(environment.Record.ID))
	if err != nil || headAfter.Entry == nil {
		t.Fatalf("Get(desired head after) = %#v/%v", headAfter, err)
	}
	if headAfter.Entry.ModRevision != headBefore.Entry.ModRevision ||
		string(headAfter.Entry.Value) != string(headBefore.Entry.Value) {
		t.Errorf("desired head changed: before=%#v after=%#v", headBefore.Entry, headAfter.Entry)
	}
}

func TestRouteRepositoryPublishesDesiredMutationAndTaskAtomically(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project, target := routeRepositoryTestHierarchy(t)
	record := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1200, "/api/*")
	fenceKey := "/v1/test/route-service-fences/" + target.Record.Desired.ID
	fence, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: fenceKey, Value: []byte(target.Record.Desired.ID),
	}})
	if err != nil || !fence.Succeeded {
		t.Fatalf("seed Route service fence = %#v, %v", fence, err)
	}
	target = refenceServiceFixture(t, store, target, fenceKey, fence.Revision)
	target.Revision = fence.Revision
	target.ReadRevision = fence.Revision
	createdAt := serviceRecordTestTime().Add(3 * time.Hour)
	task := validTaskRecord(createdAt)
	task.ID = ids.NewAt(ids.KindTask, createdAt, 1201)
	task.OperationID = ids.NewAt(ids.KindOperation, createdAt, 1202)
	task.PlanID = ids.NewAt(ids.KindPlan, createdAt, 1203)
	task.Owner = mustEnvironmentTaskOwner(t, project.Record, environment.Record)
	task.Executor = testtaskjournal.TaskExecutorController
	task.Type = testtaskjournal.TaskCreate
	task.Target = record.Desired.ID
	task.Status = testtaskjournal.TaskStatusPending
	task.Params = map[string]string{
		testtaskjournal.TaskResourceKindParam:     testtaskjournal.TaskResourceRoute,
		testtaskjournal.TaskRouteEnvironmentParam: environment.Record.ID,
	}
	task.Steps = []testtaskjournal.TaskStepRecord{
		{Kind: testtaskjournal.TaskStepOperation, ID: ids.NewAt(ids.KindStep, createdAt, 1204)},
	}
	task.TimeoutSeconds = 30
	task.RenderGeneration = 1
	task.PlanHash = strings.Repeat("a", 64)
	task.IdempotencyKey = "route-create-task-key-0001"
	intent, err := testenvironmentchanges.NewRouteMutationIntent(
		task.ID,
		task.OperationID,
		environment.Record.ID,
		record,
		nil,
		nil,
		createdAt,
	)
	if err != nil {
		t.Fatalf("NewRouteMutationIntent() error = %v", err)
	}
	body, err := json.Marshal(struct {
		Route struct {
			ID string `json:"id"`
		} `json:"route"`
		TaskID string `json:"task_id"`
	}{Route: struct {
		ID string `json:"id"`
	}{record.Desired.ID}, TaskID: task.ID})
	if err != nil {
		t.Fatalf("marshal acceptance: %v", err)
	}
	marker, err := testidempotency.NewCompletedDirectIdempotencyMarker(testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment,
		ScopeID:   environment.Record.ID,
		Method:    http.MethodPost,
		Route:     "/routes",
		Key:       task.IdempotencyKey,
	}, testDirectMarker().Intent, testidempotency.IdempotencyResponse{Status: http.StatusAccepted, ContentKind: "application/json", Body: body}, createdAt,
	)
	if err != nil {
		t.Fatalf("NewCompletedDirectIdempotencyMarker() error = %v", err)
	}
	result, err := repository.BeginRouteMutationWithTask(
		ctx,
		environment,
		project,
		target,
		nil,
		record,
		intent,
		task,
		marker,
	)
	if err != nil {
		t.Fatalf("BeginRouteMutationWithTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("mutation outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	stored, err := repository.GetRoute(ctx, record.Desired.ID)
	if err != nil || stored.Record != record {
		t.Fatalf("GetRoute() = %#v, %v", stored, err)
	}
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	storedIntent, found, err := hierarchy.GetRouteMutationIntent(ctx, task.ID)
	if err != nil || !found || storedIntent.Record.TaskID != task.ID {
		t.Fatalf("GetRouteMutationIntent() = %#v/%v/%v", storedIntent, found, err)
	}
	companions, err := store.GetMany(
		ctx, testkeyvalue.GetManyRequest{
			Keys: []string{
				testtaskjournal.TaskStorageKey(task.ID),
				testtaskjournal.TaskQueueKey(task.Executor, task.ID),
				testenvironmentchanges.RouteMutationIntentKey(task.ID),
				testenvironmentchanges.ComponentTaskActiveEnvironmentKey(environment.Record.ID),
			},
		},
	)
	if err != nil || companions == nil || len(companions.Values) != 4 {
		t.Fatalf("GetMany(Task companions) = %#v, %v", companions, err)
	}
	for index, value := range companions.Values {
		if value == nil {
			t.Fatalf("Task companion %d is missing", index)
		}
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	if _, found, err := tasks.ClaimNextControllerTask(ctx, createdAt.Add(time.Second)); err != nil || !found {
		t.Fatalf("ClaimNextControllerTask() found/error = %v/%v", found, err)
	}
	terminalAt := createdAt.Add(2 * time.Second)
	if _, err := tasks.AcknowledgeControllerTask(ctx, task.ID, testtaskjournal.TaskStatusCompleted, nil, terminalAt); err != nil {
		t.Fatalf("AcknowledgeControllerTask() error = %v", err)
	}
	observationRead, err := store.Get(ctx, testroutes.ObservationKey(record.Desired.ID))
	if err != nil || observationRead.Entry == nil {
		t.Fatalf("Get(desired-only observation) = %#v/%v", observationRead, err)
	}
	observation, err := testroutes.DecodeObservation(observationRead.Entry.Value)
	if err != nil || observation.Observation.Status != testroutes.ObservedUnserved {
		t.Fatalf("desired-only observation = %#v/%v", observation, err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, task.ID, testtaskjournal.TaskStatusCompleted, nil, terminalAt); err != nil {
		t.Fatalf("AcknowledgeControllerTask(replay) error = %v", err)
	}

	current, err := repository.GetRoute(ctx, record.Desired.ID)
	if err != nil {
		t.Fatalf("GetRoute(edit predecessor) error = %v", err)
	}
	edited, err := testroutes.ReplaceDesired(current.Record, core.Route{
		ID: record.Desired.ID, Host: record.Desired.Host, Path: record.Desired.Path,
		Exposure: "internal", TargetServiceID: record.Desired.TargetServiceID,
		TargetPort: record.Desired.TargetPort,
	})
	if err != nil {
		t.Fatalf("ReplaceRouteDesired() error = %v", err)
	}
	editCreatedAt := terminalAt.Add(time.Second)
	editTask := validTaskRecord(editCreatedAt)
	editTask.ID = ids.NewAt(ids.KindTask, editCreatedAt, 1210)
	editTask.OperationID = ids.NewAt(ids.KindOperation, editCreatedAt, 1211)
	editTask.PlanID = ids.NewAt(ids.KindPlan, editCreatedAt, 1212)
	editTask.Owner = task.Owner
	editTask.Executor = testtaskjournal.TaskExecutorController
	editTask.Type = testtaskjournal.TaskUpdate
	editTask.Target = edited.Desired.ID
	editTask.Params = map[string]string{
		testtaskjournal.TaskResourceKindParam:     testtaskjournal.TaskResourceRoute,
		testtaskjournal.TaskRouteEnvironmentParam: environment.Record.ID,
	}
	editTask.Steps = []testtaskjournal.TaskStepRecord{
		{Kind: testtaskjournal.TaskStepOperation, ID: ids.NewAt(ids.KindStep, editCreatedAt, 1213)},
	}
	editTask.TimeoutSeconds = 30
	editTask.RenderGeneration = int32(edited.DesiredGeneration)
	editTask.PlanHash = strings.Repeat("b", 64)
	editTask.IdempotencyKey = "route-edit-task-key-0001"
	editIntent, err := testenvironmentchanges.NewRouteMutationIntent(
		editTask.ID, editTask.OperationID, environment.Record.ID, edited, &current, nil, editCreatedAt,
	)
	if err != nil {
		t.Fatalf("NewRouteMutationIntent(edit) error = %v", err)
	}
	editBody, err := json.Marshal(struct {
		Route struct {
			ID string `json:"id"`
		} `json:"route"`
		TaskID string `json:"task_id"`
	}{Route: struct {
		ID string `json:"id"`
	}{edited.Desired.ID}, TaskID: editTask.ID})
	if err != nil {
		t.Fatalf("marshal edit acceptance: %v", err)
	}
	editMarker, err := testidempotency.NewCompletedDirectIdempotencyMarker(testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: environment.Record.ID,
		Method: http.MethodPatch, Route: "/routes/{id}", Key: editTask.IdempotencyKey,
	}, testDirectMarker().Intent, testidempotency.IdempotencyResponse{Status: http.StatusAccepted, ContentKind: "application/json", Body: editBody}, editCreatedAt,
	)
	if err != nil {
		t.Fatalf("NewCompletedDirectIdempotencyMarker(edit) error = %v", err)
	}
	if _, err := repository.BeginRouteMutationWithTask(
		ctx, environment, project, target, &current, edited, editIntent, editTask, editMarker,
	); err != nil {
		t.Fatalf("BeginRouteMutationWithTask(edit) error = %v", err)
	}
	if _, found, err := tasks.ClaimNextControllerTask(ctx, editCreatedAt.Add(time.Second)); err != nil || !found {
		t.Fatalf("ClaimNextControllerTask(edit) found/error = %v/%v", found, err)
	}
	editTerminalAt := editCreatedAt.Add(2 * time.Second)
	failed, err := tasks.AcknowledgeControllerTask(
		ctx,
		editTask.ID,
		testtaskjournal.TaskStatusFailed,
		nil,
		editTerminalAt,
	)
	if err != nil || failed.Record.Status != testtaskjournal.TaskStatusFailed {
		t.Fatalf("AcknowledgeControllerTask(edit failed) = %#v/%v", failed, err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, editTask.ID, testtaskjournal.TaskStatusFailed, nil, editTerminalAt); err != nil {
		t.Fatalf("AcknowledgeControllerTask(edit failed replay) error = %v", err)
	}
	retryID := ids.NewAt(ids.KindTask, editTerminalAt.Add(time.Second), 1214)
	retryMarker := pendingRetryMarker(
		failed.Record, retryID, editTerminalAt.Add(time.Second), "route-edit-retry-key-0001",
	)
	result, err = tasks.RetryTask(ctx, editTask.ID, retryID, testtaskjournal.TaskActorOperator, retryMarker)
	if err != nil {
		t.Fatalf("RetryTask(edit) error = %v", err)
	}
	if outcome, _, conflict, classifyErr := result.Classify(); classifyErr != nil || conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("RetryTask(edit) outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	if _, found, err := tasks.ClaimNextControllerTask(ctx, editTerminalAt.Add(2*time.Second)); err != nil || !found {
		t.Fatalf("ClaimNextControllerTask(edit retry) found/error = %v/%v", found, err)
	}
	retryTerminalAt := editTerminalAt.Add(3 * time.Second)
	if _, err := tasks.AcknowledgeControllerTask(ctx, retryID, testtaskjournal.TaskStatusCompleted, nil, retryTerminalAt); err != nil {
		t.Fatalf("AcknowledgeControllerTask(edit retry) error = %v", err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, retryID, testtaskjournal.TaskStatusCompleted, nil, retryTerminalAt); err != nil {
		t.Fatalf("AcknowledgeControllerTask(edit retry replay) error = %v", err)
	}
}

func routeRepositoryTestProviderPin(environmentID, serviceID string) *testenvironmentchanges.RouteProviderPin {
	componentID := ids.New(ids.KindComponent)
	return &testenvironmentchanges.RouteProviderPin{
		ComponentID: componentID, DefinitionDigest: strings.Repeat("a", 64),
		CatalogDigest: strings.Repeat("b", 64), InputRevision: 7, InputGeneration: 9,
		Destination: "components/router/config", ActionID: "activate-config", ServiceID: serviceID,
		Input: componentsdk.HTTPRouterInput{
			ComponentID: componentID, Enabled: true, GeneratedServiceID: serviceID,
			Zones: []componentsdk.HTTPRouterZoneInput{{
				ID: ids.New(ids.KindNetwork), Name: "frontend", StaticIPv4: "10.40.0.2",
			}},
			Origin: componentsdk.HTTPRouterOrigin{ServiceName: "edge-router", URL: "http://edge-router:8080"},
			Routes: []componentsdk.HTTPRoute{{
				ID: "rte_one", Host: "app.example.com", Path: "/", BackendServiceID: "svc_backend",
				BackendServiceName: "backend", TargetPort: 8080,
				Exposure: componentsdk.HTTPRouteExposurePublic,
			}},
		},
		Runtime: &testenvironmentchanges.RouteProviderRuntime{EnvironmentID: environmentID},
	}
}

func sealRouteRepositoryTestExecutionPlan(
	t *testing.T,
	task *TaskRecord,
	encoded *[]byte,
	provider *testenvironmentchanges.RouteProviderPin,
	removal bool,
) {
	t.Helper()
	if task == nil || provider == nil {
		t.Fatal("Route execution authority fixture is incomplete")
	}
	artifactID := task.Params[testtaskjournal.TaskComposeArtifactParam]
	if artifactID == "" {
		artifactID = ids.New(ids.KindConfig)
	}
	materializationID := ids.New(ids.KindConfig)
	destination := provider.Destination
	content := []byte("exact generated route file\n")
	contentDigest := sha256.Sum256(content)
	contentLength := uint64(len(content))
	if len(task.Materializations) != 0 {
		materializationID = task.Materializations[0].MaterializationID
		destination = task.Materializations[0].Destination
		contentLength = task.Materializations[0].Length
		decoded, err := hex.DecodeString(task.Materializations[0].SHA256)
		if err != nil || len(decoded) != sha256.Size {
			t.Fatalf("decode Route materialization digest: %v", err)
		}
		copy(contentDigest[:], decoded)
	}
	materializeStepID := ids.New(ids.KindStep)
	if len(task.Steps) != 0 {
		materializeStepID = task.Steps[0].ID
	}
	applyStepID := ids.New(ids.KindStep)
	activateStepID := ids.New(ids.KindStep)
	fileProbeStepID := ids.New(ids.KindStep)
	fileCompensateStepID := ids.New(ids.KindStep)
	runtimeProbeStepID := ids.New(ids.KindStep)
	runtimeCompensateStepID := ids.New(ids.KindStep)
	definitionDigest, _ := hex.DecodeString(provider.DefinitionDigest)
	catalogDigest, _ := hex.DecodeString(provider.CatalogDigest)
	imageIndexDigest, _ := hex.DecodeString(strings.Repeat("11", sha256.Size))
	imageChildDigest, _ := hex.DecodeString(strings.Repeat("22", sha256.Size))
	imageConfigDigest, _ := hex.DecodeString(strings.Repeat("33", sha256.Size))
	yaml := []byte("services:\n  edge-router:\n    image: example/router@sha256:" +
		hex.EncodeToString(imageChildDigest) + "\n")
	yamlDigest := sha256.Sum256(yaml)
	generation := uint64(task.RenderGeneration)
	service := &agentpb.ComposeService{
		ServiceId: provider.ServiceID, ComposeName: "edge-router", ExpectedReplicas: 1,
		OwnerComponentId: provider.ComponentID,
		ImageRepository:  "example/router",
		ImageReference:   "example/router@sha256:" + hex.EncodeToString(imageChildDigest),
		ImageIndexDigest: imageIndexDigest, ImageChildDigest: imageChildDigest,
		ImageConfigDigest: imageConfigDigest, ImageOs: "linux", ImageArchitecture: "amd64",
		ExpectedLabels: []*agentpb.LabelPair{
			{Key: "com.groundplane.component-id", Value: provider.ComponentID},
			{Key: "com.groundplane.environment-id", Value: task.Owner.EnvironmentID},
			{Key: "com.groundplane.image-child-digest", Value: "sha256:" + hex.EncodeToString(imageChildDigest)},
			{Key: "com.groundplane.image-config-digest", Value: "sha256:" + hex.EncodeToString(imageConfigDigest)},
			{Key: "com.groundplane.image-index-digest", Value: "sha256:" + hex.EncodeToString(imageIndexDigest)},
			{Key: "com.groundplane.image-platform", Value: "linux/amd64"},
			{Key: "com.groundplane.kind", Value: "service"},
			{Key: "com.groundplane.managed", Value: "true"},
			{Key: "com.groundplane.plan-id", Value: task.PlanID},
			{Key: "com.groundplane.render-generation", Value: strconv.FormatUint(generation, 10)},
			{Key: "com.groundplane.service-id", Value: provider.ServiceID},
		},
	}
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: task.Owner.EnvironmentID, ProjectName: "gp-" + strings.ToLower(task.Owner.EnvironmentID),
		AuthorizedVolumeDir: "/var/lib/groundplane/volumes/" + task.Owner.EnvironmentID,
		CanonicalYaml:       yaml, YamlSha256: yamlDigest[:], Services: []*agentpb.ComposeService{service},
	}
	forwardFile := &agentpb.MaterializeFile{
		ArtifactId: artifactID, MaterializationId: materializationID,
		EnvironmentId: task.Owner.EnvironmentID, Destination: destination,
		OutputKind: agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE,
		Mode:       0o444, Length: contentLength, Sha256: contentDigest[:],
	}
	emptyDigest := sha256.Sum256(nil)
	recoveryFile := &agentpb.MaterializeFile{
		ArtifactId: artifactID, MaterializationId: ids.New(ids.KindConfig),
		EnvironmentId: task.Owner.EnvironmentID, Destination: destination,
		OutputKind: agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_PLAIN_FILE,
		Mode:       0o444, Sha256: emptyDigest[:],
	}
	steps := []*agentpb.ExecutionStep{
		{StepId: materializeStepID, TimeoutSeconds: 120,
			Policy:  agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
			Payload: &agentpb.ExecutionStep_MaterializeFile{MaterializeFile: forwardFile}},
		{StepId: applyStepID, TimeoutSeconds: 120, PrerequisiteStepId: materializeStepID,
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
			Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
				ArtifactId: artifactID, ServiceIds: []string{provider.ServiceID}, NoDependencies: true,
			}}},
		{StepId: activateStepID, TimeoutSeconds: 120, PrerequisiteStepId: applyStepID,
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
			Payload: &agentpb.ExecutionStep_ComponentApply{ComponentApply: &agentpb.ComponentApply{
				ComponentId: provider.ComponentID, DefinitionDigest: definitionDigest,
				CatalogDigest: catalogDigest, ActionId: provider.ActionID,
				ArtifactId: materializationID, ArtifactDigest: contentDigest[:], Generation: generation,
			}}},
		{StepId: fileProbeStepID, TimeoutSeconds: 120,
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
			Payload: &agentpb.ExecutionStep_MaterializeFile{
				MaterializeFile: proto.Clone(recoveryFile).(*agentpb.MaterializeFile),
			}},
		{StepId: fileCompensateStepID, TimeoutSeconds: 120, PrerequisiteStepId: materializeStepID,
			Policy:  agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
			Payload: &agentpb.ExecutionStep_MaterializeFile{MaterializeFile: recoveryFile}},
		{StepId: runtimeProbeStepID, TimeoutSeconds: 120,
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
			Payload: &agentpb.ExecutionStep_ComponentRestorationProbe{
				ComponentRestorationProbe: &agentpb.ComponentRestorationProbe{
					ComponentId: provider.ComponentID, CandidateArtifactId: artifactID,
				},
			}},
		{StepId: runtimeCompensateStepID, TimeoutSeconds: 120,
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
			Payload: &agentpb.ExecutionStep_ComponentRestorationCompensate{
				ComponentRestorationCompensate: &agentpb.ComponentRestorationCompensate{
					ComponentId: provider.ComponentID, CandidateArtifactId: artifactID,
				},
			}},
	}
	steps[3].GetMaterializeFile().MaterializationId = ids.New(ids.KindConfig)
	operation := agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
	if removal {
		operation = agentpb.PlanOperation_PLAN_OPERATION_REMOVE
	}
	sealed, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: task.PlanID, RenderGeneration: generation,
		Operation: operation, TargetId: task.Target, Artifacts: []*agentpb.ComposeArtifact{artifact}, Steps: steps,
		ComponentRecoveryProcedure: &agentpb.ComponentRecoveryProcedure{
			ComponentId: provider.ComponentID, CandidateArtifactId: artifactID,
			ForwardStepIds: []string{materializeStepID, applyStepID, activateStepID},
			ConfigurationRestoration: &agentpb.ConfigurationRestoration{Files: []*agentpb.ConfigurationFileRestoration{{
				ForwardStepId: materializeStepID, ProbeStepId: fileProbeStepID,
				CompensateStepId: fileCompensateStepID,
			}}},
			RuntimeTarget: &agentpb.ComponentRecoveryProcedure_CandidateAbsence{
				CandidateAbsence: &agentpb.ComponentCandidateAbsenceRestoration{
					ComposeProjectName: artifact.ProjectName, ServiceId: provider.ServiceID,
					ProbeStepId: runtimeProbeStepID, CompensateStepId: runtimeCompensateStepID,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("seal Route execution authority: %v", err)
	}
	value, err := (proto.MarshalOptions{Deterministic: true}).Marshal(sealed)
	if err != nil {
		t.Fatalf("marshal Route execution authority: %v", err)
	}
	*encoded = value
	task.PlanHash = hex.EncodeToString(sealed.PlanHash)
	task.Steps = make([]testtaskjournal.TaskStepRecord, len(sealed.Steps))
	for index, step := range sealed.Steps {
		task.Steps[index] = testtaskjournal.TaskStepRecord{Kind: testtaskjournal.TaskStepOperation, ID: step.StepId}
	}
}
