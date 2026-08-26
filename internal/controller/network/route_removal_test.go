package network

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a Route that never reached an enabled Caddy projection must
// still use the durable deletion lifecycle without dispatching host work.
func TestPrepareControllerRouteRemovalTaskBindsExactFinalizer(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, time.August, 23, 3, 0, 0, 0, time.UTC)
	routeID := ids.NewAt(ids.KindRoute, at, 1)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 2)
	task := etcd.TaskRecord{
		ID: ids.NewAt(ids.KindTask, at, 3), OperationID: ids.NewAt(ids.KindOperation, at, 4),
		PlanID: ids.NewAt(ids.KindPlan, at, 5), Type: etcd.TaskRemove, Target: routeID,
		Status: etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: at,
	}
	intent, err := etcd.NewRouteRemovalIntent(task.ID, environmentID, routeID, 17, nil, at)
	if err != nil {
		t.Fatalf("NewRouteRemovalIntent() error = %v", err)
	}
	prepared, err := prepareControllerRouteRemovalTask(task, intent)
	if err != nil {
		t.Fatalf("PrepareControllerRouteRemovalTask() error = %v", err)
	}
	if prepared.Executor != etcd.TaskExecutorController || prepared.RenderGeneration != 1 ||
		prepared.TimeoutSeconds != routeRemovalControllerTimeoutSeconds || len(prepared.Steps) != 1 ||
		ids.Validate(ids.KindStep, prepared.Steps[0].ID) != nil || len(prepared.Params) != 2 ||
		prepared.Params[etcd.TaskResourceKindParam] != etcd.TaskResourceRoute ||
		prepared.Params[etcd.TaskRouteEnvironmentParam] != environmentID || len(prepared.PlanHash) != 64 {
		t.Fatalf("prepared Controller Route removal Task = %#v", prepared)
	}
	again, err := controllerRouteRemovalPlanHash(intent)
	if err != nil || again != prepared.PlanHash {
		t.Fatalf("controllerRouteRemovalPlanHash() = %q, %v; want %q", again, err, prepared.PlanHash)
	}
}

// Rationale: render generation is immutable execution identity, so candidate
// projections must bind it exactly and reject values the Task schema cannot
// represent rather than truncating them.
func TestPrepareControllerRouteRemovalTaskValidatesCandidateRenderGeneration(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, time.August, 23, 3, 30, 0, 0, time.UTC)
	base := etcd.RouteRemovalIntent{
		TaskID: ids.NewAt(ids.KindTask, at, 1), EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 2),
		RouteID: ids.NewAt(ids.KindRoute, at, 3), RouteRevision: 4,
		Status: etcd.TaskStatusPending, CreatedAt: at,
	}
	task := etcd.TaskRecord{ID: base.TaskID, Type: etcd.TaskRemove, Target: base.RouteID, CreatedAt: at}

	for _, test := range []struct {
		name       string
		generation uint64
		want       int32
		wantError  bool
	}{
		{name: "candidate generation", generation: 17, want: 17},
		{name: "zero", generation: 0, wantError: true},
		{name: "overflow", generation: uint64(math.MaxInt32) + 1, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			intent := base
			intent.CandidateProjection = &etcd.EnvironmentComposeProjection{RenderGeneration: test.generation}
			prepared, err := prepareControllerRouteRemovalTask(task, intent)
			if test.wantError {
				kind, ok := errs.KindOf(err)
				if err == nil || !ok || kind != errs.KindStateConflict {
					t.Fatalf("prepareControllerRouteRemovalTask() error = %v, kind = %v", err, kind)
				}
				return
			}
			if err != nil || prepared.RenderGeneration != test.want {
				t.Fatalf("prepareControllerRouteRemovalTask() = %#v, %v", prepared, err)
			}
		})
	}
}

type routeRemovalRepositoryFake struct {
	environment          etcd.Versioned[etcd.EnvironmentRecord]
	project              etcd.Versioned[etcd.ProjectRecord]
	target               etcd.Versioned[etcd.ServiceRecord]
	route                etcd.Versioned[etcd.RouteRecord]
	projection           *etcd.Versioned[etcd.EnvironmentComposeProjection]
	conflicts            int
	begins               int
	environmentRevisions []int64
	task                 etcd.TaskRecord
	intent               etcd.RouteRemovalIntent
	tombstone            etcd.DeletionTombstoneRecord
	marker               etcd.IdempotencyMarker
}

func (fake *routeRemovalRepositoryFake) GetEnvironment(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return fake.environment, nil
}

func (fake *routeRemovalRepositoryFake) GetProject(
	context.Context,
	string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	return fake.project, nil
}

func (fake *routeRemovalRepositoryFake) GetService(
	context.Context,
	string,
) (etcd.Versioned[etcd.ServiceRecord], error) {
	return fake.target, nil
}

func (fake *routeRemovalRepositoryFake) GetRoute(
	context.Context,
	string,
) (etcd.Versioned[etcd.RouteRecord], error) {
	return fake.route, nil
}

func (fake *routeRemovalRepositoryFake) GetEnvironmentComposeProjection(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	if fake.projection == nil {
		return etcd.Versioned[etcd.EnvironmentComposeProjection]{}, false, nil
	}
	return *fake.projection, true, nil
}

func (fake *routeRemovalRepositoryFake) BeginRouteDeletionWithTask(
	ctx context.Context,
	environment etcd.Versioned[etcd.EnvironmentRecord],
	project etcd.Versioned[etcd.ProjectRecord],
	target etcd.Versioned[etcd.ServiceRecord],
	route etcd.Versioned[etcd.RouteRecord],
	projection *etcd.Versioned[etcd.EnvironmentComposeProjection],
	tombstone etcd.DeletionTombstoneRecord,
	intent etcd.RouteRemovalIntent,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	fake.begins++
	fake.environmentRevisions = append(fake.environmentRevisions, fake.environment.Revision)
	fake.task = task
	fake.intent = intent
	fake.tombstone = tombstone
	fake.marker = marker
	fake.marker.Response.Body = append([]byte(nil), marker.Response.Body...)
	store := &routeRemovalTransactionStore{
		routeID: route.Record.Desired.ID, environmentID: environment.Record.ID,
		indexRevision: 40, revision: int64(100 + fake.begins), conflict: fake.begins <= fake.conflicts,
	}
	routes, err := etcd.NewRouteRepository(store)
	if err != nil {
		return etcd.IdempotencyTransactionResult{}, err
	}
	result, err := routes.BeginRouteDeletionWithTask(
		ctx, environment, project, target, route, projection,
		tombstone, intent, task, marker,
	)
	if err == nil && store.conflict {
		fake.environment.Revision++
		fake.environment.ReadRevision = fake.environment.Revision
	}
	return result, err
}

type routeRemovalTransactionStore struct {
	etcd.Store
	routeID       string
	environmentID string
	indexRevision int64
	revision      int64
	conflict      bool
}

func (store *routeRemovalTransactionStore) GetMany(
	context.Context,
	etcd.GetManyRequest,
) (*etcd.GetManyResult, error) {
	return &etcd.GetManyResult{
		Values: []*etcd.KeyValue{
			{Value: []byte(store.routeID), ModRevision: store.indexRevision},
			{Value: []byte(store.routeID), ModRevision: store.indexRevision + 1},
		},
		ReadRevision: store.revision, ResponseRevision: store.revision,
	}, nil
}

func (store *routeRemovalTransactionStore) Transact(
	_ context.Context,
	conditions []etcd.Condition,
	_ []etcd.Mutation,
) (etcd.TransactionResult, error) {
	if !store.conflict {
		return etcd.TransactionResult{Succeeded: true, Revision: store.revision}, nil
	}
	failureReads := make([]*etcd.KeyValue, len(conditions))
	for index, condition := range conditions {
		if condition.ModRevision == 0 {
			continue
		}
		revision := condition.ModRevision
		if strings.HasPrefix(condition.Key, "/v1/records/environments/") {
			revision++
		}
		failureReads[index] = &etcd.KeyValue{
			Key: condition.Key, Value: []byte(store.routeID), ModRevision: revision,
		}
	}
	return etcd.TransactionResult{Revision: store.revision, FailureReads: failureReads}, nil
}

type routeRemovalIdempotencyFake struct {
	indexed  bool
	locator  etcd.IdempotencyLocator
	replay   etcd.IdempotencyResponse
	prepares int
	existing int
	known    int
}

func (fake *routeRemovalIdempotencyFake) ResolveReplayLocator(
	context.Context,
	etcd.IdempotencyReplayTarget,
	string,
	string,
	string,
) (etcd.IdempotencyLocator, bool, error) {
	return fake.locator, fake.indexed, nil
}

func (fake *routeRemovalIdempotencyFake) Prepare(
	context.Context,
	etcd.IdempotencyLocator,
	string,
) (routeRemovalEvidence, error) {
	fake.prepares++
	ciphertext := []byte("route removal intent")
	digest := sha256.Sum256(ciphertext)
	return routeRemovalEvidence{durable: etcd.ProtectedIntentRecord{
		EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
	}}, nil
}

func (fake *routeRemovalIdempotencyFake) ResolveExisting(
	context.Context,
	etcd.IdempotencyLocator,
	routeRemovalEvidence,
) (idempotentintent.Resolution, bool, error) {
	fake.existing++
	if fake.indexed {
		return idempotentintent.Resolution{Kind: idempotentintent.ResolutionReplay, Response: fake.replay}, true, nil
	}
	return idempotentintent.Resolution{}, false, nil
}

func (fake *routeRemovalIdempotencyFake) ResolveKnown(
	_ context.Context,
	_ routeRemovalEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	fake.known++
	outcome, _, conflict, err := result.Classify()
	if err != nil {
		return idempotentintent.Resolution{}, err
	}
	switch outcome {
	case etcd.IdempotencyKnownApplied:
		return idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied}, nil
	case etcd.IdempotencyKnownConflict:
		return idempotentintent.Resolution{}, conflict
	default:
		return idempotentintent.Resolution{}, errs.New(errs.KindInternal, "unexpected Route removal result")
	}
}

func (fake *routeRemovalIdempotencyFake) ResolveUnknown(
	context.Context,
	etcd.IdempotencyLocator,
	routeRemovalEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{}, nil
}

type routeRemovalPlanFake struct {
	err       error
	calls     int
	intent    etcd.RouteRemovalIntent
	procedure controller.RouteRemovalTaskProcedureIDs
}

func (fake *routeRemovalPlanFake) PrepareRouteRemovalTask(
	_ context.Context,
	task etcd.TaskRecord,
	intent etcd.RouteRemovalIntent,
	procedure controller.RouteRemovalTaskProcedureIDs,
) (etcd.TaskRecord, error) {
	fake.calls++
	fake.intent = intent
	fake.procedure = procedure
	if fake.err != nil {
		return etcd.TaskRecord{}, fake.err
	}
	componentID := intent.CandidateProjection.Components[0].Desired.ID
	task.Params = map[string]string{
		etcd.TaskRouteEnvironmentParam:               intent.EnvironmentID,
		etcd.TaskMaterializationEnvironmentParam:     intent.EnvironmentID,
		etcd.EnvironmentBlueprintRevisionParam:       intent.CandidateProjection.BlueprintRevisionID,
		controller.EnvironmentBlueprintArtifactParam: procedure.ArtifactID,
	}
	task.RenderGeneration = int32(intent.CandidateProjection.RenderGeneration)
	task.Steps = []etcd.TaskStepRecord{{ID: procedure.MaterializeStepID}, {ID: procedure.ApplyStepID}}
	task.Materializations = []etcd.TaskMaterializationRecord{{
		StepID: procedure.MaterializeStepID, MaterializationID: procedure.MaterializationID,
		EnvironmentID: intent.EnvironmentID, Destination: controller.RouteRemovalCaddyfilePath,
		OutputKind: etcd.TaskMaterializationOutputPlainFile,
		Mode:       uint32(entrymaterialization.ModeReadOnly), Length: 1,
		SHA256: strings.Repeat("a", 64),
		Source: etcd.TaskMaterializationSource{
			Kind: etcd.TaskMaterializationSourceComponentFile,
			ComponentFile: &etcd.TaskComponentFileValueReference{
				RevisionID: intent.CandidateProjection.BlueprintRevisionID, ComponentID: componentID,
				Path: controller.RouteRemovalCaddyfilePath, RouteRemovalTaskID: task.ID,
			},
		},
	}}
	task.PlanHash = strings.Repeat("b", 64)
	return task, nil
}

func routeRemovalServiceState(
	t *testing.T,
) (*routeRemovalRepositoryFake, *routeRemovalIdempotencyFake, *routeRemovalPlanFake, *routeRemovalService) {
	t.Helper()
	at := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	projectID := ids.NewAt(ids.KindProject, at, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 3)
	serviceID := ids.NewAt(ids.KindService, at, 4)
	routeID := ids.NewAt(ids.KindRoute, at, 5)
	target, err := etcd.NewServiceRecord(
		environmentID,
		core.Service{ID: serviceID, Name: "api", Image: "api:1"},
		"",
	)
	if err != nil {
		t.Fatalf("NewServiceRecord() error = %v", err)
	}
	repository := &routeRemovalRepositoryFake{
		environment: etcd.Versioned[etcd.EnvironmentRecord]{
			Record: etcd.EnvironmentRecord{
				ID: environmentID, ProjectID: projectID, Name: "production", NetworkPool: "10.70.0.0/16",
				VolumeDir:         "/var/lib/groundplane/vol/platform/" + projectID + "/" + environmentID,
				ProvisioningState: etcd.EnvironmentProvisioningReady,
				CreateTaskID:      ids.NewAt(ids.KindTask, at, 6), CreatedAt: at,
			},
			Revision: 11, ReadRevision: 11,
		},
		project: etcd.Versioned[etcd.ProjectRecord]{
			Record: etcd.ProjectRecord{
				ID: projectID, Slug: "platform", Name: "Platform", Kind: etcd.ProjectKindBacking,
			},
			Revision: 12, ReadRevision: 12,
		},
		target: etcd.Versioned[etcd.ServiceRecord]{
			Record:   target,
			Revision: 13, ReadRevision: 13,
		},
		route: etcd.Versioned[etcd.RouteRecord]{
			Record: etcd.RouteRecord{EnvironmentID: environmentID, Desired: core.Route{
				ID: routeID, Host: "app.example.com", Path: "/", Exposure: "public",
				TargetServiceID: serviceID, TargetPort: 8080,
			}},
			Revision: 14, ReadRevision: 14,
		},
	}
	idempotency := &routeRemovalIdempotencyFake{}
	plans := &routeRemovalPlanFake{}
	service, err := newRouteRemovalService(repository, plans, idempotency)
	if err != nil {
		t.Fatalf("newRouteRemovalService() error = %v", err)
	}
	service.now = func() time.Time { return at }
	return repository, idempotency, plans, service
}

// Rationale: accepting removal must atomically bind the public Task response,
// task, immutable intent, tombstone, and protected marker to the same stable
// Route and Environment identities before any finalizer can run.
func TestRouteRemovalPublishesExactDurableIdentity(t *testing.T) {
	t.Parallel()
	repository, idempotency, _, service := routeRemovalServiceState(t)
	response, err := service.RemoveRoute(
		context.Background(), repository.route.Record.Desired.ID, "route-remove-key-0001",
	)
	if err != nil {
		t.Fatalf("RemoveRoute() error = %v", err)
	}
	var accepted apiTypes.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatalf("RemoveRoute() response = %q: %v", response.Body, err)
	}
	if response.Status != http.StatusAccepted || repository.begins != 1 || accepted.TaskID != repository.task.ID ||
		repository.task.Target != repository.route.Record.Desired.ID ||
		repository.task.Executor != etcd.TaskExecutorController ||
		repository.intent.TaskID != repository.task.ID || repository.intent.RouteID != repository.task.Target ||
		repository.intent.EnvironmentID != repository.environment.Record.ID ||
		repository.tombstone.TaskID != repository.task.ID || repository.tombstone.TargetID != repository.task.Target ||
		repository.marker.TaskID != repository.task.ID ||
		repository.marker.Locator.ScopeID != repository.environment.Record.ID ||
		repository.marker.Locator.Method != http.MethodDelete || repository.marker.Locator.Route != routeDeletionRoute ||
		repository.marker.Locator.Key != "route-remove-key-0001" || idempotency.prepares != 1 ||
		idempotency.existing != 1 || idempotency.known != 1 {
		t.Fatalf(
			"published removal = task %#v intent %#v tombstone %#v marker %#v response %#v",
			repository.task, repository.intent, repository.tombstone, repository.marker, response,
		)
	}
}

// Rationale: removing a Route present in the applied Caddy projection must
// publish host work against the exact suppressed candidate generation.
func TestRouteRemovalSelectsAgentCaddyPlanForAppliedRoute(t *testing.T) {
	t.Parallel()
	repository, _, plans, service := routeRemovalServiceState(t)
	at := service.now()
	caddyServiceID := ids.NewAt(ids.KindService, at, 20)
	component, err := etcd.NewComponentRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, at, 21), Owner: core.ComponentOwnerEnvironment,
		OwnerID: repository.environment.Record.ID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
		GeneratedServices: []string{caddyServiceID}, PinnedIPv4: "10.70.0.2", Healthy: true,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord() error = %v", err)
	}
	edge, err := etcd.NewComponentRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, at, 23), Owner: core.ComponentOwnerEnvironment,
		OwnerID: repository.environment.Record.ID, Kind: core.ComponentKindEdgeCloudflare,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(edge) error = %v", err)
	}
	repository.projection = &etcd.Versioned[etcd.EnvironmentComposeProjection]{
		Record: etcd.EnvironmentComposeProjection{
			EnvironmentID:       repository.environment.Record.ID,
			BlueprintRevisionID: ids.NewAt(ids.KindTask, at, 22), RenderGeneration: 7,
			Services: []etcd.EnvironmentComposeIdentity{{ID: caddyServiceID, Name: "caddy"}},
			Routes: []etcd.EnvironmentRouteIdentity{{
				ID: repository.route.Record.Desired.ID, Host: repository.route.Record.Desired.Host,
				Path: repository.route.Record.Desired.Path,
			}},
			Components: []etcd.ComponentRecord{component, edge},
		},
		Revision: 15, ReadRevision: 15,
	}
	if _, err := service.RemoveRoute(
		context.Background(), repository.route.Record.Desired.ID, "route-remove-key-0004",
	); err != nil {
		t.Fatalf("RemoveRoute() error = %v", err)
	}
	if plans.calls != 1 || !plans.intent.RequiresCaddy || plans.intent.CandidateProjection == nil ||
		plans.intent.CandidateProjection.RenderGeneration != 8 ||
		repository.task.Executor != etcd.TaskExecutorAgent || repository.task.RenderGeneration != 8 ||
		repository.tombstone.Phase != etcd.DeletionPhaseHostEffects {
		t.Fatalf(
			"applied Route removal = calls %d intent %#v task %#v tombstone %#v",
			plans.calls, plans.intent, repository.task, repository.tombstone,
		)
	}
}

// Rationale: candidate rendering is part of publication, so planner failure
// must escape unchanged and leave no durable deletion boundary behind.
func TestRouteRemovalPropagatesCaddyPlannerError(t *testing.T) {
	t.Parallel()
	repository, _, plans, service := routeRemovalServiceState(t)
	at := service.now()
	caddyServiceID := ids.NewAt(ids.KindService, at, 30)
	component, err := etcd.NewComponentRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, at, 31), Owner: core.ComponentOwnerEnvironment,
		OwnerID: repository.environment.Record.ID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
		GeneratedServices: []string{caddyServiceID}, PinnedIPv4: "10.70.0.3", Healthy: true,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord() error = %v", err)
	}
	edge, err := etcd.NewComponentRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, at, 33), Owner: core.ComponentOwnerEnvironment,
		OwnerID: repository.environment.Record.ID, Kind: core.ComponentKindEdgeCloudflare,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(edge) error = %v", err)
	}
	repository.projection = &etcd.Versioned[etcd.EnvironmentComposeProjection]{
		Record: etcd.EnvironmentComposeProjection{
			EnvironmentID:       repository.environment.Record.ID,
			BlueprintRevisionID: ids.NewAt(ids.KindTask, at, 32), RenderGeneration: 9,
			Services: []etcd.EnvironmentComposeIdentity{{ID: caddyServiceID, Name: "caddy"}},
			Routes: []etcd.EnvironmentRouteIdentity{{
				ID: repository.route.Record.Desired.ID, Host: repository.route.Record.Desired.Host,
				Path: repository.route.Record.Desired.Path,
			}},
			Components: []etcd.ComponentRecord{component, edge},
		},
		Revision: 16, ReadRevision: 16,
	}
	plans.err = errs.New(errs.KindInternal, "injected Caddy planner failure")
	_, err = service.RemoveRoute(
		context.Background(), repository.route.Record.Desired.ID, "route-remove-key-0005",
	)
	kind, ok := errs.KindOf(err)
	if !errors.Is(err, plans.err) || !ok || kind != errs.KindInternal || plans.calls != 1 || repository.begins != 0 {
		t.Fatalf("RemoveRoute(planner failure) = %v, calls %d, begins %d", err, plans.calls, repository.begins)
	}
}

// Rationale: compare conflicts are expected under concurrent Environment
// mutation; bounded retries must republish from fresh authority and stop after
// the third exact attempt.
func TestRouteRemovalRetriesPublicationConflicts(t *testing.T) {
	t.Parallel()
	repository, idempotency, _, service := routeRemovalServiceState(t)
	repository.conflicts = 2
	if _, err := service.RemoveRoute(
		context.Background(), repository.route.Record.Desired.ID, "route-remove-key-0002",
	); err != nil {
		t.Fatalf("RemoveRoute() error = %v", err)
	}
	if repository.begins != maximumRouteDeletionAttempts || idempotency.prepares != maximumRouteDeletionAttempts ||
		idempotency.known != maximumRouteDeletionAttempts ||
		len(repository.environmentRevisions) != maximumRouteDeletionAttempts ||
		repository.environmentRevisions[0]+1 != repository.environmentRevisions[1] ||
		repository.environmentRevisions[1]+1 != repository.environmentRevisions[2] {
		t.Fatalf(
			"retry evidence = begins %d prepares %d known %d revisions %v",
			repository.begins, idempotency.prepares, idempotency.known, repository.environmentRevisions,
		)
	}
}

// Rationale: after finalization removes the Route primary, the replay locator
// is the only safe authority; identical retries must return the original bytes
// without publishing a replacement Route.
func TestRouteRemovalReplaysFromDurableTargetLocator(t *testing.T) {
	t.Parallel()
	repository, idempotency, _, service := routeRemovalServiceState(t)
	idempotency.indexed = true
	idempotency.locator = etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: repository.environment.Record.ID,
		Method: http.MethodDelete, Route: routeDeletionRoute, Key: "route-remove-key-0003",
	}
	idempotency.replay = etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: []byte(`{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`),
	}
	response, err := service.RemoveRoute(
		context.Background(), repository.route.Record.Desired.ID, "route-remove-key-0003",
	)
	if err != nil {
		t.Fatalf("RemoveRoute(replay) error = %v", err)
	}
	if string(response.Body) != string(idempotency.replay.Body) || repository.begins != 0 ||
		idempotency.prepares != 1 || idempotency.existing != 1 {
		t.Fatalf("replay = %#v, begins %d", response, repository.begins)
	}
}
