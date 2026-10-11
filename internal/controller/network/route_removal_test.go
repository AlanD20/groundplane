package network

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testroutes "github.com/AlanD20/groundplane/internal/infra/etcd/routes"

	// Rationale: a Route that never reached an enabled provider projection must
	// still use the durable deletion lifecycle without dispatching host work.
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testenvironmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestPrepareControllerRouteRemovalTaskBindsExactFinalizer(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, time.August, 23, 3, 0, 0, 0, time.UTC)
	routeID := ids.NewAt(ids.KindRoute, at, 1)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 2)
	task := etcd.TaskRecord{
		ID: ids.NewAt(ids.KindTask, at, 3), OperationID: ids.NewAt(ids.KindOperation, at, 4),
		PlanID: ids.NewAt(ids.KindPlan, at, 5), Type: testtaskjournal.TaskRemove, Target: routeID,
		Status: testtaskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: at,
	}
	intent, err := testenvironmentchanges.NewRouteRemovalIntent(task.ID, environmentID, routeID, 17, nil, at)
	if err != nil {
		t.Fatalf("NewRouteRemovalIntent() error = %v", err)
	}
	prepared, err := prepareControllerRouteRemovalTask(task, intent)
	if err != nil {
		t.Fatalf("PrepareControllerRouteRemovalTask() error = %v", err)
	}
	if prepared.Executor != testtaskjournal.TaskExecutorController || prepared.RenderGeneration != 1 ||
		prepared.TimeoutSeconds != routeRemovalControllerTimeoutSeconds || len(prepared.Steps) != 1 ||
		ids.Validate(ids.KindStep, prepared.Steps[0].ID) != nil || len(prepared.Params) != 2 ||
		prepared.Params[testtaskjournal.TaskResourceKindParam] != testtaskjournal.TaskResourceRoute ||
		prepared.Params[testtaskjournal.TaskRouteEnvironmentParam] != environmentID || len(prepared.PlanHash) != 64 {
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
	base := testenvironmentchanges.RouteRemovalIntent{
		TaskID: ids.NewAt(ids.KindTask, at, 1), EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 2),
		RouteID: ids.NewAt(ids.KindRoute, at, 3), RouteRevision: 4,
		Status: testtaskjournal.TaskStatusPending, CreatedAt: at,
	}
	task := etcd.TaskRecord{ID: base.TaskID, Type: testtaskjournal.TaskRemove, Target: base.RouteID, CreatedAt: at}

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
			intent.CandidateProjection = &testenvironmentprojection.EnvironmentComposeProjection{
				RenderGeneration: test.generation,
			}
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
	tenant               testkeyvalue.Versioned[testhierarchy.TenantRecord]
	environment          testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
	project              testkeyvalue.Versioned[testhierarchy.ProjectRecord]
	target               testkeyvalue.Versioned[testservices.ServiceRecord]
	serviceEvidence      *routeRemovalServiceEvidenceStore
	route                testkeyvalue.Versioned[testroutes.Record]
	projection           *testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]
	conflicts            int
	begins               int
	environmentRevisions []int64
	task                 etcd.TaskRecord
	intent               testenvironmentchanges.RouteRemovalIntent
	tombstone            testdeletions.DeletionTombstoneRecord
	marker               testidempotency.IdempotencyMarker
	transactionStore     *routeRemovalTransactionStore
}

func (fake *routeRemovalRepositoryFake) GetEnvironment(
	context.Context, string,

) (testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], error) {
	return fake.environment, nil
}

func (fake *routeRemovalRepositoryFake) GetProject(
	context.Context, string,

) (testkeyvalue.Versioned[testhierarchy.ProjectRecord], error) {
	return fake.project, nil
}

func (fake *routeRemovalRepositoryFake) GetService(
	ctx context.Context,
	id string,
) (testkeyvalue.Versioned[testservices.ServiceRecord], error) {
	services, err := etcd.NewServiceRepository(fake.serviceEvidence)
	if err != nil {
		return testkeyvalue.Versioned[testservices.ServiceRecord]{}, err
	}
	return services.GetService(ctx, id)
}

func (fake *routeRemovalRepositoryFake) GetRoute(
	context.Context, string,

) (testkeyvalue.Versioned[testroutes.Record], error) {
	return fake.route, nil
}

func (fake *routeRemovalRepositoryFake) GetEnvironmentComposeProjection(
	context.Context, string,

) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	if fake.projection == nil {
		return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{}, false, nil
	}
	return *fake.projection, true, nil
}

func (fake *routeRemovalRepositoryFake) BeginRouteDeletionWithTask(
	ctx context.Context,
	environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord],
	project testkeyvalue.Versioned[testhierarchy.ProjectRecord],
	target testkeyvalue.Versioned[testservices.ServiceRecord],
	route testkeyvalue.Versioned[testroutes.Record],
	projection *testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection],
	tombstone testdeletions.DeletionTombstoneRecord,
	intent testenvironmentchanges.RouteRemovalIntent,
	task etcd.TaskRecord,
	marker testidempotency.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	fake.begins++
	fake.environmentRevisions = append(fake.environmentRevisions, fake.environment.Revision)
	fake.task = task
	fake.intent = intent
	fake.tombstone = tombstone
	fake.marker = marker
	fake.marker.Response.Body = append([]byte(nil), marker.Response.Body...)
	store := fake.transactionStore
	if store == nil {
		store = &routeRemovalTransactionStore{source: fake.serviceEvidence}
		fake.transactionStore = store
	}
	store.routeID = route.Record.Desired.ID
	store.environmentID = environment.Record.ID
	store.indexRevision = 40
	store.revision = int64(100 + fake.begins)
	store.conflict = fake.begins <= fake.conflicts
	if len(task.Materializations) != 0 && store.configuration == nil {
		if err := store.seedConfiguration(ctx, projection); err != nil {
			return etcd.IdempotencyTransactionResult{}, err
		}
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

type routeRemovalServiceEvidenceStore struct {
	testkeyvalue.Store

	environmentID   string
	projection      testenvironmentprojection.EnvironmentComposeProjection
	headValue       []byte
	rootValue       []byte
	chunkValue      []byte
	effectiveValue  []byte
	identitiesValue []byte
	headKey         string
	rootKey         string
	chunkKey        string
	effectiveKey    string
	identitiesKey   string
	readRevision    int64
}

func (store *routeRemovalServiceEvidenceStore) Range(
	_ context.Context,
	request testkeyvalue.RangeRequest,
) (*testkeyvalue.RangeResult, error) {
	if request.Prefix != "/v1/records/environment-blueprints/" {
		return &testkeyvalue.RangeResult{ReadRevision: store.readRevision, ResponseRevision: store.readRevision}, nil
	}
	return &testkeyvalue.RangeResult{
		Values:       []testkeyvalue.KeyValue{{Key: store.headKey, ModRevision: store.readRevision}},
		ReadRevision: store.readRevision, ResponseRevision: store.readRevision,
	}, nil
}

func (store *routeRemovalServiceEvidenceStore) GetMany(
	_ context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	values := make([]*testkeyvalue.KeyValue, len(request.Keys))
	for index, key := range request.Keys {
		switch key {
		case store.headKey:
			values[index] = &testkeyvalue.KeyValue{
				Key:         key,
				Value:       append([]byte(nil), store.headValue...),
				ModRevision: store.readRevision,
			}
		case store.rootKey:
			values[index] = &testkeyvalue.KeyValue{
				Key:         key,
				Value:       append([]byte(nil), store.rootValue...),
				ModRevision: store.readRevision,
			}
		case store.chunkKey:
			values[index] = &testkeyvalue.KeyValue{
				Key:         key,
				Value:       append([]byte(nil), store.chunkValue...),
				ModRevision: store.readRevision,
			}
		case store.effectiveKey:
			values[index] = &testkeyvalue.KeyValue{
				Key: key, Value: append([]byte(nil), store.effectiveValue...),
				ModRevision: store.readRevision,
			}
		case store.identitiesKey:
			values[index] = &testkeyvalue.KeyValue{
				Key: key, Value: append([]byte(nil), store.identitiesValue...),
				ModRevision: store.readRevision,
			}
		}
	}
	return &testkeyvalue.GetManyResult{
		Values:           values,
		ReadRevision:     store.readRevision,
		ResponseRevision: store.readRevision,
	}, nil
}

func routeRemovalServiceEvidence(
	t *testing.T,
	environmentID string,
	service testservices.ServiceRecord,
	route testroutes.Record,
	at time.Time,
) *routeRemovalServiceEvidenceStore {
	t.Helper()
	revisionID := ids.NewAt(ids.KindTask, at, 7)
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: revisionID, RenderGeneration: 1,
		DesiredServices: []testservices.EnvironmentServiceProjection{{
			EnvironmentID: environmentID, Desired: service.Desired,
		}},
		DesiredRoutes: []testenvironmentprojection.EnvironmentRouteProjection{{
			EnvironmentID: environmentID, Desired: route.Desired, DesiredGeneration: route.DesiredGeneration,
		}},
		NormalizedCompose: routeRemovalTestNormalizedCompose(t),
	}
	projection.ComposeArtifact = routeRemovalTestComposeArtifact(projection, "/var/lib/groundplane/vol/test")
	projectionValue, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		t.Fatalf("EncodeEnvironmentComposeProjectionStorage() error = %v", err)
	}
	desiredValue, err := testenvironmentprojection.EncodeEnvironmentDesiredInputStorage(
		testenvironmentprojection.EnvironmentDesiredInput{
			EnvironmentID: environmentID, RevisionID: revisionID, RenderGeneration: 1,
			Input: core.BlueprintDesiredInput{
				NormalizedCompose: append([]byte(nil), projection.NormalizedCompose...),
				NetworkPool:       "10.70.0.0/16",
			},
		},
	)
	if err != nil {
		t.Fatalf("EncodeEnvironmentDesiredInputStorage() error = %v", err)
	}
	identities, err := testenvironmentprojection.OwnedIdentitiesFromProjection(projection)
	if err != nil {
		t.Fatalf("OwnedIdentitiesFromProjection() error = %v", err)
	}
	identitiesValue, err := testenvironmentprojection.EncodeEnvironmentOwnedIdentities(identities)
	if err != nil {
		t.Fatalf("EncodeEnvironmentOwnedIdentities() error = %v", err)
	}
	projectionDigest := sha256.Sum256(desiredValue)
	audit := []byte("route removal fixture")
	auditDigest := sha256.Sum256(audit)
	dependencyDigest := sha256.Sum256([]byte("route removal dependency"))
	sealValue, err := testblueprints.EncodeEnvironmentBlueprintSeal(testblueprints.EnvironmentBlueprintSeal{
		EnvironmentID: environmentID, RevisionID: revisionID,
		SourceKind: testblueprints.EnvironmentBlueprintSourceMutation, RenderGeneration: 1,
		ProjectionSchema: testblueprints.EnvironmentDesiredInputSchema,
		AuditChunks:      1, AuditBytes: uint64(len(audit)), AuditSHA256: auditDigest,
		ProjectionChunks: 1, ProjectionBytes: uint64(len(desiredValue)), ProjectionSHA256: projectionDigest,
		ProjectionResources: 1, DependencyDigest: dependencyDigest,
	})
	if err != nil {
		t.Fatalf("EncodeDesiredRevisionSeal() error = %v", err)
	}
	chunkValue, err := testblueprints.EncodeEnvironmentBlueprintChunk(testblueprints.EnvironmentBlueprintChunk{
		Family: testblueprints.EnvironmentBlueprintChunkProjection, LogicalLength: uint32(len(desiredValue)),
		Digest: projectionDigest, Data: desiredValue,
	})
	if err != nil {
		t.Fatalf("EncodeDesiredRevisionChunk() error = %v", err)
	}
	headValue, err := testidempotency.EncodeTaskReference(revisionID)
	if err != nil {
		t.Fatalf("EncodeCapabilityTaskReference() error = %v", err)
	}
	headKey := "/v1/records/environment-blueprints/" + environmentID + "/current"
	return &routeRemovalServiceEvidenceStore{
		environmentID: environmentID, projection: projection,
		headValue: headValue, rootValue: sealValue, chunkValue: chunkValue,
		effectiveValue: projectionValue, identitiesValue: identitiesValue,
		headKey: headKey, rootKey: testblueprints.EnvironmentBlueprintRootKey(environmentID, revisionID),
		chunkKey: testblueprints.EnvironmentBlueprintChunkKeyFor(
			environmentID,
			revisionID, testblueprints.EnvironmentBlueprintChunkProjection, 0,
		),
		effectiveKey:  testblueprints.EnvironmentBlueprintEffectiveProjectionKey(environmentID, revisionID),
		identitiesKey: testblueprints.EnvironmentBlueprintOwnedIdentitiesKey(environmentID, revisionID),
		readRevision:  13,
	}
}

type routeRemovalIdempotencyFake struct {
	indexed  bool
	locator  testidempotency.IdempotencyLocator
	replay   testidempotency.IdempotencyResponse
	prepares int
	existing int
	known    int
}

func (fake *routeRemovalIdempotencyFake) ResolveReplayLocator(
	context.Context, testidempotency.IdempotencyReplayTarget, string, string, string,

) (testidempotency.IdempotencyLocator, bool, error) {
	return fake.locator, fake.indexed, nil
}

func (fake *routeRemovalIdempotencyFake) Prepare(
	context.Context, testidempotency.IdempotencyLocator, string,

) (routeRemovalEvidence, error) {
	fake.prepares++
	ciphertext := []byte("route removal intent")
	digest := sha256.Sum256(ciphertext)
	return routeRemovalEvidence{durable: testidempotency.ProtectedIntentRecord{
		EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
	}}, nil
}

func (fake *routeRemovalIdempotencyFake) ResolveExisting(
	context.Context, testidempotency.IdempotencyLocator,

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
	context.Context, testidempotency.IdempotencyLocator,

	routeRemovalEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{}, nil
}

type routeRemovalPlanFake struct {
	err       error
	calls     int
	intent    testenvironmentchanges.RouteRemovalIntent
	procedure testtaskplanning.RouteRemovalTaskProcedureIDs
	resolver  *testtaskplanning.TaskPlanResolver
}

func (fake *routeRemovalPlanFake) PrepareRouteRemovalTask(
	ctx context.Context,
	task etcd.TaskRecord,
	intent testenvironmentchanges.RouteRemovalIntent,
	procedure testtaskplanning.RouteRemovalTaskProcedureIDs,
) (etcd.RouteRemovalTaskPreparation, error) {
	fake.calls++
	fake.intent = intent
	fake.procedure = procedure
	if fake.err != nil {
		return etcd.RouteRemovalTaskPreparation{}, fake.err
	}
	prepared, err := fake.resolver.PrepareRouteRemovalTask(ctx, task, intent, procedure)
	if err == nil {
		fake.intent = prepared.Intent
	}
	return prepared, err
}

func routeRemovalServiceState(
	t *testing.T,
) (*routeRemovalRepositoryFake, *routeRemovalIdempotencyFake, *routeRemovalPlanFake, *routeRemovalService) {
	t.Helper()
	at := time.Date(2026, time.August, 24, 10, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, at, 1)
	projectID := ids.NewAt(ids.KindProject, at, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 3)
	serviceID := ids.NewAt(ids.KindService, at, 4)
	routeID := ids.NewAt(ids.KindRoute, at, 5)
	target, err := testservices.NewServiceRecord(
		environmentID,
		core.Service{
			ID: serviceID, Name: "api", Image: "api:1",
			Strategy: core.StrategyRecreate, Replicas: 1,
		},
		"",
	)
	if err != nil {
		t.Fatalf("NewServiceRecord() error = %v", err)
	}
	repository := &routeRemovalRepositoryFake{
		tenant: testkeyvalue.Versioned[testhierarchy.TenantRecord]{
			Record:   testhierarchy.TenantRecord{ID: tenantID, Slug: "acme", Name: "Acme"},
			Revision: 10, ReadRevision: 10,
		},
		environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{
				ID: environmentID, ProjectID: projectID, Name: "production", NetworkPool: "10.70.0.0/16",
				VolumeDir:         "/var/lib/groundplane/vol/platform/" + projectID + "/" + environmentID,
				ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
				CreateTaskID:      ids.NewAt(ids.KindTask, at, 6), CreatedAt: at,
			},
			Revision: 11, ReadRevision: 11,
		},
		project: testkeyvalue.Versioned[testhierarchy.ProjectRecord]{
			Record: testhierarchy.ProjectRecord{
				ID: projectID, TenantID: tenantID, Slug: "platform", Name: "Platform",
				Kind: testhierarchy.ProjectKindTenant,
			},
			Revision: 12, ReadRevision: 12,
		},
		target: testkeyvalue.Versioned[testservices.ServiceRecord]{
			Record:   target,
			Revision: 13, ReadRevision: 13,
		},
		route: testkeyvalue.Versioned[testroutes.Record]{
			Record: testroutes.Record{EnvironmentID: environmentID, Desired: core.Route{
				ID: routeID, Host: "app.example.com", Path: "/", Exposure: "public",
				TargetServiceID: serviceID, TargetPort: 8080,
			}, DesiredGeneration: 1, Observed: testroutes.Observation{
				Status: testroutes.ObservedUnserved, DesiredGeneration: 1,
			}},
			Revision: 14, ReadRevision: 14,
		},
	}
	repository.serviceEvidence = routeRemovalServiceEvidence(t, environmentID, target, repository.route.Record, at)
	repository.projection = &testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
		Record: repository.serviceEvidence.projection, Revision: 13, ReadRevision: 13,
	}
	repository.transactionStore = &routeRemovalTransactionStore{
		routeID: repository.route.Record.Desired.ID, environmentID: environmentID,
		indexRevision: 40, revision: 100, source: repository.serviceEvidence,
	}
	if err := repository.transactionStore.seedConfiguration(context.Background(), repository.projection); err != nil {
		t.Fatalf("seedConfiguration() error = %v", err)
	}
	if err := repository.transactionStore.seedTenant(repository.tenant); err != nil {
		t.Fatalf("seedTenant() error = %v", err)
	}
	idempotency := &routeRemovalIdempotencyFake{}
	plans := newRouteRemovalPlanFake(t, repository)
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
		repository.task.Executor != testtaskjournal.TaskExecutorController ||
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

// HTTP-06: Rationale: removing a Route present in an applied provider projection must
// publish host work against the exact suppressed candidate generation.
func TestRouteRemovalSelectsAgentProviderPlanForAppliedRoute(t *testing.T) {
	t.Parallel()
	repository, _, plans, service := routeRemovalServiceState(t)
	at := service.now()
	caddyServiceID := ids.NewAt(ids.KindService, at, 20)
	zoneID := ids.NewAt(ids.KindNetwork, at, 24)
	component, err := testcomponents.NewRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, at, 21), Owner: core.ComponentOwnerEnvironment,
		OwnerID: repository.environment.Record.ID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
			ZoneIDs: []string{zoneID},
		}},
		GeneratedServices: []string{caddyServiceID}, PinnedIPv4: "10.70.1.2", Healthy: true,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord() error = %v", err)
	}
	edge, err := testcomponents.NewRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, at, 23), Owner: core.ComponentOwnerEnvironment,
		OwnerID: repository.environment.Record.ID, Kind: core.ComponentKindEdgeCloudflare,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(edge) error = %v", err)
	}
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: repository.environment.Record.ID,
		RevisionID:    repository.serviceEvidence.projection.RevisionID, RenderGeneration: 1,
		DesiredServices: []testservices.EnvironmentServiceProjection{
			{EnvironmentID: repository.environment.Record.ID, Desired: repository.target.Record.Desired},
			{EnvironmentID: repository.environment.Record.ID, Desired: core.Service{
				ID: caddyServiceID, Name: "caddy", Image: "caddy:1",
				Strategy: core.StrategyRecreate, Replicas: 1,
			}},
		},
		DesiredZones: []testenvironmentprojection.EnvironmentZoneProjection{{
			EnvironmentID: repository.environment.Record.ID,
			Desired: core.Zone{ID: zoneID, Name: "frontend", Subnet: "10.70.1.0/24",
				OwnerKind: core.ZoneOwnerEnvironment, OwnerID: repository.environment.Record.ID},
		}},
		DesiredRoutes: []testenvironmentprojection.EnvironmentRouteProjection{{
			EnvironmentID: repository.environment.Record.ID, Desired: repository.route.Record.Desired,
			DesiredGeneration: repository.route.Record.DesiredGeneration,
		}},
		Components: []testcomponents.Record{component, edge},
	}
	projection.NormalizedCompose = routeRemovalTestRouterNormalizedCompose(t)
	projection.ComposeArtifact = routeRemovalTestComposeArtifact(projection, repository.environment.Record.VolumeDir)
	repository.projection = &testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
		Record: projection, Revision: 13, ReadRevision: 13,
	}
	identities, err := testenvironmentprojection.OwnedIdentitiesFromProjection(projection)
	if err != nil {
		t.Fatal(err)
	}
	repository.serviceEvidence.identitiesValue, err = testenvironmentprojection.EncodeEnvironmentOwnedIdentities(
		identities,
	)
	if err != nil {
		t.Fatal(err)
	}
	repository.serviceEvidence.effectiveValue, err = testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(
		projection,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RemoveRoute(
		context.Background(), repository.route.Record.Desired.ID, "route-remove-key-0004",
	); err != nil {
		t.Fatalf("RemoveRoute() error = %v", err)
	}
	if plans.calls != 1 || plans.intent.Provider == nil || plans.intent.CandidateProjection == nil ||
		plans.intent.CandidateProjection.RenderGeneration != 2 ||
		repository.task.Executor != testtaskjournal.TaskExecutorAgent || repository.task.RenderGeneration != 2 ||
		repository.tombstone.Phase != testdeletions.DeletionPhaseHostEffects {
		t.Fatalf(
			"applied Route removal = calls %d intent %#v task %#v tombstone %#v",
			plans.calls, plans.intent, repository.task, repository.tombstone,
		)
	}
}

// Rationale: candidate rendering is part of publication, so planner failure
// must escape unchanged and leave no durable deletion boundary behind.
func TestRouteRemovalPropagatesProviderPlannerError(t *testing.T) {
	t.Parallel()
	repository, _, plans, service := routeRemovalServiceState(t)
	at := service.now()
	caddyServiceID := ids.NewAt(ids.KindService, at, 30)
	zoneID := ids.NewAt(ids.KindNetwork, at, 34)
	component, err := testcomponents.NewRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, at, 31), Owner: core.ComponentOwnerEnvironment,
		OwnerID: repository.environment.Record.ID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
			ZoneIDs: []string{zoneID},
		}},
		GeneratedServices: []string{caddyServiceID}, PinnedIPv4: "10.70.0.3", Healthy: true,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord() error = %v", err)
	}
	edge, err := testcomponents.NewRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, at, 33), Owner: core.ComponentOwnerEnvironment,
		OwnerID: repository.environment.Record.ID, Kind: core.ComponentKindEdgeCloudflare,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(edge) error = %v", err)
	}
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: repository.environment.Record.ID,
		RevisionID:    ids.NewAt(ids.KindTask, at, 32), RenderGeneration: 9,
		DesiredServices: []testservices.EnvironmentServiceProjection{
			{EnvironmentID: repository.environment.Record.ID, Desired: repository.target.Record.Desired},
			{EnvironmentID: repository.environment.Record.ID, Desired: core.Service{
				ID: caddyServiceID, Name: "caddy", Image: "caddy:1",
				Strategy: core.StrategyRecreate, Replicas: 1,
			}},
		},
		DesiredZones: []testenvironmentprojection.EnvironmentZoneProjection{{
			EnvironmentID: repository.environment.Record.ID,
			Desired: core.Zone{ID: zoneID, Name: "frontend", Subnet: "10.70.1.0/24",
				OwnerKind: core.ZoneOwnerEnvironment, OwnerID: repository.environment.Record.ID},
		}},
		DesiredRoutes: []testenvironmentprojection.EnvironmentRouteProjection{{
			EnvironmentID: repository.environment.Record.ID, Desired: repository.route.Record.Desired,
			DesiredGeneration: repository.route.Record.DesiredGeneration,
		}},
		Components: []testcomponents.Record{component, edge},
	}
	projection.NormalizedCompose = routeRemovalTestNormalizedCompose(t)
	projection.ComposeArtifact = routeRemovalTestComposeArtifact(projection, repository.environment.Record.VolumeDir)
	repository.projection = &testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
		Record: projection, Revision: 16, ReadRevision: 16,
	}
	plans.err = errs.New(errs.KindInternal, "injected provider planner failure")
	_, err = service.RemoveRoute(
		context.Background(), repository.route.Record.Desired.ID, "route-remove-key-0005",
	)
	kind, ok := errs.KindOf(err)
	if !errors.Is(err, plans.err) || !ok || kind != errs.KindInternal || plans.calls != 1 || repository.begins != 0 {
		t.Fatalf("RemoveRoute(planner failure) = %v, calls %d, begins %d", err, plans.calls, repository.begins)
	}
}

func routeRemovalTestNormalizedCompose(t *testing.T) []byte {
	t.Helper()
	project := &composetypes.Project{Services: composetypes.Services{
		"api": {Name: "api", Image: "api:1"},
	}}
	normalized, err := testcomposerender.MarshalNormalizedEnvironmentProject(project)
	if err != nil {
		t.Fatalf("marshal Route removal fixture normalized Compose: %v", err)
	}
	return normalized
}

func routeRemovalTestComposeArtifact(
	projection testenvironmentprojection.EnvironmentComposeProjection,
	volumeDir string,
) []byte {
	canonicalYAML := []byte("services: {}\n")
	digest := sha256.Sum256(canonicalYAML)
	services := make([]*agentpb.ComposeService, len(projection.DesiredServices))
	for index, service := range projection.DesiredServices {
		services[index] = &agentpb.ComposeService{
			ServiceId: service.Desired.ID, ComposeName: service.Desired.Name,
		}
		for _, component := range projection.Components {
			if component.Desired.Enabled && slices.Contains(component.Runtime.GeneratedServices, service.Desired.ID) {
				services[index].OwnerComponentId = component.Desired.ID
				image := routeRemovalTestOCIImage()
				platform := image.Platforms[0]
				services[index].ImageRepository = image.Repository
				services[index].ImageReference = image.Repository + "@sha256:" + platform.ChildDigest
				services[index].ImageSelectedTag = image.Tag
				services[index].ImageOs = platform.OS
				services[index].ImageArchitecture = platform.Architecture
				services[index].ImageVariant = platform.Variant
				for _, digest := range []struct {
					text string
					out  *[]byte
				}{
					{image.IndexDigest, &services[index].ImageIndexDigest},
					{platform.ChildDigest, &services[index].ImageChildDigest},
					{platform.ConfigDigest, &services[index].ImageConfigDigest},
				} {
					value, err := hex.DecodeString(digest.text)
					if err != nil {
						panic(err)
					}
					*digest.out = value
				}
			}
		}
	}
	value, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		OwnerKind:  agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:    projection.EnvironmentID, ProjectName: "gp-" + strings.ToLower(projection.EnvironmentID),
		CanonicalYaml: canonicalYAML, YamlSha256: digest[:],
		AuthorizedVolumeDir: volumeDir, Services: services,
	})
	if err != nil {
		panic(err)
	}
	return value
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
	idempotency.locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: repository.environment.Record.ID,
		Method: http.MethodDelete, Route: routeDeletionRoute, Key: "route-remove-key-0003",
	}
	idempotency.replay = testidempotency.IdempotencyResponse{
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
