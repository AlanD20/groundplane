package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/runtimeconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: SVC-15/JOURNEY-02 Route create/edit publication must persist the
// exact configuration authority on the Task that owns its generated file.
func TestRouteMutationFileWriterPublishesConfigurationAuthority(t *testing.T) {
	ctx := context.Background()
	repository, store, environment, project, target := routeRepositoryTestHierarchy(t)
	selected, found, err := currentEnvironmentProjectionAtRevision(ctx, store, environment.Record.ID, 0)
	if err != nil || !found {
		t.Fatalf("currentEnvironmentProjectionAtRevision() = %#v/%v/%v", selected, found, err)
	}
	record := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1400, "/config/*")
	createdAt := serviceRecordTestTime().Add(8 * time.Hour)
	task := routeConfigurationWriterTask(t, project, environment, record, selected, createdAt)
	intent, err := NewRouteMutationIntent(
		task.ID, task.OperationID, environment.Record.ID, record, nil, &selected, createdAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := ApplyEnvironmentRoute(selected.Record, record)
	if err != nil {
		t.Fatal(err)
	}
	candidate.RevisionID = task.ID
	intent.CandidateProjection = &candidate
	intent.Provider = routeRepositoryTestProviderPin(target.Record.Desired.ID)
	marker := routeMutationAcceptanceMarker(t, task, environment.Record.ID, http.MethodPost, "/routes")
	result, err := repository.BeginRouteMutationWithTask(
		ctx, environment, project, target, nil, record, intent, task, marker,
	)
	if err != nil {
		t.Fatalf("BeginRouteMutationWithTask() error = %v", err)
	}
	assertAppliedResult(t, result)
	assertPublishedTaskConfiguration(t, store, task.ID)
}

// Rationale: SVC-15/JOURNEY-02 Route removal publication must bind the exact
// candidate file before the Route remains pending behind Agent acknowledgement.
func TestRouteRemovalFileWriterPublishesConfigurationAuthority(t *testing.T) {
	ctx := context.Background()
	repository, store, environment, project, target := routeRepositoryTestHierarchy(t)
	record := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1410, "/remove/*")
	projection := routeDeletionTestProjection(
		t, store, environment, project, target, Versioned[RouteRecord]{Record: record},
	)
	current, err := repository.GetRoute(ctx, record.Desired.ID)
	if err != nil {
		t.Fatal(err)
	}
	task, marker, tombstone, intent := routeDeletionTestRecords(t, project, environment, current, projection)
	intent.Provider = routeRepositoryTestProviderPin(target.Record.Desired.ID)
	task.Executor = TaskExecutorAgent
	task.TimeoutSeconds = 120
	task.Params = map[string]string{
		TaskRouteEnvironmentParam: environment.Record.ID, TaskMaterializationEnvironmentParam: environment.Record.ID,
		EnvironmentDesiredRevisionParam: intent.CandidateProjection.RevisionID,
		TaskComposeArtifactParam:        ids.New(ids.KindConfig),
	}
	attachComponentConfiguration(t, &task, environment.Record.ID, intent.CandidateProjection.RevisionID)
	tombstone.Phase = DeletionPhaseHostEffects
	result, err := repository.BeginRouteDeletionWithTask(
		ctx, environment, project, target, current, &projection, tombstone, intent, task, marker,
	)
	if err != nil {
		t.Fatalf("BeginRouteDeletionWithTask() error = %v", err)
	}
	assertAppliedResult(t, result)
	assertPublishedTaskConfiguration(t, store, task.ID)
}

// Rationale: SEC-07/SVC-15 missing or stale acknowledged configuration cannot
// authorize a file-writing Task, and the compare remains atomic with publication.
func TestConfigurationWriterRejectsMissingAndStaleHead(t *testing.T) {
	ctx := context.Background()
	t.Run("missing", func(t *testing.T) {
		store := newMemoryHierarchyStore()
		task := nonSecretConfigurationTaskFixture()
		projectionValue, err := encodeEnvironmentComposeProjection(withTestEnvironmentComposeArtifact(
			EnvironmentComposeProjection{EnvironmentID: task.Owner.EnvironmentID,
				RevisionID: ids.New(ids.KindTask), RenderGeneration: 1},
		))
		if err != nil {
			t.Fatal(err)
		}
		seed, err := store.Transact(ctx, nil, []Mutation{{Type: MutationPut,
			Key: environmentComposeProjectionKey(task.Owner.EnvironmentID), Value: projectionValue}})
		clear(projectionValue)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := prepareConfigurationTaskPublication(
			ctx, store, task, seed.Revision,
		); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			t.Fatalf("missing head error = %v", err)
		}
	})
	t.Run("stale", func(t *testing.T) {
		store := newMemoryHierarchyStore()
		task := nonSecretConfigurationTaskFixture()
		headValue := stageTestRuntimeConfiguration(t, store, task.Owner.EnvironmentID, 1)
		seed, err := store.Transact(ctx, nil, []Mutation{{Type: MutationPut,
			Key: runtimeConfigurationHeadKey(task.Owner.EnvironmentID), Value: headValue}})
		clear(headValue)
		if err != nil {
			t.Fatal(err)
		}
		publication, err := prepareConfigurationTaskPublication(ctx, store, task, seed.Revision)
		if err != nil {
			t.Fatal(err)
		}
		defer publication.clear()
		conditions, mutations, classify, err := publication.bind(nil, []Mutation{{
			Type: MutationPut, Key: taskKey(task.ID), Value: []byte("Task"),
		}}, func(int64, []*KeyValue) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Transact(ctx, nil, []Mutation{{Type: MutationPut,
			Key: runtimeConfigurationHeadKey(task.Owner.EnvironmentID), Value: []byte(`{"changed":true}`)}}); err != nil {
			t.Fatal(err)
		}
		result, err := store.Transact(ctx, conditions, mutations)
		if err != nil || result.Succeeded || !errors.Is(
			classify(result.Revision, result.FailureReads), errs.New(errs.KindStateConflict, ""),
		) {
			t.Fatalf("stale publication = %#v/%v", result, err)
		}
	})
}

func routeConfigurationWriterTask(
	t *testing.T,
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	record RouteRecord,
	projection Versioned[EnvironmentComposeProjection],
	createdAt time.Time,
) TaskRecord {
	t.Helper()
	task := validTaskRecord(createdAt)
	task.ID, task.OperationID, task.PlanID = ids.New(ids.KindTask), ids.New(ids.KindOperation), ids.New(ids.KindPlan)
	task.Owner = mustEnvironmentTaskOwner(t, project.Record, environment.Record)
	task.Executor, task.Type, task.Target = TaskExecutorAgent, TaskCreate, record.Desired.ID
	task.Params = map[string]string{
		TaskResourceKindParam: TaskResourceRoute, TaskRouteEnvironmentParam: environment.Record.ID,
		TaskMaterializationEnvironmentParam: environment.Record.ID,
		EnvironmentDesiredRevisionParam:     task.ID, TaskComposeArtifactParam: ids.New(ids.KindConfig),
	}
	task.TimeoutSeconds = 120
	task.RenderGeneration = int32(projection.Record.RenderGeneration + 1)
	task.IdempotencyKey = "route-config-task-key-0001"
	attachComponentConfiguration(t, &task, environment.Record.ID, task.ID)
	return task
}

func attachComponentConfiguration(t *testing.T, task *TaskRecord, environmentID, revisionID string) {
	t.Helper()
	content := []byte("exact generated route file\n")
	digest := sha256.Sum256(content)
	task.Materializations = []TaskMaterializationRecord{{
		StepID: task.Steps[0].ID, MaterializationID: ids.New(ids.KindConfig), EnvironmentID: environmentID,
		Destination: "components/router/config", OutputKind: TaskMaterializationOutputPlainFile, Mode: 0o444,
		Length: uint64(len(content)), SHA256: hex.EncodeToString(digest[:]),
		Source: TaskMaterializationSource{Kind: TaskMaterializationSourceComponentFile,
			ComponentFile: &TaskComponentFileValueReference{
				RevisionID: revisionID, ComponentID: ids.New(ids.KindComponent), Path: "components/router/config",
			}},
	}}
}

func attachRemovalConfiguration(task *TaskRecord, environmentID string) {
	digest := sha256.Sum256(nil)
	task.Materializations = []TaskMaterializationRecord{{
		StepID: task.Steps[0].ID, MaterializationID: ids.New(ids.KindConfig), EnvironmentID: environmentID,
		Destination: "config/removed-entry", OutputKind: TaskMaterializationOutputRemovePlainFile, Mode: 0o444,
		SHA256: hex.EncodeToString(
			digest[:],
		), Source: TaskMaterializationSource{Kind: TaskMaterializationSourceRemoval},
	}}
}

func nonSecretConfigurationTaskFixture() TaskRecord {
	task := configurationTaskFixture()
	task.Materializations = task.Materializations[:2]
	return task
}

func routeMutationAcceptanceMarker(
	t *testing.T, task TaskRecord, environmentID, method, route string,
) IdempotencyMarker {
	t.Helper()
	marker, err := NewCompletedDirectIdempotencyMarker(
		IdempotencyLocator{ScopeKind: IdempotencyScopeEnvironment, ScopeID: environmentID,
			Method: method, Route: route, Key: task.IdempotencyKey},
		testDirectMarker().Intent,
		IdempotencyResponse{Status: http.StatusAccepted, ContentKind: "application/json",
			Body: []byte(`{"route":{"id":"` + task.Target + `"},"task_id":"` + task.ID + `"}`)},
		task.CreatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return marker
}

func stageTestRuntimeConfiguration(
	t *testing.T, store *memoryHierarchyStore, environmentID string, generation uint64,
) []byte {
	t.Helper()
	repository, err := runtimeconfiguration.NewRepository(runtimeConfigurationStore{store: store})
	if err != nil {
		t.Fatal(err)
	}
	reference, err := repository.Stage(context.Background(), runtimeconfiguration.Snapshot{
		ID: ids.New(ids.KindConfig), EnvironmentID: environmentID, Generation: generation,
		Files: []TaskMaterializationRecord{},
	})
	if err != nil {
		t.Fatal(err)
	}
	value, err := runtimeconfiguration.EncodeReference(reference)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func seedTestRuntimeConfigurationHead(
	t *testing.T, store *memoryHierarchyStore, environmentID string, generation uint64,
) int64 {
	t.Helper()
	value := stageTestRuntimeConfiguration(t, store, environmentID, generation)
	result, err := store.Transact(context.Background(), nil, []Mutation{{
		Type: MutationPut, Key: runtimeConfigurationHeadKey(environmentID), Value: value,
	}})
	clear(value)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed runtime configuration = %#v/%v", result, err)
	}
	return result.Revision
}

func assertPublishedTaskConfiguration(t *testing.T, store *memoryHierarchyStore, taskID string) TaskRecord {
	t.Helper()
	read, err := store.Get(context.Background(), taskKey(taskID))
	if err != nil || read.Entry == nil {
		t.Fatalf("Get(Task) = %#v/%v", read, err)
	}
	task, err := decodeTaskRecord(read.Entry.Value)
	if err != nil || task.Configuration == nil {
		t.Fatalf("published Task configuration = %#v/%v", task.Configuration, err)
	}
	return task
}

func assertAppliedResult(t *testing.T, result IdempotencyTransactionResult) {
	t.Helper()
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("publication result = %v/%v/%v", outcome, conflict, err)
	}
}

func assertRuntimeConfigurationHead(
	t *testing.T, store *memoryHierarchyStore, task TaskRecord, shouldEqualCurrent bool,
) {
	t.Helper()
	read, err := store.Get(context.Background(), runtimeConfigurationHeadKey(task.Owner.EnvironmentID))
	if err != nil || read.Entry == nil {
		t.Fatalf("Get(configuration head) = %#v/%v", read, err)
	}
	reference, err := runtimeconfiguration.DecodeReference(read.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	equal := task.Configuration != nil && reference == task.Configuration.Current
	if equal != shouldEqualCurrent {
		t.Fatalf("configuration head matches candidate = %t, want %t", equal, shouldEqualCurrent)
	}
}
