package etcd

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/runtimeconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SVC-15/JOURNEY-02: staged and failed configuration cannot replace the last
// working inputs, and those inputs must remain readable after Task pruning.
func TestRuntimeConfigurationAcknowledgesOnlySuccessAndSurvivesTaskPruning(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	task := configurationTaskFixture()
	seed, err := store.Transact(
		ctx,
		nil,
		[]Mutation{{Type: MutationPut, Key: taskKey(task.ID), Value: []byte("old Task")}},
	)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := prepareRuntimeConfigurationTask(ctx, store, task, seed.Revision)
	if err != nil {
		t.Fatal(err)
	}
	key := runtimeConfigurationHeadKey(task.Owner.EnvironmentID)
	if value := store.valueAt(key, store.revision); value != nil {
		t.Fatal("preparation promoted configuration before any acknowledgement")
	}
	for _, status := range []TaskStatus{TaskStatusPending, TaskStatusRunning, TaskStatusFailed, TaskStatusTimedOut} {
		prepared.Status = status
		change, prepareErr := prepareRuntimeConfigurationAcknowledgement(prepared)
		if prepareErr != nil || change.applies || len(change.mutations) != 0 {
			t.Fatalf("non-success %s promoted configuration: %#v, %v", status, change, prepareErr)
		}
	}
	prepared.Status = TaskStatusCompleted
	change, err := prepareRuntimeConfigurationAcknowledgement(prepared)
	if err != nil {
		t.Fatal(err)
	}
	commit, err := store.Transact(ctx, change.conditions, change.mutations)
	if err != nil || !commit.Succeeded {
		t.Fatalf("acknowledgement = %#v, %v", commit, err)
	}
	acknowledged := store.valueAt(key, store.revision)
	if acknowledged == nil ||
		string(acknowledged.Value) != configurationReferenceJSON(t, prepared.Configuration.Current) {
		t.Fatal("successful acknowledgement did not publish the exact prepared source set")
	}
	if _, err := store.Transact(ctx, nil, []Mutation{{Type: MutationDelete, Key: taskKey(task.ID)}}); err != nil {
		t.Fatal(err)
	}
	next := configurationTaskFixture()
	next.ID = ids.New(ids.KindTask)
	next.RenderGeneration++
	next.Materializations = nil
	next, err = prepareRuntimeConfigurationTask(ctx, store, next, store.revision)
	if err != nil || next.Configuration == nil || next.Configuration.Prior == nil ||
		*next.Configuration.Prior != prepared.Configuration.Current || next.Configuration.Current != prepared.Configuration.Current {
		t.Fatalf("Task pruning lost prior configuration: %v", err)
	}
	sources, err := runtimeconfiguration.NewRepository(runtimeConfigurationStore{store: store})
	if err != nil {
		t.Fatal(err)
	}
	reference, err := taskRuntimeConfiguration(next)
	if err != nil || reference == nil {
		t.Fatalf("retained reference = %#v, %v", reference, err)
	}
	snapshot, err := sources.Load(ctx, *reference, store.revision)
	if err != nil || len(snapshot.Files) != 4 {
		t.Fatalf("retained source members = %d, %v", len(snapshot.Files), err)
	}
	for _, original := range task.Materializations {
		matched := false
		for _, retained := range snapshot.Files {
			if retained.Destination == original.Destination {
				matched = retained.SHA256 == original.SHA256 && retained.Mode == original.Mode &&
					retained.UID == original.UID && retained.GID == original.GID && retained.Source.Kind == original.Source.Kind
			}
		}
		if !matched {
			t.Fatalf("lost exact file authority for %s", original.Destination)
		}
	}
}

// SVC-15: source and writer races must fail before a stale Task can write files
// or publish its terminal state. The compare and head update are one transaction.
func TestRuntimeConfigurationFencesClaimAndTerminalPublication(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	task := configurationTaskFixture()
	seed, err := store.Transact(
		ctx,
		nil,
		[]Mutation{{Type: MutationPut, Key: taskKey(task.ID), Value: []byte("Task")}},
	)
	if err != nil {
		t.Fatal(err)
	}
	task, err = prepareRuntimeConfigurationTask(ctx, store, task, seed.Revision)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	conditions, err := repository.runtimeConfigurationClaimConditions(ctx, task, store.revision)
	if err != nil || len(conditions) != 1 || conditions[0].ModRevision != 0 {
		t.Fatalf("initial claim authority = %#v, %v", conditions, err)
	}
	task.Status = TaskStatusCompleted
	change, err := prepareRuntimeConfigurationAcknowledgement(task)
	if err != nil {
		t.Fatal(err)
	}
	headKey := runtimeConfigurationHeadKey(task.Owner.EnvironmentID)
	if _, err := store.Transact(ctx, nil, []Mutation{{Type: MutationPut, Key: headKey,
		Value: []byte(configurationReferenceJSON(t, task.Configuration.Current))}}); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.runtimeConfigurationClaimConditions(ctx, task, store.revision); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("stale claim error = %v", err)
	}
	terminalKey := taskKey(ids.New(ids.KindTask))
	change.mutations = append(
		change.mutations,
		Mutation{Type: MutationPut, Key: terminalKey, Value: []byte("completed")},
	)
	commit, err := store.Transact(ctx, change.conditions, change.mutations)
	if err != nil || commit.Succeeded || store.valueAt(terminalKey, store.revision) != nil {
		t.Fatalf("stale terminal was not atomic: %#v, %v", commit, err)
	}
	task.Configuration.Prior = &runtimeconfiguration.Reference{ID: "malformed"}
	task.Configuration.PriorRevision = 1
	if _, _, err := bindRuntimeConfigurationPublication(task, nil,
		func(int64, []*KeyValue) error { return nil }); err == nil {
		t.Fatal(
			"malformed predecessor reached publication instead of failing before the transaction",
		)
	}
}

func configurationReferenceJSON(t *testing.T, reference runtimeconfiguration.Reference) string {
	t.Helper()
	value, err := runtimeconfiguration.EncodeReference(reference)
	if err != nil {
		t.Fatal(err)
	}
	return string(value)
}

// SVC-15: an old installation with no acknowledgement cannot be silently
// backfilled from desired state or historical Release input.
func TestRuntimeConfigurationRejectsMissingAppliedAuthority(t *testing.T) {
	store := newMemoryHierarchyStore()
	task := configurationTaskFixture()
	seed, err := store.Transact(context.Background(), nil, []Mutation{{Type: MutationPut,
		Key: environmentComposeProjectionKey(
			task.Owner.EnvironmentID,
		), Value: []byte("applied authority")}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = prepareRuntimeConfigurationTask(context.Background(), store, task, seed.Revision)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) ||
		!strings.Contains(err.Error(), "no acknowledged configuration") {
		t.Fatalf("missing acknowledgement error = %v", err)
	}
}

func configurationTaskFixture() TaskRecord {
	task := taskWithMaterializationReferences()
	task.Owner = TaskOwner{WorkspaceType: TaskWorkspaceTenant, EnvironmentID: task.Target,
		TenantID: ids.NewAt(
			ids.KindTenant,
			task.CreatedAt,
			550,
		), ProjectID: ids.NewAt(ids.KindProject, task.CreatedAt, 551)}
	return task
}

// SVC-15: restart and Retry must reuse the captured source set without aliasing
// its predecessor or allowing a foreign Environment into the durable Task.
func TestRuntimeConfigurationTaskCodecAndRetryPreserveAuthority(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	task := configurationTaskFixture()
	seed, err := store.Transact(ctx, nil, []Mutation{{Type: MutationPut, Key: taskKey(task.ID), Value: []byte("Task")}})
	if err != nil {
		t.Fatal(err)
	}
	task, err = prepareRuntimeConfigurationTask(ctx, store, task, seed.Revision)
	if err != nil {
		t.Fatal(err)
	}
	prior := task.Configuration.Current
	task.Configuration.Prior, task.Configuration.PriorRevision = &prior, seed.Revision
	value, err := encodeTaskRecord(task)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := decodeTaskRecord(value)
	if err != nil || restored.Configuration == nil || restored.Configuration.Prior == nil ||
		restored.Configuration.Current != prior || *restored.Configuration.Prior != prior {
		t.Fatalf("Task storage lost configuration authority: %v", err)
	}
	cloned := cloneTaskRecord(restored)
	cloned.Configuration.Prior.ID = ids.New(ids.KindConfig)
	if *restored.Configuration.Prior != prior {
		t.Fatal("cloned Task aliases its source predecessor")
	}
	failed, err := transitionTaskStatus(restored, TaskStatusPending, TaskStatusRunning, task.CreatedAt.Add(1))
	if err != nil {
		t.Fatal(err)
	}
	failed, err = transitionTaskStatus(failed, TaskStatusRunning, TaskStatusFailed, task.CreatedAt.Add(2))
	if err != nil {
		t.Fatal(err)
	}
	retry, err := cloneRetryTask(failed, ids.New(ids.KindTask), TaskActorOperator, task.CreatedAt.Add(3))
	if err != nil || retry.Configuration == nil || retry.Configuration.Current != prior ||
		retry.Configuration.Prior == nil || *retry.Configuration.Prior != prior {
		t.Fatalf("Retry lost pinned configuration: %v", err)
	}
	retry.Configuration.Prior.EnvironmentID = ids.New(ids.KindEnvironment)
	if _, err := encodeTaskRecord(retry); err == nil {
		t.Fatal("durable Task accepted a foreign configuration predecessor")
	}
}
