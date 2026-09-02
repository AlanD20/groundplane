package etcd

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestInitialBlueprintComponentRetryReacquiresNewZoneAddress(t *testing.T) {
	// Rationale: an initial Blueprint with no candidate Releases still owns the
	// canonical no-Compose procedure marker and must reacquire its exact new-Zone
	// Component address even though no applied projection exists yet.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime().Add(50 * time.Second)
	source := validTaskRecord(now)
	source.Type = TaskUpdate
	source.Target = ids.NewAt(ids.KindEnvironment, now, 1540)
	pinComponentTaskDesiredRevision(&source)
	source.Params[componentTaskBlueprintProcedureParam] = componentTaskBlueprintProcedureNone
	createLifecycleTask(t, repository, source)
	records := componentTaskLifecycleRecords(t, source.Target, now, false)
	seedInitialBlueprintComponentTaskLifecycle(t, store, source, records)
	terminalAt := now.Add(time.Second)
	if _, err := repository.AbortPendingTask(ctx, source.ID, terminalAt); err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	retryID := ids.NewAt(ids.KindTask, terminalAt.Add(time.Second), 1541)
	marker := pendingRetryMarker(source, retryID, terminalAt.Add(time.Second), "initial-blueprint-component-retry")
	result, err := repository.RetryTask(ctx, source.ID, retryID, TaskActorOperator, marker)
	if err != nil {
		t.Fatalf("RetryTask() error = %v", err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("RetryTask() outcome/conflict/error = %v/%v/%v", outcome, conflict, err)
	}
	registry := readComponentTaskRegistry(t, store, records.candidateZone)
	if registry.Reservations[records.candidate.Desired.ID] != records.candidate.Runtime.PinnedIPv4 {
		t.Fatal("initial Blueprint retry did not reacquire the exact candidate address")
	}
	retry, err := repository.GetTask(ctx, retryID)
	if err != nil || retry.Record.Params[EnvironmentDesiredRevisionParam] !=
		source.Params[EnvironmentDesiredRevisionParam] {
		t.Fatalf("retry desired revision = %#v, %v", retry, err)
	}
}

func TestNonBlueprintComponentRetryRequiresAppliedProjection(t *testing.T) {
	// Rationale: permitting an unapplied initial Blueprint retry must not weaken
	// the applied-projection fence for ordinary Component reconciliation.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime().Add(60 * time.Second)
	source := validTaskRecord(now)
	source.Type = TaskUpdate
	source.Target = ids.NewAt(ids.KindEnvironment, now, 1550)
	pinComponentTaskDesiredRevision(&source)
	createLifecycleTask(t, repository, source)
	records := componentTaskLifecycleRecords(t, source.Target, now, false)
	seedInitialBlueprintComponentTaskLifecycle(t, store, source, records)
	terminalAt := now.Add(time.Second)
	if _, err := repository.AbortPendingTask(ctx, source.ID, terminalAt); err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	retryID := ids.NewAt(ids.KindTask, terminalAt.Add(time.Second), 1551)
	marker := pendingRetryMarker(source, retryID, terminalAt.Add(time.Second), "ordinary-component-retry")
	_, err = repository.RetryTask(ctx, source.ID, retryID, TaskActorOperator, marker)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("RetryTask() error = %v", err)
	}
}

func TestComponentRetryRejectsMalformedBlueprintProcedureAuthority(t *testing.T) {
	// Rationale: procedure authority is a closed durable marker; missing or
	// inconsistent Release evidence cannot silently downgrade a Blueprint retry
	// to ordinary Component reconciliation.
	t.Parallel()
	tests := []TaskRecord{
		{Params: map[string]string{componentTaskBlueprintProcedureParam: "unknown"}},
		{Params: map[string]string{TaskReleasePublicationParam: ids.NewULID()}},
		{Params: map[string]string{
			componentTaskBlueprintProcedureParam: componentTaskBlueprintProcedureCandidateReleases,
		}},
		{Params: map[string]string{
			componentTaskBlueprintProcedureParam: componentTaskBlueprintProcedureNone,
			TaskReleasePublicationParam:          ids.NewULID(),
		}},
	}
	for index, task := range tests {
		if _, err := componentTaskRetryIsBlueprint(task); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			t.Fatalf("componentTaskRetryIsBlueprint(case %d) error = %v", index, err)
		}
	}
}

func seedInitialBlueprintComponentTaskLifecycle(
	t *testing.T,
	store *memoryTaskStore,
	task TaskRecord,
	records componentTaskLifecycleFixture,
) {
	t.Helper()
	projectID := ids.NewAt(ids.KindProject, task.CreatedAt, 1560)
	environment := EnvironmentRecord{
		ID: task.Target, ProjectID: projectID, Name: "production", NetworkPool: "10.40.0.0/16",
		VolumeDir:         "/var/lib/groundplane/vol/platform/" + projectID + "/" + task.Target,
		ProvisioningState: EnvironmentProvisioningReady, CreateTaskID: task.ID, CreatedAt: task.CreatedAt,
	}
	environmentValue, err := encodeEnvironment(environment)
	if err != nil {
		t.Fatalf("encodeEnvironment() error = %v", err)
	}
	seedTaskRepositoryValue(t, store, environmentKey(task.Target), environmentValue)
	tunnel, err := NewComponentRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, task.CreatedAt, 1561), Owner: core.ComponentOwnerEnvironment,
		OwnerID: task.Target, Kind: core.ComponentKindEdgeCloudflare,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(tunnel) error = %v", err)
	}
	components := []ComponentRecord{records.candidate, tunnel}
	sort.Slice(components, func(left int, right int) bool {
		return components[left].Desired.Kind < components[right].Desired.Kind
	})
	projection := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID: task.Target, RevisionID: task.Params[EnvironmentDesiredRevisionParam],
		RenderGeneration: uint64(task.RenderGeneration),
		DesiredZones: []EnvironmentZoneProjection{
			{EnvironmentID: task.Target, Desired: records.candidateZone.Desired},
			{EnvironmentID: task.Target, Desired: records.currentZone.Desired},
		},
		Components: components,
	})
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil {
		t.Fatalf("unmarshal Compose artifact: %v", err)
	}
	artifact.Services = append(artifact.Services, &agentpb.ComposeService{
		ServiceId: records.candidate.Runtime.GeneratedServices[0], ComposeName: "caddy",
		OwnerComponentId: records.candidate.Desired.ID,
	})
	projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatalf("marshal Compose artifact: %v", err)
	}
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	stageEnvironmentBlueprintForPublicationTest(
		t, hierarchy, 0,
		environmentBlueprintTestRevision(task.Target, task, "services: {}\n"),
		projection, environmentBlueprintTestMarker(task, task.Target),
	)
	headValue, err := encodeTaskReference(task.Params[EnvironmentDesiredRevisionParam])
	if err != nil {
		t.Fatalf("encodeTaskReference() error = %v", err)
	}
	seedTaskRepositoryValue(t, store, environmentBlueprintHeadKey(task.Target), headValue)
	currentValue, err := encodeComponentRecord(records.current)
	if err != nil {
		t.Fatalf("encodeComponentRecord() error = %v", err)
	}
	seedTaskRepositoryValue(t, store, componentKey(records.current.Desired.ID), currentValue)
	stored, err := store.Get(context.Background(), componentKey(records.current.Desired.ID))
	if err != nil || stored.Entry == nil {
		t.Fatalf("Get(Component) = %#v, %v", stored, err)
	}
	registry := componentAddressRegistry{Reservations: map[string]string{
		records.candidate.Desired.ID: records.candidate.Runtime.PinnedIPv4,
	}}
	registryValue, err := encodeComponentAddressRegistry(records.candidateZone, registry)
	if err != nil {
		t.Fatalf("encodeComponentAddressRegistry() error = %v", err)
	}
	seedTaskRepositoryValue(t, store, componentAddressRegistryKey(records.candidateZone.Desired.ID), registryValue)
	intent, err := NewComponentTaskIntent(task.ID, task.Target, []ComponentTaskCandidate{{
		CurrentRevision: stored.Entry.ModRevision, Current: records.current, Candidate: records.candidate,
	}}, task.CreatedAt)
	if err != nil {
		t.Fatalf("NewComponentTaskIntent() error = %v", err)
	}
	intentValue, err := encodeComponentTaskIntent(intent)
	if err != nil {
		t.Fatalf("encodeComponentTaskIntent() error = %v", err)
	}
	seedTaskRepositoryValue(t, store, componentTaskIntentKey(task.ID), intentValue)
	seedTaskRepositoryValue(t, store, componentTaskActiveEnvironmentKey(task.Target), []byte(task.ID))
}
