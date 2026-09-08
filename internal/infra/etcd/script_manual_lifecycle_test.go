package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: a visible manual Task must own the complete prepared source root;
// the old body-only transaction leaves no legal bounded terminal-release path.
func TestManualScriptPublicationActivatesPreparedSources(t *testing.T) {
	store, sources, execution, task, marker := manualScriptLifecycleFixture(t)
	repository := &ScriptRepository{store: store}
	result, err := repository.PublishExecutionWithTask(context.Background(), sources, execution, task, marker)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("manual publication = %v, %v, %v", outcome, conflict, err)
	}
	rootValue := store.valueAt(scriptSourceRootKey(task.OperationID), store.revision)
	if rootValue == nil {
		t.Fatal("visible manual Task has no immutable source root")
	}
	root, err := decodeScriptOperationSourceRoot(rootValue.Value)
	if err != nil || root.Phase != ScriptOperationSourceActive || root.MembershipCount != 4 {
		t.Fatalf("published manual source root = %#v, %v", root, err)
	}
	if store.valueAt(scriptSourcePreparationKey(task.OperationID), store.revision) != nil {
		t.Fatal("activated source set retained its preparation descriptor")
	}
	if rootValue.ModRevision != store.valueAt(taskKey(task.ID), store.revision).ModRevision {
		t.Fatal("source root and Task were not published atomically")
	}
	script, err := repository.GetScript(context.Background(), task.Target)
	if err != nil || script.Record.ActiveReferences != 1 {
		t.Fatalf("manual publication Script count = %d, %v", script.Record.ActiveReferences, err)
	}
	if store.valueAt(scriptBodyReverseReferenceKey(execution.ID), store.revision) != nil {
		t.Fatal("manual publication retained the superseded body-only reference protocol")
	}
}

// Rationale: assignment must compare the exact source-set identity sealed with
// the plan, not merely trust that a record exists under the operation's key.
func TestManualScriptClaimRejectsChangedSourceRoot(t *testing.T) {
	store, sources, execution, task, marker := manualScriptLifecycleFixture(t)
	result, err := (&ScriptRepository{store: store}).PublishExecutionWithTask(
		context.Background(),
		sources,
		execution,
		task,
		marker,
	)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("publish = %#v, %v", result, err)
	}
	rootValue := store.valueAt(scriptSourceRootKey(task.OperationID), store.revision)
	root, err := decodeScriptOperationSourceRoot(rootValue.Value)
	if err != nil {
		t.Fatal(err)
	}
	root.MembershipSHA256 = scriptSourceReferenceDigest("substituted source set")
	value, err := encodeEnvelope("script-operation-source-root", root)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := store.Transact(context.Background(), nil, []Mutation{{
		Type: MutationPut, Key: rootValue.Key, Value: value,
	}})
	if err != nil || !changed.Succeeded {
		t.Fatalf("substitute root = %#v, %v", changed, err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	_, found, err := tasks.ClaimNextTask(
		context.Background(),
		ids.NewAt(ids.KindAgent, task.CreatedAt, 99),
		1,
		task.CreatedAt.Add(time.Second),
	)
	if found || err == nil {
		t.Fatalf("claim accepted substituted source root: found=%t, error=%v", found, err)
	}
}

// Rationale: pending Abort proves that start never became authorized, drains
// only its own references, and terminalizes together with final root removal.
func TestManualScriptPendingAbortReleasesSourcesAndReplays(t *testing.T) {
	store, sources, execution, task, marker := manualScriptLifecycleFixture(t)
	scripts := &ScriptRepository{store: store}
	result, err := scripts.PublishExecutionWithTask(context.Background(), sources, execution, task, marker)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("publish = %#v, %v", result, err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := tasks.AbortPendingTask(context.Background(), task.ID, task.CreatedAt.Add(time.Second))
	if err != nil || terminal.Record.Status != TaskStatusAborted {
		t.Fatalf("pending Abort = %s, %v", terminal.Record.Status, err)
	}
	if store.valueAt(scriptSourceRootKey(task.OperationID), store.revision) != nil {
		t.Fatal("terminal pending Abort retained its source root")
	}
	retained, err := scripts.GetScript(context.Background(), task.Target)
	if err != nil || retained.Record.ActiveReferences != 0 {
		t.Fatalf("pending Abort retained Script references: %d, %v", retained.Record.ActiveReferences, err)
	}
	closed, err := scripts.GetScriptExecution(context.Background(), execution.ID)
	if err != nil || closed.Record.State != ScriptExecutionCleanupProven || closed.Record.ActiveReference ||
		closed.Record.Outcome == nil || closed.Record.Outcome.Reason != ScriptOutcomeAbortBeforeStart {
		t.Fatalf("pending Abort execution cleanup = %s, %v", closed.Record.State, err)
	}
	revision := store.revision
	replayed, err := tasks.AbortPendingTask(context.Background(), task.ID, task.CreatedAt.Add(2*time.Second))
	if err != nil || replayed.Revision != terminal.Revision || store.revision != revision {
		t.Fatalf("pending Abort replay changed state: %d -> %d, %v", revision, store.revision, err)
	}
}

func manualScriptLifecycleFixture(t *testing.T) (
	*memoryHierarchyStore, ScriptExecutionSources, ScriptExecutionRecord, TaskRecord, IdempotencyMarker,
) {
	t.Helper()
	ctx := context.Background()
	_, store, environment, project, target := routeRepositoryTestHierarchy(t)
	repository := &ScriptRepository{store: store}
	at := environment.Record.CreatedAt.Add(time.Minute)
	script, err := NewScriptRecord(environment.Record.ID, target.Record.Desired.ID, core.Script{
		ID: ids.NewAt(ids.KindScript, at, 1), Slug: "manual-proof", ServiceName: target.Record.Desired.Name,
		Body: "exit 0", When: core.ScriptManual,
	})
	if err != nil {
		t.Fatal(err)
	}
	created, err := repository.CreateScript(ctx, environment, project, target, script)
	if err != nil {
		t.Fatal(err)
	}
	execution := scriptCheckpointTestRecord(at)
	execution.ScriptID, execution.EnvironmentID, execution.ServiceID = script.Desired.ID, environment.Record.ID, target.Record.Desired.ID
	execution.ScriptSetGeneration = created.Record.ScriptSetGeneration
	execution.BodySHA256 = scriptSourceReferenceDigest(script.Desired.Body)
	task := validTaskRecord(at)
	task.ID, task.OperationID = execution.CurrentTaskID, execution.OperationID
	task.Owner = mustEnvironmentTaskOwner(t, project.Record, environment.Record)
	task.Actor, task.Executor, task.Type, task.Target = TaskActorOperator, TaskExecutorAgent, TaskScript, execution.ScriptID
	task.PlanHash, task.RenderGeneration = execution.PlanHash, int32(execution.RenderGeneration)
	task.Params = map[string]string{ScriptExecutionIDParam: execution.ID, ScriptGenerationParam: "1"}
	task.Steps = []TaskStepRecord{{Kind: TaskStepOperation, ID: execution.StepID}}
	task.TimeoutSeconds = executionplan.ScriptExecutionTimeoutSeconds
	intent := domain.Intent{
		ID: execution.ReleaseID, EnvironmentID: execution.EnvironmentID, ServiceID: execution.ServiceID,
		OperationID: ids.NewAt(ids.KindOperation, at, 21), OperationKind: domain.OperationDeploy,
		CandidateWorkload: releaseTestWorkloadSeal("registry.example/app:v1"), Tag: "v1",
		Strategy: domain.StrategyRecreate, OnFailure: domain.OnFailureLeaveActive,
		RenderInputID: ids.NewAt(ids.KindConfig, at, 22), RenderInputDigest: scriptSourceReferenceDigest("render"),
		CreatedAt: at, Actor: "test", OriginatingTaskID: ids.NewAt(ids.KindTask, at, 23),
		Workspace: domain.Workspace{Kind: domain.WorkspaceTenant, TenantID: project.Record.TenantID,
			ProjectID: project.Record.ID, EnvironmentID: environment.Record.ID},
	}
	intentValue, err := encodeEnvelope("release-intent", intent)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := store.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: releaseIntentStagingKey("", intent.ID), Value: intentValue},
		{Type: MutationPut, Key: releaseProjectionKey(execution.ServiceID), Value: []byte("serving revision fence")},
		{Type: MutationPut, Key: releaseRenderInputStagingKey("", intent.ID), Value: []byte("immutable render fence")},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed serving source authority = %#v, %v", seed, err)
	}
	revision := seed.Revision
	head, projection, err := loadScriptExecutionDesiredProjection(ctx, store, environment.Record.ID, "", revision)
	if err != nil {
		t.Fatal(err)
	}
	target, err = findServiceAtRevision(ctx, store, target.Record.Desired.ID, revision)
	if err != nil {
		t.Fatal(err)
	}
	storage, err := readActiveScriptStorage(ctx, store, script.Desired.ID, revision)
	if err != nil {
		t.Fatal(err)
	}
	bodyKey := scriptSetBodyGenerationKey(
		environment.Record.ID,
		created.Record.ScriptSetGeneration,
		script.Desired.ID,
		1,
	)
	bodyValue := store.valueAt(bodyKey, revision)
	body, err := decodeScriptBodyGeneration(bodyValue.Value)
	if err != nil {
		t.Fatal(err)
	}
	storage.Script.Record.Desired.Body = body.Body
	active, err := readActiveScriptSet(ctx, store, environment.Record.ID, revision)
	if err != nil {
		t.Fatal(err)
	}
	tenantValue := store.valueAt(tenantKey(project.Record.TenantID), revision)
	tenant, err := decodeTenant(tenantValue.Value)
	if err != nil {
		t.Fatal(err)
	}
	environment.ReadRevision, project.ReadRevision = revision, revision
	sources := ScriptExecutionSources{
		Revision: revision, Tenant: Versioned[TenantRecord]{Record: tenant, Revision: tenantValue.ModRevision, ReadRevision: revision},
		Project: project, Environment: environment, Service: target, ScriptSet: active, Script: storage.Script,
		BodyGeneration: Versioned[ScriptBodyGenerationRecord]{
			Record:       body,
			Revision:     bodyValue.ModRevision,
			ReadRevision: revision,
		},
		Release: ServingRelease{
			Intent:             intent,
			IntentRevision:     revision,
			ProjectionRevision: revision,
			Revision:           revision,
		},
		RenderInput: Versioned[ReleaseRenderInput]{Record: ReleaseRenderInput{
			ReleaseID: intent.ID, Projection: projection.Record,
		}, Revision: revision, ReadRevision: revision},
		DesiredHead: head, DesiredProjection: projection,
	}
	sources.Networks, err = resolveScriptExecutionNetworks(projection)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &agentpb.ResolvedRunnerSnapshot{
		SnapshotId: execution.SnapshotID, ScriptExecutionId: execution.ID, EnvironmentId: execution.EnvironmentID,
		ServiceId: execution.ServiceID, ReleaseId: execution.ReleaseID,
	}
	execution.Snapshot, err = proto.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	execution.SnapshotSHA256 = scriptSourceReferenceBytesDigest(execution.Snapshot)
	marker := pendingTaskMarker(task)
	marker.Locator.ScopeID, marker.Locator.Route = environment.Record.ID, "/scripts/{id}/run"
	return store, sources, execution, task, marker
}
