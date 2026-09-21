package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testreleasequeries "github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testscriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testscriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	testscriptsourcereference "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: a visible manual Task must own the complete prepared source root;
// the old body-only transaction leaves no legal bounded terminal-release path.
func TestManualScriptPublicationActivatesPreparedSources(t *testing.T) {
	store, sources, execution, task, marker := manualScriptLifecycleFixture(t)
	repository := composeScriptRepository(store)
	result, err := repository.PublishExecutionWithTask(context.Background(), sources, execution, task, marker)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("manual publication = %v, %v, %v", outcome, conflict, err)
	}
	rootValue := store.valueAt(testscriptsourceevidence.ScriptSourceRootKey(task.OperationID), store.revision)
	if rootValue == nil {
		t.Fatal("visible manual Task has no immutable source root")
	}
	root, err := testscriptsourceevidence.DecodeScriptOperationSourceRoot(rootValue.Value)
	if err != nil || root.Phase != testscriptsourceevidence.ScriptOperationSourceActive || root.MembershipCount != 4 {
		t.Fatalf("published manual source root = %#v, %v", root, err)
	}
	if store.valueAt(testscriptsourcereference.PreparationKey(task.OperationID), store.revision) != nil {
		t.Fatal("activated source set retained its preparation descriptor")
	}
	if rootValue.ModRevision != store.valueAt(testtaskjournal.TaskStorageKey(task.ID), store.revision).ModRevision {
		t.Fatal("source root and Task were not published atomically")
	}
	script, err := repository.GetScript(context.Background(), task.Target)
	if err != nil || script.Record.ActiveReferences != 1 {
		t.Fatalf("manual publication Script count = %d, %v", script.Record.ActiveReferences, err)
	}
	if store.valueAt(testscriptexecutions.ScriptBodyReverseReferenceKey(execution.ID), store.revision) != nil {
		t.Fatal("manual publication retained the superseded body-only reference protocol")
	}
}

// Rationale: assignment must compare the exact source-set identity sealed with
// the plan, not merely trust that a record exists under the operation's key.
func TestManualScriptClaimRejectsChangedSourceRoot(t *testing.T) {
	store, sources, execution, task, marker := manualScriptLifecycleFixture(t)
	result, err := (composeScriptRepository(store)).PublishExecutionWithTask(
		context.Background(),
		sources,
		execution,
		task,
		marker,
	)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("publish = %#v, %v", result, err)
	}
	rootValue := store.valueAt(testscriptsourceevidence.ScriptSourceRootKey(task.OperationID), store.revision)
	root, err := testscriptsourceevidence.DecodeScriptOperationSourceRoot(rootValue.Value)
	if err != nil {
		t.Fatal(err)
	}
	root.MembershipSHA256 = scriptSourceReferenceDigest("substituted source set")
	value, err := testrecordcodec.Encode("script-operation-source-root", root)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: rootValue.Key, Value: value,
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
	scripts := composeScriptRepository(store)
	result, err := scripts.PublishExecutionWithTask(context.Background(), sources, execution, task, marker)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("publish = %#v, %v", result, err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := tasks.AbortPendingTask(context.Background(), task.ID, task.CreatedAt.Add(time.Second))
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusAborted {
		t.Fatalf("pending Abort = %s, %v", terminal.Record.Status, err)
	}
	if store.valueAt(testscriptsourceevidence.ScriptSourceRootKey(task.OperationID), store.revision) != nil {
		t.Fatal("terminal pending Abort retained its source root")
	}
	retained, err := scripts.GetScript(context.Background(), task.Target)
	if err != nil || retained.Record.ActiveReferences != 0 {
		t.Fatalf("pending Abort retained Script references: %d, %v", retained.Record.ActiveReferences, err)
	}
	closed, err := scripts.GetScriptExecution(context.Background(), execution.ID)
	if err != nil || closed.Record.State != testscriptexecutions.ScriptExecutionCleanupProven ||
		closed.Record.ActiveReference ||
		closed.Record.Outcome == nil ||
		closed.Record.Outcome.Reason != testscriptexecutions.ScriptOutcomeAbortBeforeStart {
		t.Fatalf("pending Abort execution cleanup = %s, %v", closed.Record.State, err)
	}
	revision := store.revision
	replayed, err := tasks.AbortPendingTask(context.Background(), task.ID, task.CreatedAt.Add(2*time.Second))
	if err != nil || replayed.Revision != terminal.Revision || store.revision != revision {
		t.Fatalf("pending Abort replay changed state: %d -> %d, %v", revision, store.revision, err)
	}
}

func manualScriptLifecycleFixture(t *testing.T) (
	*memoryHierarchyStore, testscriptsourcequeries.ScriptExecutionSources, testscriptexecutions.ScriptExecutionRecord, TaskRecord, testidempotency.IdempotencyMarker,
) {
	return manualScriptLifecycleContextFixture(t, nil)
}

func manualScriptLifecycleContextFixture(t *testing.T, executionContext *core.ScriptExecution) (
	*memoryHierarchyStore, testscriptsourcequeries.ScriptExecutionSources, testscriptexecutions.ScriptExecutionRecord, TaskRecord, testidempotency.IdempotencyMarker,
) {
	t.Helper()
	ctx := context.Background()
	_, store, environment, project, target := routeRepositoryTestHierarchy(t)
	repository := composeScriptRepository(store)
	at := environment.Record.CreatedAt.Add(time.Minute)
	script, err := testscripts.NewRecord(environment.Record.ID, target.Record.Desired.ID, core.Script{
		ID: ids.NewAt(ids.KindScript, at, 1), Slug: "manual-proof", ServiceName: target.Record.Desired.Name,
		Body: "exit 0", When: core.ScriptManual, Execution: executionContext,
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
	task.Actor, task.Executor, task.Type, task.Target = testtaskjournal.TaskActorOperator, testtaskjournal.TaskExecutorAgent, testtaskjournal.TaskScript, execution.ScriptID
	task.PlanHash, task.RenderGeneration = execution.PlanHash, int32(execution.RenderGeneration)
	task.Params = map[string]string{
		testscriptexecutions.ScriptExecutionIDParam: execution.ID,
		testscriptexecutions.ScriptGenerationParam:  "1",
	}
	task.Steps = []testtaskjournal.TaskStepRecord{{Kind: testtaskjournal.TaskStepOperation, ID: execution.StepID}}
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
	intentValue, err := testrecordcodec.Encode("release-intent", intent)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testreleases.ReleaseIntentStagingKey("", intent.ID), Value: intentValue},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testreleases.ReleaseProjectionKey(execution.ServiceID),
			Value: []byte("serving revision fence"),
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testreleases.ReleaseRenderInputStagingKey("", intent.ID),
			Value: []byte("immutable render fence"),
		},
	})
	if err != nil || !seed.Succeeded {
		t.Fatalf("seed serving source authority = %#v, %v", seed, err)
	}
	revision := seed.Revision
	projection, found, err := testenvironmentqueries.NewProjectionReader(store).
		GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil || !found || projection.ReadRevision != revision {
		t.Fatal(err)
	}
	head := testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintHead]{
		Record: testblueprints.EnvironmentBlueprintHead{
			EnvironmentID: environment.Record.ID,
			RevisionID:    projection.Record.RevisionID,
		},
		Revision:     projection.Revision,
		ReadRevision: projection.ReadRevision,
	}
	root := store.valueAt(
		testblueprints.EnvironmentBlueprintRootKey(environment.Record.ID, projection.Record.RevisionID),
		revision,
	)
	if root == nil {
		t.Fatal("missing immutable Blueprint root")
	}
	projection.Revision = root.ModRevision
	target, err = testenvironmentqueries.FindServiceAtRevision(ctx, store, target.Record.Desired.ID, revision)
	if err != nil {
		t.Fatal(err)
	}
	storage, err := testscripts.ReadActiveScriptStorage(ctx, store, script.Desired.ID, revision)
	if err != nil {
		t.Fatal(err)
	}
	bodyKey := testscripts.ScriptSetBodyGenerationKey(
		environment.Record.ID,
		created.Record.ScriptSetGeneration,
		script.Desired.ID,
		1,
	)
	bodyValue := store.valueAt(bodyKey, revision)
	body, err := testscripts.DecodeScriptBodyGeneration(bodyValue.Value)
	if err != nil {
		t.Fatal(err)
	}
	storage.Script.Record.Desired.Body = body.Body
	active, err := testscripts.ReadActiveScriptSet(ctx, store, environment.Record.ID, revision)
	if err != nil {
		t.Fatal(err)
	}
	tenantValue := store.valueAt(testhierarchy.TenantKey(project.Record.TenantID), revision)
	tenant, err := testhierarchy.DecodeTenant(tenantValue.Value)
	if err != nil {
		t.Fatal(err)
	}
	environment.ReadRevision, project.ReadRevision = revision, revision
	sources := testscriptsourcequeries.ScriptExecutionSources{
		Revision: revision, Tenant: testkeyvalue.Versioned[testhierarchy.TenantRecord]{Record: tenant, Revision: tenantValue.ModRevision, ReadRevision: revision},
		Project: project, Environment: environment, Service: target, ScriptSet: active, Script: storage.Script,
		BodyGeneration: testkeyvalue.Versioned[testscripts.BodyGenerationRecord]{
			Record:       body,
			Revision:     bodyValue.ModRevision,
			ReadRevision: revision,
		},
		Release: testreleasequeries.ServingRelease{
			Intent:             intent,
			IntentRevision:     revision,
			ProjectionRevision: revision,
			Revision:           revision,
		},
		RenderInput: testkeyvalue.Versioned[testreleaserender.ReleaseRenderInput]{
			Record: testreleaserender.ReleaseRenderInput{
				ReleaseID: intent.ID, Projection: projection.Record,
			},
			Revision:     revision,
			ReadRevision: revision,
		},
		DesiredHead: head, DesiredProjection: projection,
	}
	for _, desired := range projection.Record.DesiredZones {
		network, joinErr := testenvironmentqueries.JoinZone(projection, desired)
		if joinErr != nil {
			t.Fatal(joinErr)
		}
		sources.Networks = append(sources.Networks, network)
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
