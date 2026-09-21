package app

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
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
	testscriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func manualScriptLifecycleContextFixture(t *testing.T, executionContext *core.ScriptExecution) (
	*memoryHierarchyStore, testscriptsourcequeries.ScriptExecutionSources, testscriptexecutions.ScriptExecutionRecord, etcd.TaskRecord, testidempotency.IdempotencyMarker,
) {
	t.Helper()
	ctx := context.Background()
	_, store, environment, project, target := routeRepositoryTestHierarchy(t)
	repository, err := etcd.NewScriptRepository(store)
	if err != nil {
		t.Fatal(err)
	}
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
