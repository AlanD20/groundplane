package app

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	scriptcontroller "github.com/AlanD20/groundplane/internal/controller/scripts"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	"github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SCR-04/TASK-54: the actual deletion producer must publish valid native work;
// the definition stays readable until its Controller decision commits, and an
// exact request after deletion replays before looking up the removed Script.
func TestNativeScriptRemovalPublishesSettlesAndReplays(t *testing.T) {
	f := newNativeRouteFixture(t)
	ctx := t.Context()
	hierarchy := f.base.blueprint.HierarchyRepository
	environment, err := hierarchy.GetEnvironment(ctx, f.base.environmentID)
	if err != nil {
		t.Fatal(err)
	}
	project, err := hierarchy.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		t.Fatal(err)
	}
	services, err := etcd.NewServiceRepository(f.base.store)
	if err != nil {
		t.Fatal(err)
	}
	target, err := services.GetService(ctx, f.target.ID)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := etcd.NewScriptRepository(f.base.store)
	if err != nil {
		t.Fatal(err)
	}
	record, err := scripts.NewRecord(environment.Record.ID, target.Record.Desired.ID, core.Script{
		ID: ids.New(ids.KindScript), Slug: "remove-check", ServiceName: target.Record.Desired.Name,
		Body: "exit 0", When: core.ScriptManual,
	})
	if err != nil {
		t.Fatal(err)
	}
	// The reused Volume fixture supplies Environment metadata directly rather
	// than running bootstrap; supply its missing initial Script set as well.
	initialSet, err := scripts.EncodeScriptSetGeneration(scripts.SetGenerationRecord{
		EnvironmentID: environment.Record.ID, GenerationID: environment.Record.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(initialSet)
	setKey := scripts.ScriptSetActiveKey(environment.Record.ID)
	if result, err := f.base.store.Transact(ctx, []keyvalue.Condition{{Key: setKey}}, []keyvalue.Mutation{{
		Type: keyvalue.MutationPut, Key: setKey, Value: initialSet,
	}}); err != nil || !result.Succeeded {
		t.Fatalf("seed initial Script set: %v", err)
	}
	environment, err = hierarchy.GetEnvironment(ctx, environment.Record.ID)
	if err != nil {
		t.Fatal(err)
	}
	project, err = hierarchy.GetProject(ctx, project.Record.ID)
	if err != nil {
		t.Fatal(err)
	}
	target, err = services.GetService(ctx, target.Record.Desired.ID)
	if err != nil {
		t.Fatal(err)
	}
	current, err := repository.CreateScript(ctx, environment, project, target, record)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := etcd.NewTaskRepository(f.base.store)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := etcd.NewReleaseLedger(f.base.store, tasks)
	if err != nil {
		t.Fatal(err)
	}
	mutations, err := scriptcontroller.NewMutationRepository(hierarchy, services, repository, ledger)
	if err != nil {
		t.Fatal(err)
	}
	protector, ciphertext := manualJourneyEncryptedValue(t, "script-removal-intent")
	defer clear(ciphertext)
	coordinator, err := requestidempotency.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	replays, err := etcd.NewIdempotencyRepository(f.base.store)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := scriptcontroller.NewDeletionIdempotency(coordinator, replays)
	if err != nil {
		t.Fatal(err)
	}
	deletions, err := scriptcontroller.NewDeletionService(mutations, idempotency)
	if err != nil {
		t.Fatal(err)
	}
	const key = "native-script-removal-0001"
	bodyKey := scripts.ScriptSetBodyGenerationKey(environment.Record.ID, current.Record.ScriptSetGeneration,
		record.Desired.ID, current.Record.ActiveGeneration)
	storedBody, err := f.base.store.Get(ctx, bodyKey)
	if err != nil || storedBody.Entry == nil {
		t.Fatalf("Script body capture: %v", err)
	}
	defer clear(storedBody.Entry.Value)
	body, err := scripts.DecodeScriptBodyGeneration(storedBody.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := scriptsourcepublication.NewAuthority(f.base.store)
	if err != nil {
		t.Fatal(err)
	}
	operationID := ids.New(ids.KindOperation)
	members := []scriptsourceevidence.ScriptSourcePreparationMember{{
		Reference: scriptsourcereference.Reference{OperationID: operationID, ScriptExecutionID: ids.NewULID(),
			Source: scriptsourcereference.SourceIdentity{Kind: scriptsourcereference.SourceBody,
				EnvironmentID: environment.Record.ID, ScriptSetGeneration: current.Record.ScriptSetGeneration,
				ScriptID: record.Desired.ID, BodyGeneration: current.Record.ActiveGeneration},
			SourceOwnerID: environment.Record.ID, SourceModRevision: storedBody.Entry.ModRevision, SourceDigest: body.BodySHA256},
		Evidence: scriptsourceevidence.ScriptSourceEvidence{
			Existing: &scriptsourceevidence.ScriptExistingSourceEvidence{SourceKey: bodyKey}},
	}}
	if _, err := authority.Prepare(ctx, operationID, members); err != nil {
		t.Fatal(err)
	}
	protectedRevision := f.base.store.revision
	if _, err := deletions.RemoveScript(ctx, record.Desired.ID, key); !errors.Is(
		err,
		errs.New(errs.KindResourceInUse, ""),
	) ||
		f.base.store.revision != protectedRevision {
		t.Fatalf("prepared body source did not prevent removal without writes: %v", err)
	}
	if err := authority.Abandon(ctx, operationID, members); err != nil {
		t.Fatal(err)
	}
	response, err := deletions.RemoveScript(ctx, current.Record.Desired.ID, key)
	if err != nil {
		t.Fatalf("native Script removal acceptance: %v", err)
	}
	var accepted api.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil || response.Status != http.StatusAccepted ||
		accepted.TaskID == "" {
		t.Fatalf("Script removal response: %#v, %v", response, err)
	}
	page, err := f.base.queue.ListWork(ctx, accepted.TaskID, "", 10, 0)
	if err != nil || len(page.Items) != 1 || page.Items[0].Item.Operation != workexecution.ScriptRemoval ||
		page.Items[0].Item.Placement.Executor != workexecution.Controller || page.Items[0].Item.Phase != workexecution.Ready {
		t.Fatalf("Script removal did not publish ready native Controller work: %#v, %v", page, err)
	}
	if _, err := repository.GetScript(ctx, record.Desired.ID); err != nil {
		t.Fatalf("Script disappeared before its removal decision: %v", err)
	}
	view, err := f.base.queue.ReadWork(ctx, accepted.TaskID, page.Items[0].Item.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := scriptremoval.NewRepository(f.base.store)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := owner.PrepareDecision(ctx, view, workexecution.Succeeded, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	defer keyvalue.ClearMutationValues(decision.Mutations)
	if result, err := f.base.queue.Commit(ctx, decision); err != nil || !result.Succeeded {
		t.Fatalf("Script removal settlement: committed=%t, %v", result.Succeeded, err)
	}
	if _, err := repository.GetScript(ctx, record.Desired.ID); !errors.Is(err, errs.New(errs.KindScriptNotFound, "")) {
		t.Fatalf("Script survived successful removal: %v", err)
	}
	if body, err := f.base.store.Get(ctx, bodyKey); err != nil || body.Entry != nil {
		t.Fatalf("Script removal retained its unreferenced body bytes: %v", err)
	}
	beforeFinish := f.base.store.revision
	if _, err := deletions.RemoveScript(ctx, record.Desired.ID, key); !errors.Is(
		err,
		errs.New(errs.KindIdempotencyInProgress, ""),
	) ||
		f.base.store.revision != beforeFinish {
		t.Fatalf("unfinished visible Task did not preserve pending replay without writes: %v", err)
	}
	workOwner, err := scriptcontroller.NewRemovalWork(owner, f.base.queue)
	if err != nil {
		t.Fatal(err)
	}
	completions, err := workdispatch.NewCompletions(f.base.queue, []workdispatch.CompletionRegistration{{
		Action: workexecution.ActionRemove, Operation: workexecution.ScriptRemoval, Owner: workOwner,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := completions.Finish(ctx, accepted.TaskID, time.Now().UTC(), time.Hour); err != nil {
		t.Fatalf("finish visible Script removal Task: %v", err)
	}
	before := f.base.store.revision
	replay, err := deletions.RemoveScript(ctx, record.Desired.ID, key)
	if err != nil || replay.Status != response.Status || string(replay.Body) != string(response.Body) ||
		f.base.store.revision != before {
		t.Fatalf("Script removal replay changed identity or wrote after deletion: %v", err)
	}
	settled, err := f.base.queue.ReadWork(ctx, accepted.TaskID, page.Items[0].Item.ID, 0)
	if err != nil || settled.Item.Item.Phase != workexecution.Settled ||
		settled.Item.Item.Outcome != workexecution.Succeeded ||
		settled.Item.Item.AttemptNumber != 0 ||
		settled.Task.Task.Progress.Succeeded != 1 || settled.Task.Task.FinishedAt == nil ||
		settled.Task.Task.Outcome != workexecution.Succeeded {
		t.Fatalf("Script deletion and work progress were not settled together: %v", err)
	}
}
