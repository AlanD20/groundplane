package app

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	attachmentcontroller "github.com/AlanD20/groundplane/internal/controller/attachments"
	releasedomain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TASK-162: extend the actual Custom Attach journey through queued Detach expiry,
// rejection of inconsistent parent accounting, same-work Retry and removal.
// This catches the discarded shared Retry error before it can return a binding-
// only change. No host effects, Agent attempt or physical cleanup are simulated.
func nativeUnstartedDetachJourney(t *testing.T, f *nativeVolumeFixture,
	mutations *attachmentcontroller.MutationService, records *etcd.AttachRepository, attachID string,
) {
	t.Helper()
	ctx := t.Context()
	bindingBefore, err := records.GetAttach(ctx, attachID)
	if err != nil {
		t.Fatal(err)
	}
	projectionKey := releases.ReleaseProjectionKey(bindingBefore.Record.ServiceID)
	for _, stage := range []string{"serving", "successful", "active"} {
		projection := releasedomain.ServiceProjection{EnvironmentID: f.environmentID,
			ServiceID: bindingBefore.Record.ServiceID, Revision: 1}
		switch stage {
		case "serving":
			projection.ServingReleaseID = ids.New(ids.KindDeployment)
		case "successful":
			projection.CurrentSuccessfulReleaseID = ids.New(ids.KindDeployment)
		case "active":
			projection.ActiveOperationID = ids.New(ids.KindOperation)
		}
		raw, err := releases.EncodeReleaseRecord("service-release-projection", projection)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.Put(ctx, projectionKey, raw); err != nil {
			t.Fatal(err)
		}
		clear(raw)
		before := f.store.revision
		_, err = mutations.DetachAttach(ctx, attachID, "native-detach-refuse-"+stage)
		if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || f.store.revision != before {
			t.Fatalf("Detach ignored %s execution authority without an acknowledged runtime: %v", stage, err)
		}
		if _, err := f.store.Delete(ctx, projectionKey); err != nil {
			t.Fatal(err)
		}
	}
	response, err := mutations.DetachAttach(ctx, attachID, "native-custom-detach")
	if err != nil {
		t.Fatalf("native Detach publication: %v", err)
	}
	var accepted api.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil || accepted.TaskID == "" {
		t.Fatalf("native Detach response: %v", err)
	}
	page, err := f.queue.ListWork(ctx, accepted.TaskID, "", 10, 0)
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("native Detach membership: %v", err)
	}
	workID := page.Items[0].Item.ID
	view, err := f.queue.ReadWork(ctx, accepted.TaskID, workID, 0)
	if err != nil {
		t.Fatal(err)
	}
	item := view.Item.Item
	if view.Task.Task.Action != workexecution.ActionDetach || item.TargetID != attachID ||
		item.Operation != workexecution.Detach || item.Placement.Executor != workexecution.Controller ||
		item.AttemptNumber != 0 || item.Phase != workexecution.Ready || item.Input == nil {
		t.Fatal("plain Detach lost its binding or invented host work")
	}
	inputs, err := attachremoval.NewInputRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	failedAt := item.Deadline.Add(time.Millisecond)
	failure, err := inputs.PrepareDetachDecision(ctx, view, failedAt)
	if err != nil {
		t.Fatalf("queued Detach expiry: %v", err)
	}
	defer keyvalue.ClearMutationValues(failure.Mutations)
	f.commit(t, failure)
	view, err = f.queue.ReadWork(ctx, accepted.TaskID, workID, 0)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := records.GetAttach(ctx, attachID)
	if err != nil || binding.Record.Status != "failed" || binding.Record.TaskID != accepted.TaskID ||
		view.Item.Item.Outcome != workexecution.Failed || view.Item.Item.AttemptNumber != 0 {
		t.Fatalf("expiry discarded the binding or invented execution: %v", err)
	}

	// A structurally valid parent can still have a contradictory work counter.
	// Shared Retry must reject it, never return only the binding mutation.
	inconsistent := view
	inconsistent.Task.Task = workexecution.CloneTask(view.Task.Task)
	inconsistent.Task.Task.Progress.Failed = 0
	inconsistent.Task.Task.Progress.Succeeded = 1
	if err := inconsistent.Task.Task.Validate(); err != nil {
		t.Fatalf("counter fault must reach cross-record Retry validation: %v", err)
	}
	before := f.store.revision
	retryAt := failedAt.Add(time.Millisecond)
	rejected, err := inputs.PrepareDetachRetry(ctx, inconsistent, workqueue.AttemptSnapshot{}, retryAt)
	defer keyvalue.ClearMutationValues(rejected.Mutations)
	if err == nil || len(rejected.Mutations) != 0 || f.store.revision != before {
		t.Fatalf("inconsistent Retry returned a partial binding change: %v", err)
	}
	preserved, err := records.GetAttach(ctx, attachID)
	if err != nil || preserved.Revision != binding.Revision || preserved.Record.Status != "failed" {
		t.Fatalf("rejected Retry changed the failed binding: %v", err)
	}
	retry, err := inputs.PrepareDetachRetry(ctx, view, workqueue.AttemptSnapshot{}, retryAt)
	if err != nil {
		t.Fatalf("valid captured Detach Retry: %v", err)
	}
	defer keyvalue.ClearMutationValues(retry.Mutations)
	f.commit(t, retry)
	view, err = f.queue.ReadWork(ctx, accepted.TaskID, workID, 0)
	if err != nil {
		t.Fatal(err)
	}
	binding, err = records.GetAttach(ctx, attachID)
	if err != nil || binding.Record.Status != "detaching" || binding.Record.TaskID != accepted.TaskID ||
		view.Item.Item.ID != item.ID || view.Item.Item.Input == nil || *view.Item.Item.Input != *item.Input ||
		view.Item.Item.AttemptNumber != 0 || view.Item.Item.Phase != workexecution.Ready {
		t.Fatalf("Retry changed captured identity or failed to reclaim the binding: %v", err)
	}
	completion, err := inputs.PrepareDetachDecision(ctx, view, retryAt.Add(time.Millisecond))
	if err != nil {
		t.Fatalf("Controller-only Detach completion: %v", err)
	}
	defer keyvalue.ClearMutationValues(completion.Mutations)
	f.store.beforeCommit = func(mutations []keyvalue.Mutation) {
		for _, mutation := range mutations {
			if mutation.Key != attachments.AttachKey(attachID) || mutation.Type != keyvalue.MutationDelete {
				continue
			}
			f.store.beforeCommit = nil
			raw, err := releases.EncodeReleaseRecord("service-release-projection", releasedomain.ServiceProjection{
				EnvironmentID: f.environmentID, ServiceID: binding.Record.ServiceID, Revision: 1,
				ActiveOperationID: ids.New(ids.KindOperation),
			})
			if err != nil {
				t.Fatal(err)
			}
			defer clear(raw)
			if _, err := f.store.Put(ctx, projectionKey, raw); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	raced, err := f.queue.Commit(ctx, completion)
	if err != nil || raced.Succeeded || f.store.beforeCommit != nil {
		t.Fatalf("late Deploy escaped metadata-only Detach settlement: %v", err)
	}
	if _, err := records.GetAttach(ctx, attachID); err != nil {
		t.Fatalf("rejected settlement removed the binding: %v", err)
	}
	if _, err := f.store.Delete(ctx, projectionKey); err != nil {
		t.Fatal(err)
	}
	f.commit(t, completion)
	for _, key := range []string{
		attachments.AttachKey(attachID), attachments.AttachFactsKey(attachID),
		attachments.AttachOwnerKey(f.environmentID, attachID),
		attachments.AttachNameKey(f.environmentID, binding.Record.Name),
		attachments.AttachServiceKey(binding.Record.ServiceID, attachID),
		attachments.AttachBackingServiceKey(binding.Record.BackingServiceID, attachID),
	} {
		read, err := f.store.Get(ctx, key)
		if err != nil || read.Entry != nil {
			t.Fatalf("completed Detach retained binding data: %s %v", key, err)
		}
	}
	view, err = f.queue.ReadWork(ctx, accepted.TaskID, workID, 0)
	if err != nil || view.Item.Item.Outcome != workexecution.Succeeded ||
		view.Item.Item.Phase != workexecution.Settled || view.Item.Item.AttemptNumber != 0 {
		t.Fatalf("binding removal and native work did not settle together: %v", err)
	}
}
