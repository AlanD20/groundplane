//go:build etcd_acceptance

package app

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	deletion "github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/controller/tasks"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	api "github.com/AlanD20/groundplane/pkg/api"
)

type nativeDeletionClock struct{ at time.Time }

func (clock nativeDeletionClock) Now() time.Time { return clock.at }

// TASK-166: expiry is a failed, unexecuted action, not proof of deletion. Public
// Retry after client restart must renew the same captured work and reopen its
// terminal Task atomically, preserving failure evidence and request replay.
func TestNativeHierarchyRealEtcdFailedWorkRetryKeepsIdentity(t *testing.T) {
	ctx, _, open := nativeEtcdAcceptanceConnector(t)
	store := open()
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	parents, err := etcd.NewHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	tenant := hierarchy.TenantRecord{ID: ids.New(ids.KindTenant), Slug: "retry-removal", Name: "Retry removal"}
	if _, err := parents.CreateTenant(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	project := hierarchy.ProjectRecord{ID: ids.New(ids.KindProject), TenantID: tenant.ID,
		Slug: "captured", Name: "Captured", Kind: hierarchy.ProjectKindTenant}
	if _, err := parents.CreateProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	key, err := ageinfra.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	// Only admission/publication use an old clock. Ordinary scheduling, expiry,
	// public Retry and completion run at current time without rewriting records.
	old := time.Now().UTC().Add(-deletion.OperationDeadline - time.Minute)
	owner := newNativeHierarchyEtcdRuntime(t, store, key, nativeDeletionClock{old})
	accepted, err := owner.service.Delete(ctx, deletion.DeleteRequest{
		TargetKind: deletion.TargetProject, TargetID: project.ID, IdempotencyKey: "native-etcd-expired-removal"})
	if err != nil {
		t.Fatal(err)
	}
	owner.publish(t, ctx, accepted.TaskID, old)
	page, err := owner.queue.ListWork(ctx, accepted.TaskID, "", 10, 0)
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("captured removal work: %v", err)
	}
	captured := page.Items[0].Item
	sweep, err := owner.scheduler.Schedule(ctx, owner.identity, "", time.Now().UTC())
	if err != nil || len(sweep.Issues) != 0 || len(sweep.Settled) != 1 {
		t.Fatalf("ordinary queued expiry: %+v, %v", sweep, err)
	}
	owner.finalize(t, ctx, accepted.TaskID)
	failed, err := owner.queue.ReadWork(ctx, accepted.TaskID, captured.ID, 0)
	if err != nil || failed.Task.Task.Outcome != workexecution.Failed || failed.Task.Task.FinishedAt == nil ||
		failed.Item.Item.Outcome != workexecution.Failed || failed.Item.Item.AttemptNumber != 0 ||
		failed.Item.Item.FirstFailure == nil {
		t.Fatalf("expiry did not retain a failed unexecuted Task: %v", err)
	}
	failure := *failed.Item.Item.FirstFailure
	if _, err := parents.GetProject(ctx, project.ID); err != nil {
		t.Fatalf("expiry removed metadata: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = open()
	owner = newNativeHierarchyEtcdRuntime(t, store, key, deletion.SystemClock{})
	retry, err := tasks.NewRetryService(owner.queue, owner.retries, owner.evidence, owner.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	request := api.TaskRetryRequest{WorkItemID: captured.ID, QueuedFailureAt: &failed.Item.Item.UpdatedAt}
	response, err := retry.RetryTask(ctx, accepted.TaskID, "native-etcd-retry-removal-request", request)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(response.Body)
	var retried api.TaskAccepted
	if err := json.Unmarshal(response.Body, &retried); err != nil || retried.TaskID != accepted.TaskID {
		t.Fatalf("Retry replaced the visible Task: %v", err)
	}
	ready, err := owner.queue.ReadWork(ctx, accepted.TaskID, captured.ID, 0)
	if err != nil || ready.Task.Task.FinishedAt != nil || ready.Item.Item.Phase != workexecution.Ready ||
		ready.Item.Item.Input == nil || captured.Input == nil || *ready.Item.Item.Input != *captured.Input ||
		ready.Item.Item.AttemptNumber != 0 || ready.Item.Item.TargetID != project.ID ||
		ready.Item.Item.FirstFailure == nil || *ready.Item.Item.FirstFailure != failure {
		t.Fatalf("Retry lost captured work or original failure: %v", err)
	}
	before, err := store.Get(ctx, hierarchy.TenantKey(tenant.ID))
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := retry.RetryTask(ctx, accepted.TaskID, "native-etcd-retry-removal-request", request)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(replayed.Body)
	after, err := store.Get(ctx, hierarchy.TenantKey(tenant.ID))
	if err != nil || !bytes.Equal(response.Body, replayed.Body) || response.Status != replayed.Status ||
		after.ReadRevision != before.ReadRevision {
		t.Fatalf("Retry replay changed response or durable state: %v", err)
	}
	owner.finish(t, ctx, accepted.TaskID)
	complete, err := owner.queue.ReadWork(ctx, accepted.TaskID, captured.ID, 0)
	if err != nil || complete.Task.Task.FinishedAt == nil || complete.Task.Task.Outcome != workexecution.Succeeded ||
		complete.Item.Item.Outcome != workexecution.Succeeded || complete.Item.Item.FirstFailure == nil ||
		*complete.Item.Item.FirstFailure != failure {
		t.Fatalf("retried removal lost original failure or failed to finish: %v", err)
	}
	parents, err = etcd.NewHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if removed, err := store.Get(ctx, hierarchy.ProjectKey(project.ID)); err != nil || removed.Entry != nil {
		t.Fatalf("retried removal retained the Project: %v", err)
	}
	if _, err := parents.GetTenant(ctx, tenant.ID); err != nil {
		t.Fatalf("Project Retry removed its Tenant: %v", err)
	}
}
