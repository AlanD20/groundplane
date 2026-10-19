package app

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/imagedelivery"
	taskoperations "github.com/AlanD20/groundplane/internal/controller/tasks"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/imageoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RUN-13: actual native publication, scheduling, journal, settlement and public
// Retry, with a simulated registry. No Docker/process-ownership claim is made.
func TestImageFetchPinsContentAcrossFailureRetryAndRequestReplay(t *testing.T) {
	f, ctx := newNativeImageFixture(t), t.Context()
	original := f.registry.selected
	response, accepted := f.accept(t, "image-fetch-pinned-content")
	f.registry.selected = imageTestPlan("d", "e")
	if _, err := f.service.FetchImage(ctx, original.Requested, "image-fetch-pinned-content"); err == nil ||
		f.registry.resolutions != 1 {
		t.Fatalf("pending replay resolved a moved tag: resolves=%d, error=%v", f.registry.resolutions, err)
	}
	f.registry.returnedID = "sha256:" + strings.Repeat("c", 64)
	failed, err := f.execute(t, accepted.TaskID)
	if err == nil || len(f.registry.tagged) != 0 {
		t.Fatal("mismatched image was accepted or tagged")
	}
	replay, err := f.service.FetchImage(ctx, original.Requested, "image-fetch-pinned-content")
	if err != nil || !bytes.Equal(replay.Body, response.Body) || f.registry.resolutions != 1 {
		t.Fatalf("terminal replay selected new content: %v", err)
	}
	request := api.TaskRetryRequest{WorkItemID: failed.Item.Item.ID, Attempt: 1}
	retry, err := f.retry.RetryTask(ctx, accepted.TaskID, "image-fetch-retry", request)
	if err != nil {
		t.Fatal(err)
	}
	var retried api.TaskAccepted
	if err := json.Unmarshal(retry.Body, &retried); err != nil || retried.TaskID != accepted.TaskID {
		t.Fatalf("Retry replaced its Task: %#v %v", retried, err)
	}
	repeated, err := f.retry.RetryTask(ctx, accepted.TaskID, "image-fetch-retry", request)
	if err != nil || !bytes.Equal(retry.Body, repeated.Body) {
		t.Fatalf("Retry request replay: %v", err)
	}
	f.registry.returnedID = original.ConfigDigest
	succeeded, err := f.execute(t, accepted.TaskID)
	if err != nil || succeeded.Attempt.Attempt.Number != 2 || succeeded.Item.Item.ID != failed.Item.Item.ID ||
		succeeded.Attempt.Attempt.Input != failed.Attempt.Attempt.Input || f.registry.resolutions != 1 ||
		len(f.registry.fetched) != 2 || f.registry.fetched[0] != original || f.registry.fetched[1] != original ||
		len(f.registry.tagged) != 1 || f.registry.tagged[0] != original {
		t.Fatalf("Retry changed captured identity/content: %#v %v", f.registry, err)
	}
	// A lost settlement ACK permits reconciliation, not another Docker call.
	if err := f.service.Reconcile(ctx, succeeded); err != nil {
		t.Fatal(err)
	}
	if err := f.service.Settle(ctx, f.queue, succeeded, time.Now().UTC()); err != nil || len(f.registry.tagged) != 1 {
		t.Fatalf("settlement replay repeated tag write: %v", err)
	}
	current, previous, err := f.queue.ReadAttempt(ctx, accepted.TaskID, failed.Item.Item.ID, 1, 0)
	if err != nil || previous.Attempt.Outcome != workexecution.Failed ||
		current.Item.Item.Outcome != workexecution.Succeeded {
		t.Fatalf("Retry replaced prior failure evidence: %#v %v", previous, err)
	}
	if _, err := f.retry.RetryTask(ctx, accepted.TaskID, "image-fetch-completed-retry", api.TaskRetryRequest{
		WorkItemID: succeeded.Item.Item.ID, Attempt: 2,
	}); err == nil {
		t.Fatal("successful Fetch accepted Retry")
	}

	// Newer selections may move a tag; an older captured Retry cannot undo it.
	f.registry.selected, f.registry.returnedID = imageTestPlan("1", "2"), "wrong-image"
	_, stale := f.accept(t, "image-fetch-stale-selection")
	staleDelivery, err := f.execute(t, stale.TaskID)
	if err == nil {
		t.Fatal("stale mismatched content accepted")
	}
	f.registry.selected = imageTestPlan("4", "5")
	_, latest := f.accept(t, "image-fetch-latest-selection")
	if _, err := f.retry.RetryTask(ctx, stale.TaskID, "image-fetch-stale-retry", api.TaskRetryRequest{
		WorkItemID: staleDelivery.Item.Item.ID, Attempt: 1,
	}); err != nil {
		t.Fatal(err)
	}
	f.registry.returnedID = f.registry.selected.ConfigDigest
	if _, err := f.execute(t, latest.TaskID); err != nil {
		t.Fatal(err)
	}
	tagged := len(f.registry.tagged)
	f.registry.returnedID = stale.ConfigDigest
	if _, err := f.execute(t, stale.TaskID); err == nil || len(f.registry.tagged) != tagged {
		t.Fatalf("stale Retry moved current tag: %v", err)
	}
	f.registry.selected = imageTestPlan("6", "7")
	f.registry.selected.Requested = "nginx@" + f.registry.selected.ManifestDigest
	f.registry.returnedID = f.registry.selected.ConfigDigest
	_, digest := f.accept(t, "image-fetch-digest-only")
	if _, err := f.execute(t, digest.TaskID); err != nil || len(f.registry.tagged) != tagged {
		t.Fatalf("digest-only Fetch assigned a tag: %v", err)
	}
}

// Unknown writes retain their resource claim, not unrelated worker capacity.
func TestNativeImageUnknownTagDoesNotGrantRetryOrBlockUnrelatedWork(t *testing.T) {
	f := newNativeImageFixture(t)
	f.registry.tagOutcome = imagefetch.TagAssignmentUnknown
	_, accepted := f.accept(t, "image-fetch-unknown-write")
	delivery, _ := f.execute(t, accepted.TaskID)
	view, _, err := f.queue.ReadAttempt(t.Context(), accepted.TaskID, delivery.Item.Item.ID, 1, 0)
	if err != nil || view.Item.Item.Phase != workexecution.Attention {
		t.Fatalf("unknown tag lost unresolved effect: %#v %v", view.Item.Item, err)
	}
	if _, err := f.retry.RetryTask(t.Context(), accepted.TaskID, "image-fetch-unknown-retry", api.TaskRetryRequest{
		WorkItemID: delivery.Item.Item.ID, Attempt: 1,
	}); err == nil {
		t.Fatal("unknown write authorized another execution")
	}
	if _, err := f.service.FetchImage(t.Context(), f.registry.selected.Requested, "image-fetch-conflicting-selection"); err == nil {
		t.Fatal("unresolved tag write allowed conflicting selection")
	}
	f.registry.selected = imageTestPlan("8", "9")
	f.registry.selected.Requested = "nginx@" + f.registry.selected.ManifestDigest
	f.registry.returnedID = f.registry.selected.ConfigDigest
	_, unrelated := f.accept(t, "image-fetch-unrelated-digest")
	if _, err := f.execute(t, unrelated.TaskID); err != nil {
		t.Fatalf("unrelated work blocked: %v", err)
	}
	ops, err := imageoperations.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	unknown, err := ops.Load(t.Context(), accepted.TaskID)
	if err != nil || unknown.Record.Phase != imageoperations.Uncertain {
		t.Fatalf("unknown tag released: %v", err)
	}
}

type nativeImageFixture struct {
	store    *memoryHierarchyStore
	queue    *workqueue.Repository
	service  *imagedelivery.Service
	registry *imageFetchRegistry
	identity workexecution.Identity
	retry    interface {
		RetryTask(context.Context, string, string, api.TaskRetryRequest) (idempotency.IdempotencyResponse, error)
	}
}

func newNativeImageFixture(t *testing.T) *nativeImageFixture {
	t.Helper()
	f := &nativeImageFixture{store: newMemoryHierarchyStore()}
	f.store.revision = 1
	var err error
	f.queue, err = workqueue.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := etcd.NewTaskRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	receipts, err := etcd.NewIdempotencyRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	protector, encrypted := manualJourneyEncryptedValue(t, "image-fetch-intent")
	clear(encrypted)
	coordinator, err := requestidempotency.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	diagnostics, err := executiondiagnostics.New(f.store, f.queue)
	if err != nil {
		t.Fatal(err)
	}
	releases, err := etcd.NewReleaseLedger(f.store, tasks)
	if err != nil {
		t.Fatal(err)
	}
	f.registry = &imageFetchRegistry{selected: imageTestPlan("a", "b"), tagOutcome: imagefetch.TagAssignmentApplied}
	f.registry.returnedID = f.registry.selected.ConfigDigest
	f.service, err = imagedelivery.New(f.registry, f.store, tasks, receipts, coordinator, diagnostics, releases)
	if err != nil {
		t.Fatal(err)
	}
	retries, err := workdispatch.NewRetries(
		f.queue,
		[]workdispatch.RetryRegistration{{Operation: workexecution.Image, Owner: f.service}},
	)
	if err != nil {
		t.Fatal(err)
	}
	f.retry, err = taskoperations.NewRetryService(f.queue, retries, receipts, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	registration, record, err := workqueue.PrepareRegistration(workqueue.ExecutorSnapshot{}, workqueue.ExecutorRecord{
		Identity: workexecution.Identity{
			Executor: workexecution.Controller,
		}, Capacity: 2, Operations: []workexecution.Operation{workexecution.Image},
	})
	if err != nil {
		t.Fatal(err)
	}
	f.commit(t, registration)
	f.identity = record.Identity
	executor, err := f.queue.ReadExecutor(t.Context(), workexecution.Placement{Executor: workexecution.Controller})
	if err != nil {
		t.Fatal(err)
	}
	available, err := workqueue.PrepareAvailability(executor, f.identity, true, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	f.commit(t, available)
	return f
}

func imageTestPlan(manifest, config string) imagefetch.Plan {
	return imagefetch.Plan{Requested: "nginx:latest", Repository: "docker.io/library/nginx",
		ManifestDigest: "sha256:" + strings.Repeat(
			manifest,
			64,
		), ConfigDigest: "sha256:" + strings.Repeat(config, 64), Architecture: "amd64"}
}

func (f *nativeImageFixture) commit(t *testing.T, changes ...workqueue.Change) {
	t.Helper()
	result, err := f.queue.Commit(t.Context(), changes...)
	if err != nil || !result.Succeeded {
		t.Fatalf("native image commit: %t %v", result.Succeeded, err)
	}
}

func (f *nativeImageFixture) accept(
	t *testing.T,
	key string,
) (idempotency.IdempotencyResponse, api.ImageFetchAccepted) {
	t.Helper()
	response, err := f.service.FetchImage(t.Context(), f.registry.selected.Requested, key)
	if err != nil {
		t.Fatal(err)
	}
	var accepted api.ImageFetchAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.Image != f.registry.selected.Reference() || accepted.ConfigDigest != f.registry.selected.ConfigDigest {
		t.Fatal("accepted content changed")
	}
	return response, accepted
}

func (f *nativeImageFixture) execute(t *testing.T, taskID string) (workqueue.Delivery, error) {
	t.Helper()
	ctx := t.Context()
	page, err := f.queue.ListWork(ctx, taskID, "", 10, 0)
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("image membership: %v", err)
	}
	item := page.Items[0]
	queued, err := f.store.Get(ctx, workqueue.QueueKey(item.Item))
	if err != nil || queued.Entry == nil {
		t.Fatalf("image not queued: %v", err)
	}
	at := time.Now().UTC()
	candidate, err := f.queue.ReadCandidate(ctx, workqueue.QueueEntry{TaskID: taskID, WorkItemID: item.Item.ID,
		Revision: queued.Entry.ModRevision}, f.identity, at, nil)
	if err != nil || candidate.Wait != nil || candidate.Stopped {
		t.Fatalf("image candidate: %#v %v", candidate.Wait, err)
	}
	decision, err := f.service.Admit(ctx, candidate.Assignment, at)
	if err != nil || decision.Disposition != workdispatch.Dispatch {
		t.Fatalf("image admission: %#v %v", decision, err)
	}
	request := candidate.Assignment
	request.At, request.Deadline, request.AttemptID = at, decision.Deadline, ids.New(ids.KindAttempt)
	assigned, attempt, err := workqueue.PrepareAssignment(request)
	if err != nil {
		t.Fatal(err)
	}
	f.commit(t, assigned, decision.Change)
	resume, err := f.queue.ReadResume(ctx, taskID, item.Item.ID, attempt.ID, f.identity)
	if err != nil {
		t.Fatal(err)
	}
	delivery := workqueue.Delivery{Item: resume.Work.Item, Attempt: resume.Attempt}
	permission, err := f.service.AuthorizeStart(ctx, delivery)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := f.queue.ReadExecutor(ctx, item.Item.Placement)
	if err != nil {
		t.Fatal(err)
	}
	start, err := workqueue.PrepareStart(resume.Work.Task, resume.Work.Item, resume.Attempt, executor, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	f.commit(t, start, permission)
	resume, err = f.queue.ReadResume(ctx, taskID, item.Item.ID, attempt.ID, f.identity)
	if err != nil {
		t.Fatal(err)
	}
	delivery = workqueue.Delivery{Item: resume.Work.Item, Attempt: resume.Attempt}
	executionErr := f.service.Execute(ctx, delivery)
	if err := f.service.Settle(ctx, f.queue, delivery, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	settled, err := f.queue.ReadWork(ctx, taskID, item.Item.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if settled.Item.Item.Phase == workexecution.Settled {
		completion, err := workdispatch.NewCompletions(f.queue, []workdispatch.CompletionRegistration{
			{
				Action:    settled.Task.Task.Action,
				Operation: workexecution.Image,
				Owner:     workdispatch.CheckImageCompletion{},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := completion.Finish(ctx, taskID, time.Now().UTC(), time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	return delivery, executionErr
}

type imageFetchRegistry struct {
	selected    imagefetch.Plan
	returnedID  string
	resolutions int
	fetched     []imagefetch.Plan
	tagged      []imagefetch.Plan
	tagOutcome  imagefetch.TagAssignmentOutcome
	tagErr      error
	images      []imagefetch.LocalImage
}

func (registry *imageFetchRegistry) List(context.Context) ([]imagefetch.LocalImage, error) {
	return registry.images, nil
}
func (*imageFetchRegistry) Remove(context.Context, string) error {
	return errs.New(errs.KindInternal, "unexpected image removal")
}
func (registry *imageFetchRegistry) Resolve(context.Context, string) (imagefetch.Plan, error) {
	registry.resolutions++
	return registry.selected, nil
}

func (registry *imageFetchRegistry) Fetch(
	_ context.Context,
	plan imagefetch.Plan,
	report imagefetch.Reporter,
) (string, error) {
	if err := report(imagefetch.Progress{Phase: "verifying"}); err != nil {
		return "", err
	}
	registry.fetched = append(registry.fetched, plan)
	return registry.returnedID, nil
}

func (registry *imageFetchRegistry) AssignTag(
	_ context.Context,
	plan imagefetch.Plan,
) (imagefetch.TagAssignmentOutcome, error) {
	registry.tagged = append(registry.tagged, plan)
	return registry.tagOutcome, registry.tagErr
}
