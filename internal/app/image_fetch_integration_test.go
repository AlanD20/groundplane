package app

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/imagedelivery"
	taskoperations "github.com/AlanD20/groundplane/internal/controller/tasks"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RUN-13: real publication, execution and Retry must preserve the first selected
// content when a tag moves, and reject a host image different from that selection.
func TestImageFetchPinsContentAcrossFailureRetryAndRequestReplay(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	store.revision = 1 // An empty real etcd still has a positive read revision.
	tasks, err := etcd.NewTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := etcd.NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	protector, encrypted := manualJourneyEncryptedValue(t, "image-fetch-intent")
	clear(encrypted)
	coordinator, err := requestidempotency.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	original := imagefetch.Plan{
		Requested: "nginx:latest", Repository: "docker.io/library/nginx",
		ManifestDigest: "sha256:" + strings.Repeat("a", 64), ConfigDigest: "sha256:" + strings.Repeat("b", 64),
		Architecture: "amd64",
	}
	registry := &imageFetchRegistry{selected: original, returnedID: "sha256:" + strings.Repeat("c", 64)}
	service, err := imagedelivery.New(registry, store, tasks, idempotency, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	const key = "image-fetch-pinned-content"
	response, err := service.FetchImage(ctx, original.Requested, key)
	if err != nil {
		t.Fatal(err)
	}
	var accepted api.ImageFetchAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.Image != original.Reference() || accepted.ConfigDigest != original.ConfigDigest ||
		len(registry.fetched) != 0 {
		t.Fatalf("acceptance changed identity or fetched before execution: %#v", accepted)
	}
	registry.selected.ManifestDigest = "sha256:" + strings.Repeat("d", 64)
	registry.selected.ConfigDigest = "sha256:" + strings.Repeat("e", 64)
	if _, err := service.FetchImage(ctx, original.Requested, key); err == nil || registry.resolutions != 1 {
		t.Fatalf("pending replay resolved a moved tag: resolves=%d, error=%v", registry.resolutions, err)
	}
	claim, found, err := tasks.ClaimNextControllerTask(ctx, time.Now().UTC())
	if err != nil || !found || claim.Task.Record.ID != accepted.TaskID {
		t.Fatalf("claim accepted fetch: found=%t, error=%v", found, err)
	}
	if err := service.Execute(ctx, claim.Task.Record); err == nil {
		t.Fatal("wrong host image was accepted as successful delivery")
	}
	failedProgress, err := tasks.GetTask(ctx, accepted.TaskID)
	if err != nil || failedProgress.Record.ImageFetchProgress.ErrorCode != "state.conflict" || failedProgress.Record.ImageFetchProgress.Phase != "verifying" {
		t.Fatalf("fetch failure diagnostic was not persisted: %#v, %v", failedProgress.Record.ImageFetchProgress, err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, accepted.TaskID, taskjournal.TaskStatusFailed, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	replay, err := service.FetchImage(ctx, original.Requested, key)
	if err != nil || !bytes.Equal(replay.Body, response.Body) || registry.resolutions != 1 {
		t.Fatalf("terminal replay selected new bytes: resolves=%d, error=%v", registry.resolutions, err)
	}
	retryIntents, err := taskoperations.NewRetryIdempotency(coordinator, idempotency)
	if err != nil {
		t.Fatal(err)
	}
	retries, err := taskoperations.NewRetryService(tasks, retryIntents, imageFetchUnexpectedBackup{})
	if err != nil {
		t.Fatal(err)
	}
	retryResponse, err := retries.RetryTask(ctx, accepted.TaskID, "image-fetch-retry")
	if err != nil {
		t.Fatal(err)
	}
	var retry api.TaskAccepted
	if err := json.Unmarshal(retryResponse.Body, &retry); err != nil {
		t.Fatal(err)
	}
	claim, found, err = tasks.ClaimNextControllerTask(ctx, time.Now().UTC())
	if err != nil || !found || claim.Task.Record.ID != retry.TaskID {
		t.Fatalf("claim retry: found=%t, error=%v", found, err)
	}
	registry.returnedID = original.ConfigDigest
	if claim.Task.Record.ImageFetchProgress.Phase != "" {
		t.Fatal("retry inherited failed attempt progress")
	}
	if err := service.Execute(ctx, claim.Task.Record); err != nil {
		t.Fatal(err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, retry.TaskID, taskjournal.TaskStatusCompleted, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	verified, err := tasks.GetTask(ctx, retry.TaskID)
	if err != nil || verified.Record.ImageFetchProgress.Phase != "verified" || verified.Record.ImageFetchProgress.ErrorCode != "" {
		t.Fatalf("retry progress did not record verified content: %#v, %v", verified.Record.ImageFetchProgress, err)
	}
	if err := service.Execute(ctx, claim.Task.Record); err == nil {
		t.Fatal("late progress overwrote a completed Task")
	}
	if registry.resolutions != 1 || len(registry.fetched) != 2 || registry.fetched[0] != original ||
		registry.fetched[1] != original {
		t.Fatalf("retry changed selected content: %#v", registry)
	}
	if _, found, err := tasks.ClaimNextControllerTask(ctx, time.Now().UTC()); err != nil || found {
		t.Fatalf("replay published duplicate work: found=%t, error=%v", found, err)
	}
	newResponse, err := service.FetchImage(ctx, original.Requested, "image-fetch-new-selection")
	if err != nil {
		t.Fatal(err)
	}
	var newSelection api.ImageFetchAccepted
	if err := json.Unmarshal(newResponse.Body, &newSelection); err != nil {
		t.Fatal(err)
	}
	if newSelection.Image != registry.selected.Reference() || newSelection.TaskID == accepted.TaskID ||
		registry.resolutions != 2 {
		t.Fatal("a fresh request did not select the new tag content")
	}
	// IMG-01: inventory must join attempts and their status to immutable manifests,
	// including Docker's familiar repository spelling, not today's mutable tag.
	registry.images = []imagefetch.LocalImage{
		{ID: original.ConfigDigest, Digests: []string{"nginx@" + original.ManifestDigest}},
		{ID: registry.selected.ConfigDigest, Digests: []string{registry.selected.Reference()}},
		{ID: original.ConfigDigest, Digests: []string{"nginx@sha256:" + strings.Repeat("f", 64)}},
	}
	inventory, err := service.ListImages(ctx)
	if err != nil || len(inventory.Images) != 3 {
		t.Fatalf("inventory: %#v %v", inventory, err)
	}
	if history := inventory.Images[0].Fetches; len(history) != 2 || history[0].TaskID != retry.TaskID ||
		history[0].Requested != "nginx:latest" || history[0].Image != original.Reference() ||
		history[0].Status != "completed" || history[1].Status != "failed" || history[1].TaskID != accepted.TaskID {
		t.Fatalf("original fetch history lost or reassigned: %#v", history)
	}
	if len(inventory.Images[1].Fetches) != 1 || inventory.Images[1].Fetches[0].Status != "pending" ||
		len(inventory.Images[2].Fetches) != 0 {
		t.Fatal("pending attempt lost its status or another manifest gained the wrong history")
	}
	claim, found, err = tasks.ClaimNextControllerTask(ctx, time.Now().UTC())
	if err != nil || !found || claim.Task.Record.ID != newSelection.TaskID {
		t.Fatalf("claim new selection: %t %v", found, err)
	}
	registry.returnedID = registry.selected.ConfigDigest
	if err := service.Execute(ctx, claim.Task.Record); err != nil {
		t.Fatal(err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, newSelection.TaskID, taskjournal.TaskStatusCompleted, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	inventory, err = service.ListImages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(inventory.Images[0].Fetches) != 2 || inventory.Images[0].Fetches[0].Image != original.Reference() ||
		len(inventory.Images[1].Fetches) != 1 || inventory.Images[1].Fetches[0].Image != newSelection.Image ||
		inventory.Images[1].Fetches[0].Requested != "nginx:latest" || inventory.Images[1].Fetches[0].Status != "completed" {
		t.Fatalf("moved tag history: %#v", inventory.Images)
	}
}

type imageFetchRegistry struct {
	selected    imagefetch.Plan
	returnedID  string
	resolutions int
	fetched     []imagefetch.Plan
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

func (registry *imageFetchRegistry) Fetch(_ context.Context, plan imagefetch.Plan, report imagefetch.Reporter) (string, error) {
	if err := report(imagefetch.Progress{Phase: "verifying"}); err != nil {
		return "", err
	}
	registry.fetched = append(registry.fetched, plan)
	return registry.returnedID, nil
}

type imageFetchUnexpectedBackup struct{}

func (imageFetchUnexpectedBackup) RetryBackupTask(
	context.Context,
	string,
	string,
	idempotencyrecord.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return etcd.IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "image fetch dispatched as backup")
}
