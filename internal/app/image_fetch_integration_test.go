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
		Requested: imagefetch.RegistryAuthority + "/app:release", Repository: imagefetch.RegistryAuthority + "/app",
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
	if err := service.Execute(ctx, claim.Task.Record); err != nil {
		t.Fatal(err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, retry.TaskID, taskjournal.TaskStatusCompleted, time.Now().UTC()); err != nil {
		t.Fatal(err)
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
}

type imageFetchRegistry struct {
	selected    imagefetch.Plan
	returnedID  string
	resolutions int
	fetched     []imagefetch.Plan
}

func (*imageFetchRegistry) List(context.Context) ([]imagefetch.LocalImage, error) { return nil, nil }
func (*imageFetchRegistry) Remove(context.Context, string) error {
	return errs.New(errs.KindInternal, "unexpected image removal")
}

func (registry *imageFetchRegistry) Resolve(context.Context, string) (imagefetch.Plan, error) {
	registry.resolutions++
	return registry.selected, nil
}

func (registry *imageFetchRegistry) Fetch(_ context.Context, plan imagefetch.Plan) (string, error) {
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
