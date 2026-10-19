package app

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/imagedelivery"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// IMG-02: unreadable native authority must disable deletion, not inventory.
func TestImageInventoryRemainsReadableWhenProtectionCannotBeVerified(t *testing.T) {
	f := newNativeImageFixture(t)
	_, accepted := f.accept(t, "image-protection-corruption")
	page, err := f.queue.ListWork(t.Context(), accepted.TaskID, "", 10, 0)
	if err != nil || len(page.Items) != 1 {
		t.Fatal(err)
	}
	if _, err := f.store.Put(t.Context(), workqueue.ItemKey(page.Items[0].Item.ID), []byte("unreadable")); err != nil {
		t.Fatal(err)
	}
	f.registry.images = []imagefetch.LocalImage{{ID: accepted.ConfigDigest, Tags: []string{"nginx:latest"}}}
	inventory, err := f.service.ListImages(t.Context())
	if err != nil || len(inventory.Images) != 1 || inventory.Images[0].RemovalBlocked == "" ||
		inventory.Images[0].Tags[0] != "nginx:latest" {
		t.Fatalf("inventory lost or unprotected: %#v %v", inventory, err)
	}
	if _, err := f.service.RemoveImage(t.Context(), accepted.ConfigDigest, "reject-unverified-removal"); err == nil ||
		f.registry.removals != 0 {
		t.Fatalf("unverified removal reached Docker: %v", err)
	}
}

// IMG-02: desired Runner/runtime references and stopped containers stay held;
// completed unrelated work must not create a host-wide image-removal lock.
func TestNativeImageRemovalProtectsRunnerAndContainersWithoutGlobalHistoryLock(t *testing.T) {
	f := newNativeImageFixture(t)
	_, fetched := f.accept(t, "fetch-before-removal")
	if _, err := f.execute(t, fetched.TaskID); err != nil {
		t.Fatal(err)
	}
	imageID := "sha256:" + strings.Repeat("d", 64)
	desired := runners.RunnerDesiredRecord{ID: ids.New(ids.KindRunner), Slug: "image-guard",
		OwnerKind: runners.RunnerOwnerTenant, OwnerID: ids.New(ids.KindTenant),
		GitHubURL: "https://github.com/example", ImageRef: "docker.io/library/runner@" + imageID}
	desired.TenantID = desired.OwnerID
	raw, err := runners.EncodeRunnerDesiredRecord(desired)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Put(t.Context(), runners.RunnerKey(desired.ID), raw); err != nil {
		t.Fatal(err)
	}
	clear(raw)
	f.registry.images = []imagefetch.LocalImage{
		{ID: fetched.ConfigDigest, Digests: []string{fetched.Image}},
		{ID: "sha256:" + strings.Repeat("c", 64), Containers: 1},
		{ID: imageID, Digests: []string{"runner@" + imageID}},
	}
	inventory, err := f.service.ListImages(t.Context())
	if err != nil || len(inventory.Images) != 3 {
		t.Fatalf("inventory: %#v %v", inventory, err)
	}
	if inventory.Images[0].RemovalBlocked != "" || inventory.Images[1].RemovalBlocked == "" ||
		inventory.Images[2].RemovalBlocked != "Retained Runner "+desired.ID {
		t.Fatalf("incorrect protection: %#v", inventory.Images)
	}
	for _, image := range f.registry.images[1:] {
		if _, err := f.service.RemoveImage(t.Context(), image.ID, "protected-"+image.ID); !errors.Is(
			err,
			errs.New(errs.KindResourceInUse, ""),
		) {
			t.Fatalf("protected image accepted removal: %v", err)
		}
	}
	f.registry.removeEnabled = true
	task := nativeAcceptImageRemoval(t, f, fetched.ConfigDigest)
	if _, err := f.execute(t, task.TaskID); err != nil || f.registry.removals != 1 {
		t.Fatalf("unrelated deletion: %v", err)
	}
	if len(f.registry.images) != 2 || f.registry.images[1].ID != imageID {
		t.Fatal("removal changed protected images")
	}
}

// IMG-02/03: actual native deletion advances the publication fence. A lost
// Docker result retains it; neither reconnect nor Retry repeats that effect.
func TestNativeImageRemovalFencesPublicationAndPreservesUnknownEffects(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		name := "confirmed"
		if unknown {
			name = "unknown"
		}
		t.Run(name, func(t *testing.T) {
			f := newNativeImageFixture(t)
			image := "sha256:" + strings.Repeat("a", 64)
			f.registry.images = []imagefetch.LocalImage{{ID: image}}
			f.registry.removeEnabled = true
			if unknown {
				f.registry.removeFailure = errs.New(errs.KindInternal, "lost Docker response")
			}
			resolver := imagedelivery.SelectionResolver{Images: nativeImageSelection{}, Store: f.store}
			stale := imagefence.WithScope(t.Context())
			if _, err := resolver.ResolveWorkloadImages(stale, "agent", nil); err != nil {
				t.Fatal(err)
			}
			accepted := nativeAcceptImageRemoval(t, f, image)
			delivery, executionErr := f.execute(t, accepted.TaskID)
			if (executionErr != nil) != unknown || f.registry.removals != 1 {
				t.Fatalf("removal outcome: %v calls=%d", executionErr, f.registry.removals)
			}
			view, err := f.queue.ReadWork(t.Context(), accepted.TaskID, delivery.Item.Item.ID, 0)
			if err != nil {
				t.Fatal(err)
			}
			if unknown {
				if view.Item.Item.Phase != workexecution.Attention {
					t.Fatal("unknown removal released effect ownership")
				}
				if _, err := f.retry.RetryTask(t.Context(), accepted.TaskID, "unknown-removal-retry", api.TaskRetryRequest{
					WorkItemID: delivery.Item.Item.ID, Attempt: 1,
				}); err == nil {
					t.Fatal("unknown removal accepted Retry")
				}
				f.registry.removeFailure = nil
				if err := f.service.Reconcile(t.Context(), delivery); err == nil || f.registry.removals != 1 {
					t.Fatal("reconciliation repeated or acknowledged unknown removal")
				}
			} else if view.Item.Item.Outcome != workexecution.Succeeded || len(f.registry.images) != 0 {
				t.Fatal("confirmed absence did not settle removal")
			}
			fresh := imagefence.WithScope(t.Context())
			_, selectionErr := resolver.ResolveWorkloadImages(fresh, "agent", nil)
			if (selectionErr != nil) != unknown {
				t.Fatalf("selection fence: %v", selectionErr)
			}
			result, err := f.store.Transact(stale, keyvalue.ImageSelectionConditions(stale, nil), []keyvalue.Mutation{
				{Type: keyvalue.MutationPut, Key: "/test/stale-image-publication", Value: []byte("must not publish")},
			})
			if err != nil || result.Succeeded {
				t.Fatalf("stale publication committed: %#v %v", result, err)
			}
			if _, err := keyvalue.ImageSelectionResult(stale, result); !errors.Is(
				err,
				errs.New(errs.KindResourceInUse, ""),
			) {
				t.Fatalf("stale result: %v", err)
			}
			if !unknown {
				if err := f.service.Settle(t.Context(), f.queue, delivery, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
				replayed, err := f.service.RemoveImage(t.Context(), image, "native-image-removal")
				var same api.TaskAccepted
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(replayed.Body, &same); err != nil || same.TaskID != accepted.TaskID ||
					f.registry.removals != 1 {
					t.Fatalf("deletion replay: %#v %v", same, err)
				}
			}
		})
	}
}

func nativeAcceptImageRemoval(t *testing.T, f *nativeImageFixture, image string) api.TaskAccepted {
	t.Helper()
	response, err := f.service.RemoveImage(t.Context(), image, "native-image-removal")
	if err != nil {
		t.Fatal(err)
	}
	var accepted api.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatal(err)
	}
	return accepted
}

type nativeImageSelection struct{}

func (nativeImageSelection) ResolveWorkloadImages(
	context.Context,
	string,
	[]*agentpb.WorkloadImageSelector,
) (*agentpb.WorkloadImageResolutionResult, error) {
	return &agentpb.WorkloadImageResolutionResult{}, nil
}
