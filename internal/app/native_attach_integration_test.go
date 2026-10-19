package app

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	attachmentcontroller "github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/attachwork"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/core"
	releasedomain "github.com/AlanD20/groundplane/internal/core/release"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachprovision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ATT-01/02: a hook-free Custom Attach to an undeployed consumer must publish
// exactly one native action, preserve replay and settle without inventing host
// execution. Ready backing state is supplied; no container/network effect is
// claimed by this repository and Controller integration check.
func TestNativeCustomAttachPublicationReplayAndControllerSettlement(t *testing.T) {
	nativeControllerAttachJourney(t, "custom")
}

// ATT-01/02: explicit no-auth Valkey still exposes its captured endpoint facts,
// but must not run a credential provisioning procedure or invent an Agent attempt.
func TestNativeNoAuthValkeyAttachPublicationReplayAndControllerSettlement(t *testing.T) {
	nativeControllerAttachJourney(t, "valkey")
}

func nativeControllerAttachJourney(t *testing.T, adapter string) {
	t.Helper()
	f := newNativeRouteFixture(t)
	creation, images, _ := newNativeBackingCreationService(t, f.base)
	backingRequest := api.BackingServiceCreate{
		Slug: "shared-search", Name: "Shared search", Adapter: adapter, Image: images.plan.Requested,
		NetworkPool: "10.200.0.0/24",
		Zone:        api.BackingServiceZoneCreate{Name: "data", Subnet: "10.200.0.0/28", Internal: true},
	}
	if adapter == "valkey" {
		images.plan.Requested, images.plan.Repository = "valkey/valkey:9-alpine", "docker.io/valkey/valkey"
		backingRequest.Image, backingRequest.AdapterVersion, backingRequest.Authentication = "", "9", "none"
	}
	created, err := creation.CreateBackingService(t.Context(), backingRequest, "native-attach-backing")
	if err != nil {
		t.Fatal(err)
	}
	var backing api.BackingServiceCreated
	if err := json.Unmarshal(created.Body, &backing); err != nil {
		t.Fatal(err)
	}
	environment, err := f.base.blueprint.GetEnvironment(t.Context(), backing.BackingService.EnvironmentID)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := hierarchy.CompleteEnvironmentProvisioning(environment.Record, environment.Record.CreateTaskID, true)
	if err != nil {
		t.Fatal(err)
	}
	nativeVolumePut(t, f.base.store, hierarchy.EnvironmentKey(ready.ID), ready, hierarchy.EncodeEnvironment)
	mutations, records := nativeAttachMutations(t, f.base)
	request := api.AttachRequest{ServiceID: f.target.ID, BackingServiceID: backing.BackingService.ServiceID,
		Name: "shared-search", Credential: api.AttachCredential{Mode: api.AttachCredentialNew}}
	projectionKey := releases.ReleaseProjectionKey(request.ServiceID)
	for _, stage := range []string{"serving", "active"} {
		projection := releasedomain.ServiceProjection{
			EnvironmentID: f.base.environmentID,
			ServiceID:     request.ServiceID,
			Revision:      1,
		}
		if stage == "serving" {
			projection.ServingReleaseID = ids.New(ids.KindDeployment)
		} else {
			projection.ActiveOperationID = ids.New(ids.KindOperation)
		}
		value, err := releases.EncodeReleaseRecord("service-release-projection", projection)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.base.store.Put(t.Context(), projectionKey, value); err != nil {
			t.Fatal(err)
		}
		clear(value)
		revision := f.base.store.revision
		if _, err := mutations.CreateAttach(t.Context(), request, "native-attach-refuse-"+stage); !errors.Is(
			err, errs.New(errs.KindStateConflict, ""),
		) || f.base.store.revision != revision {
			t.Fatalf("Attach mistook %s execution with missing runtime for an undeployed Service: %v", stage, err)
		}
		if _, err := f.base.store.Delete(t.Context(), projectionKey); err != nil {
			t.Fatal(err)
		}
	}
	response, err := mutations.CreateAttach(t.Context(), request, "native-custom-attach")
	if err != nil || response.Status != http.StatusAccepted {
		t.Fatalf("native Attach acceptance: status=%d error=%v", response.Status, err)
	}
	var accepted api.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil || accepted.TaskID == "" {
		t.Fatalf("native Attach response: %v", err)
	}
	page, err := f.base.queue.ListWork(t.Context(), accepted.TaskID, "", 10, 0)
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("native Attach items: %v", err)
	}
	view, err := f.base.queue.ReadWork(t.Context(), accepted.TaskID, page.Items[0].Item.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	item, task := view.Item.Item, view.Task.Task
	if task.Action != workexecution.ActionAttach || task.Scope.EnvironmentID != f.base.environmentID ||
		item.Operation != workexecution.Attach || item.TargetID != task.TargetID ||
		item.Placement.Executor != workexecution.Controller || item.Phase != workexecution.Ready ||
		item.AttemptNumber != 0 || item.ActiveAttemptID != "" {
		t.Fatal("hook-free connection lost its scope or invented host execution")
	}
	pending, err := records.GetAttach(t.Context(), item.TargetID)
	if err != nil || pending.Record.Status != core.AttachPending || pending.Revision != view.Task.Revision ||
		pending.Record.ServiceID != request.ServiceID || pending.Record.BackingServiceID != request.BackingServiceID {
		t.Fatalf("Attach and Task were not published atomically: %v", err)
	}
	for _, key := range []string{
		attachments.AttachOwnerKey(f.base.environmentID, item.TargetID),
		attachments.AttachNameKey(f.base.environmentID, request.Name),
		attachments.AttachServiceKey(request.ServiceID, item.TargetID),
		attachments.AttachBackingServiceKey(request.BackingServiceID, item.TargetID),
	} {
		value, err := f.base.store.Get(t.Context(), key)
		if err != nil || value.Entry == nil || value.Entry.ModRevision != pending.Revision ||
			string(value.Entry.Value) != item.TargetID {
			t.Fatalf("Attach ownership was not published atomically: %s %v", key, err)
		}
	}
	inputs, err := attachprovision.NewInputRepository(f.base.store)
	if err != nil {
		t.Fatal(err)
	}
	captured, err := inputs.LoadCaptured(t.Context(), view)
	if err != nil {
		t.Fatal(err)
	}
	defer captured.Clear()
	if !captured.Generation.Record.SettlesInController() || captured.Generation.Record.AdapterKey != adapter ||
		captured.Generation.Record.ConsumerServiceID != request.ServiceID {
		t.Fatal("native capture changed the accepted connection")
	}
	if adapter == "valkey" && (captured.Generation.Record.Authentication != core.BackingAuthenticationNone ||
		captured.Generation.Record.Provision == nil || captured.Generation.Record.Provision.RequiresProvisioning ||
		len(captured.Generation.Record.GeneratedInputs.Ciphertext) == 0) {
		t.Fatal("no-auth Valkey lost encrypted endpoint facts or requested credential provisioning")
	}
	revision := f.base.store.revision
	_, err = mutations.CreateAttach(t.Context(), request, "native-custom-attach")
	if !errors.Is(err, errs.New(errs.KindIdempotencyInProgress, "")) ||
		f.base.store.revision != revision {
		t.Fatalf("pending Attach replay changed identity or wrote state: %v", err)
	}
	changed := request
	changed.Name = "different-search"
	if _, err := mutations.CreateAttach(t.Context(), changed, "native-custom-attach"); !errors.Is(
		err, errs.New(errs.KindIdempotencyMismatch, ""),
	) || f.base.store.revision != revision {
		t.Fatalf("request-key reuse changed captured intent: %v", err)
	}
	settlement, err := inputs.PrepareDirectControllerSettlement(t.Context(), view, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	defer keyvalue.ClearMutationValues(settlement.Mutations)
	f.base.store.beforeCommit = func(mutations []keyvalue.Mutation) {
		for _, mutation := range mutations {
			if mutation.Key != attachments.AttachKey(item.TargetID) {
				continue
			}
			f.base.store.beforeCommit = nil
			value, err := releases.EncodeReleaseRecord("service-release-projection", releasedomain.ServiceProjection{
				EnvironmentID: f.base.environmentID, ServiceID: request.ServiceID,
				ActiveOperationID: ids.New(ids.KindOperation), Revision: 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer clear(value)
			if _, err := f.base.store.Put(t.Context(), projectionKey, value); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	raced, err := f.base.queue.Commit(t.Context(), settlement)
	if err != nil || raced.Succeeded || f.base.store.beforeCommit != nil {
		t.Fatalf("concurrent Deploy escaped the unapplied-runtime guard: %v", err)
	}
	preserved, err := records.GetAttach(t.Context(), item.TargetID)
	if err != nil || preserved.Record.Status != core.AttachPending {
		t.Fatalf("rejected settlement partially readied the Attach: %v", err)
	}
	if _, err := f.base.store.Delete(t.Context(), projectionKey); err != nil {
		t.Fatal(err)
	}
	view, err = f.base.queue.ReadWork(t.Context(), accepted.TaskID, item.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	settlement, err = inputs.PrepareDirectControllerSettlement(t.Context(), view, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	defer keyvalue.ClearMutationValues(settlement.Mutations)
	f.base.commit(t, settlement)
	settled, err := records.GetAttach(t.Context(), item.TargetID)
	if err != nil || settled.Record.Status != core.AttachReady || settled.Record.ID != pending.Record.ID {
		t.Fatalf("Controller settlement did not preserve and ready the binding: %v", err)
	}
	finished, err := f.base.queue.ReadWork(t.Context(), accepted.TaskID, item.ID, 0)
	if err != nil || finished.Item.Item.Phase != workexecution.Settled ||
		finished.Item.Item.Outcome != workexecution.Succeeded || finished.Item.Item.AttemptNumber != 0 {
		t.Fatalf("Controller binding and work did not settle together: %v", err)
	}
	result, err := f.base.queue.Commit(t.Context(), settlement)
	if err != nil || result.Succeeded {
		t.Fatalf("stale settlement could commit twice: %v", err)
	}
	registrations, err := attachwork.DirectCompletions(inputs)
	if err != nil {
		t.Fatal(err)
	}
	completions, err := workdispatch.NewCompletions(f.base.queue, registrations)
	if err != nil {
		t.Fatal(err)
	}
	resultKey := attachprovision.DirectControllerResultKey(item.ID)
	savedResult, err := f.base.store.Get(t.Context(), resultKey)
	if err != nil || savedResult.Entry == nil {
		t.Fatalf("Controller result is missing: %v", err)
	}
	defer clear(savedResult.Entry.Value)
	if _, err := f.base.store.Put(t.Context(), resultKey, []byte("corrupt")); err != nil {
		t.Fatal(err)
	}
	revision = f.base.store.revision
	if err := completions.Finish(t.Context(), accepted.TaskID, time.Now().UTC(), time.Hour); err == nil ||
		f.base.store.revision != revision {
		t.Fatalf("Task finished without its exact Controller result: %v", err)
	}
	if _, err := f.base.store.Put(t.Context(), resultKey, savedResult.Entry.Value); err != nil {
		t.Fatal(err)
	}
	if err := completions.Finish(t.Context(), accepted.TaskID, time.Now().UTC(), time.Hour); err != nil {
		t.Fatalf("Attach Task could not finish: %v", err)
	}
	revision = f.base.store.revision
	replay, err := mutations.CreateAttach(t.Context(), request, "native-custom-attach")
	if err != nil || !bytes.Equal(replay.Body, response.Body) || f.base.store.revision != revision {
		t.Fatalf("settled Attach replay lost its original action: %v", err)
	}
}

func nativeAttachMutations(
	t *testing.T,
	f *nativeVolumeFixture,
) (*attachmentcontroller.MutationService, *etcd.AttachRepository) {
	t.Helper()
	services, err := etcd.NewServiceRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	records, err := etcd.NewAttachRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := attachmentcontroller.NewRepository(f.blueprint.HierarchyRepository, services, records)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ageinfra.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	cipher := nativeVolumeCipher{key: key}
	protector, err := secretvalue.NewProtector(cipher, cipher)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := requestidempotency.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	replays, err := etcd.NewIdempotencyRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := attachmentcontroller.NewMutationIdempotency(coordinator, replays)
	if err != nil {
		t.Fatal(err)
	}
	facts, err := attachmentcontroller.NewFactService(records, protector)
	if err != nil {
		t.Fatal(err)
	}
	mutations, err := attachmentcontroller.NewMutationService(repository, facts, idempotency)
	if err != nil {
		t.Fatal(err)
	}
	return mutations, records
}
