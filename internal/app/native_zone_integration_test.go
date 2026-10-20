package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	networkcontroller "github.com/AlanD20/groundplane/internal/controller/network"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	networkmodel "github.com/AlanD20/groundplane/internal/core/network"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkwork"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// TASK-83: native Zone removal must publish one captured host operation and
// exact replay identity, not the retired generic plan or hidden cascade parent.
// Also exercises TASK-163 diagnostics under a storage outage and report replay.
// Bridge absence is supplied; this does not prove physical Docker removal.
func TestNativeZoneRemovalCapturesHostAndPendingReplay(t *testing.T) {
	f := newNativeRouteFixture(t)
	created, err := f.network.CreateZone(t.Context(), networkmodel.CreateZoneRequest{
		EnvironmentID: f.base.environmentID, Name: "isolated", Subnet: "10.40.0.0/28", Internal: true}, "native-zone-create-isolated")
	if err != nil {
		t.Fatal(err)
	}
	var zone api.Zone
	if err := json.Unmarshal(created.Body, &zone); err != nil || zone.ID == "" {
		t.Fatalf("Zone creation: %v", err)
	}
	before, found, err := f.base.blueprint.GetEnvironmentComposeProjection(t.Context(), f.base.environmentID)
	if err != nil || !found {
		t.Fatalf("Zone baseline: %v", err)
	}
	removed, err := f.network.RemoveZone(t.Context(), zone.ID, "native-zone-remove-isolated")
	if err != nil {
		t.Fatal(err)
	}
	var accepted api.TaskAccepted
	if err := json.Unmarshal(removed.Body, &accepted); err != nil || accepted.TaskID == "" ||
		removed.Status != http.StatusAccepted {
		t.Fatalf("Zone removal: %v", err)
	}
	page, err := f.base.queue.ListWork(t.Context(), accepted.TaskID, "", 10, 0)
	if err != nil || len(page.Items) != 1 {
		t.Fatalf("Zone work: %v", err)
	}
	item := page.Items[0].Item
	if item.Operation != workexecution.Network || item.TargetID != zone.ID ||
		item.Placement.Executor != workexecution.Agent ||
		item.Placement.HostID != f.base.authorization.Identity().HostID ||
		item.Input == nil ||
		item.AttemptNumber != 0 {
		t.Fatal("Zone removal lost its captured resource/host or invented an execution attempt")
	}
	inputs, err := networkwork.NewInputRepository(f.base.store)
	if err != nil {
		t.Fatal(err)
	}
	view, err := f.base.queue.ReadWork(t.Context(), accepted.TaskID, item.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	input, _, err := inputs.Load(t.Context(), view)
	if err != nil {
		t.Fatal(err)
	}
	if input.NetworkId != zone.ID || input.EnvironmentId != zone.EnvironmentID {
		t.Fatal("Zone input targets another resource")
	}
	after, found, err := f.base.blueprint.GetEnvironmentComposeProjection(t.Context(), f.base.environmentID)
	if err != nil || !found || !reflect.DeepEqual(before.Record.DesiredServices, after.Record.DesiredServices) ||
		!reflect.DeepEqual(before.Record.Volumes, after.Record.Volumes) {
		t.Fatalf("Zone removal changed unrelated desired state: %v", err)
	}
	revision := f.base.store.revision
	_, err = f.network.RemoveZone(t.Context(), zone.ID, "native-zone-remove-isolated")
	if !errors.Is(err, errs.New(errs.KindIdempotencyInProgress, "")) || f.base.store.revision != revision {
		t.Fatalf("pending Zone replay allocated more work: %v", err)
	}
	verifyNetworkDiagnosticReplay(t, f.base, accepted.TaskID, inputs, input)
}

type networkDiagnosticFaultStore struct {
	*nativeVolumeStore
	fail     bool
	failures int
}

func (store *networkDiagnosticFaultStore) Transact(ctx context.Context, conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	if store.fail {
		store.failures++
		return keyvalue.TransactionResult{}, errs.New(errs.KindInternal, "injected diagnostic storage outage")
	}
	return store.nativeVolumeStore.Transact(ctx, conditions, mutations)
}

func verifyNetworkDiagnosticReplay(t *testing.T, f *nativeVolumeFixture, taskID string,
	inputs *networkwork.InputRepository, input *agentpb.NetworkWorkInput,
) {
	t.Helper()
	ctx := t.Context()
	view := f.assign(t, taskID)
	owner, err := networkcontroller.NewAgentWork(inputs, f.queue)
	if err != nil {
		t.Fatal(err)
	}
	starts, err := workdispatch.NewAgentStarts(f.queue, []workdispatch.AgentStartRegistration{
		{Operation: workexecution.Network, Owner: owner},
	})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := workexecution.EncodeAgentAttempt(view.Attempt.Attempt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := starts.Start(ctx, f.authorization, &agentpb.WorkStartRequest{Attempt: wire}); err != nil {
		t.Fatal(err)
	}
	view, err = f.queue.ReadResume(
		ctx,
		taskID,
		view.Work.Item.Item.ID,
		view.Attempt.Attempt.ID,
		f.authorization.Identity(),
	)
	if err != nil {
		t.Fatal(err)
	}
	attempt := view.Attempt.Attempt
	progress, err := networkexecution.NewProgress(attempt, input.Action, input.NetworkId, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	begin, err := progress.Begin(attempt, input.Action, input.NetworkId, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	fault := &networkDiagnosticFaultStore{nativeVolumeStore: f.store, fail: true}
	diagnostics, err := executiondiagnostics.New(fault, f.queue)
	if err != nil {
		t.Fatal(err)
	}
	reports, err := networkcontroller.NewWorkReports(f.queue, inputs, diagnostics)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := func(progress networkexecution.Progress) {
		t.Helper()
		raw, err := networkexecution.EncodeProgress(attempt, input.Action, input.NetworkId, progress)
		if err != nil {
			t.Fatal(err)
		}
		ack, err := reports.CheckpointNetwork(ctx, f.authorization,
			&agentpb.NetworkWorkCheckpointRequest{Attempt: wire, Progress: raw})
		if err != nil || ack == nil || ack.Sequence != progress.Sequence {
			t.Fatalf("Network checkpoint acknowledgement: %v, %v", ack, err)
		}
	}
	checkpoint(begin)
	if fault.failures == 0 {
		t.Fatal("diagnostic outage was not exercised")
	}
	retained, err := f.queue.ReadResume(ctx, taskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil || retained.Checkpoint.Record == nil || retained.Checkpoint.Record.Sequence != 1 {
		t.Fatalf("diagnostic outage blocked authoritative checkpoint: %v", err)
	}
	fault.fail = false
	at := time.Now().UTC()
	confirmed, err := begin.Confirm(attempt, input.Action, input.NetworkId, networkexecution.Observation{
		NetworkID: input.NetworkId, Input: attempt.Input, Absent: true, At: at,
	}, at)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint(confirmed)
	checkpoint(confirmed) // Lost response replay must not duplicate stage events.
	checkpoint(begin)     // An older accepted boundary must not regress the result.
	snapshot, err := diagnostics.Read(ctx, taskID, attempt.WorkItemID, attempt.ID, 0)
	if err != nil || snapshot.Record == nil || snapshot.Record.Sequence != 2 || len(snapshot.Record.Stages) != 1 {
		t.Fatalf("recovered Network diagnostics: %+v, %v", snapshot, err)
	}
	stage := snapshot.Record.Stages[0]
	if stage.State != workexecution.StageSucceeded || stage.StartedAt == nil || stage.FinishedAt == nil ||
		!stage.StartedAt.Equal(*begin.BeganAt) || !stage.FinishedAt.Equal(confirmed.UpdatedAt) {
		t.Fatalf("Network diagnostics lost original Agent timing or outcome: %+v", stage)
	}
	events, err := diagnostics.Events(ctx, taskID, attempt.WorkItemID, attempt.ID, 0, 0)
	if err != nil || len(events.Items) != 2 || events.Last != 2 {
		t.Fatalf("Network replay duplicated diagnostics: %+v, %v", events, err)
	}
}

// TASK-83: accepting creation is not provisioning proof. Even an approved
// impact token cannot remove a Backing Zone whose Environment is not ready.
func TestNativeBackingZoneRemovalRequiresReadyEnvironment(t *testing.T) {
	f := newNativeRouteFixture(t)
	creation, images, _ := newNativeBackingCreationService(t, f.base)
	response, err := creation.CreateBackingService(t.Context(), api.BackingServiceCreate{
		Slug: "zone-source", Name: "Zone source", Adapter: "custom", Image: images.plan.Requested,
		NetworkPool: "10.200.0.0/24", Zone: api.BackingServiceZoneCreate{Name: "data", Subnet: "10.200.0.0/28", Internal: true}}, "native-backing-zone-source")
	if err != nil {
		t.Fatal(err)
	}
	var created api.BackingServiceCreated
	if err := json.Unmarshal(response.Body, &created); err != nil {
		t.Fatal(err)
	}
	zoneID := created.BackingService.BackingNetworkID
	impact, err := f.network.GetZoneRemovalImpact(t.Context(), zoneID)
	if err != nil || impact.ImpactToken == "" || impact.Mode != networkmodel.ZoneRemovalImpactCascade {
		t.Fatalf("Backing Zone impact: %v", err)
	}
	revision := f.base.store.revision
	if _, err := f.network.RemoveZone(t.Context(), zoneID, "native-backing-zone-no-approval"); err == nil ||
		f.base.store.revision != revision {
		t.Fatalf("Backing Zone bypassed impact approval: %v", err)
	}
	_, err = f.network.RemoveZoneWithImpact(
		t.Context(),
		zoneID,
		"native-backing-zone-approved",
		impact.ImpactToken,
	)
	if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) || f.base.store.revision != revision {
		t.Fatalf("unfinished Backing provisioning admitted Zone removal or changed state: %v", err)
	}
}
