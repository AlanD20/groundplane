package app

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	networkmodel "github.com/AlanD20/groundplane/internal/core/network"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkwork"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TASK-83: native Zone removal must publish one captured host operation and
// exact replay identity, not the retired generic plan or hidden cascade parent.
// This checks acceptance only; no bridge or container effects are supplied.
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
