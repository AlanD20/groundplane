package app

import (
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: idempotency must compare the verified logical Blueprint rather than unstable multipart framing or map order.
func TestEnvironmentBlueprintIntentManifestIsCanonical(t *testing.T) {
	bundle := core.BlueprintBundle{
		RootPath: "blueprint.yaml", ComposeSources: []string{"blueprint.yaml"},
		Files:         []core.BlueprintFile{{Path: "blueprint.yaml", Content: []byte("services: {}\n")}},
		Interpolation: map[string]string{"ZED": "2", "ALPHA": "1"},
	}
	manifest, err := environmentBlueprintIntentManifest(bundle)
	if err != nil {
		t.Fatalf("environmentBlueprintIntentManifest() error = %v", err)
	}
	if manifest.FormatVersion != 1 || manifest.Files[0].Part != "file-000001" ||
		!reflect.DeepEqual(manifest.Interpolation, []idempotentintent.Interpolation{
			{Name: "ALPHA", Value: "1"}, {Name: "ZED", Value: "2"},
		}) {
		t.Fatalf("manifest = %#v", manifest)
	}
}

func TestPrepareEnvironmentBlueprintServiceChangesPreservesRuntimeIntent(t *testing.T) {
	// Rationale: Blueprint desired replacement must preserve a stopped Service's
	// Controller-owned runtime intent byte-for-byte.
	t.Parallel()
	at := time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	serviceID := ids.NewAt(ids.KindService, at, 2)
	record, err := etcd.NewServiceRecord(environmentID, core.Service{
		ID: serviceID, Name: "api", Image: "app:old",
	}, "")
	if err != nil {
		t.Fatalf("NewServiceRecord() error = %v", err)
	}
	record, err = etcd.SetServiceRuntimeIntent(record, core.ServiceRuntimeIntentStopped)
	if err != nil {
		t.Fatalf("SetServiceRuntimeIntent() error = %v", err)
	}
	current := etcd.Versioned[etcd.ServiceRecord]{Record: record, Revision: 7, ReadRevision: 9}

	changes, err := prepareEnvironmentBlueprintServiceChanges(
		environmentID,
		[]core.Service{{ID: serviceID, Name: "api", Image: "app:new"}},
		[]etcd.Versioned[etcd.ServiceRecord]{current},
	)
	if err != nil {
		t.Fatalf("prepareEnvironmentBlueprintServiceChanges() error = %v", err)
	}
	if len(changes) != 1 || changes[0].Current == nil ||
		changes[0].Record.Desired.Image != "app:new" || changes[0].Record.Runtime != record.Runtime {
		t.Fatalf("changes = %#v", changes)
	}
}

func TestPrepareEnvironmentBlueprintServiceChangesStartsNewServiceRunning(t *testing.T) {
	// Rationale: a Service first introduced by Blueprint starts with the exact
	// running runtime intent accepted by ADR 0028.
	t.Parallel()
	at := time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 3)
	serviceID := ids.NewAt(ids.KindService, at, 4)

	changes, err := prepareEnvironmentBlueprintServiceChanges(
		environmentID,
		[]core.Service{{ID: serviceID, Name: "worker", Image: "app:1"}},
		nil,
	)
	if err != nil {
		t.Fatalf("prepareEnvironmentBlueprintServiceChanges() error = %v", err)
	}
	if len(changes) != 1 || changes[0].Current != nil ||
		changes[0].Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
		t.Fatalf("changes = %#v", changes)
	}
}

func TestPrepareEnvironmentBlueprintRouteChangesPreservesImmutableTarget(t *testing.T) {
	// Rationale: Blueprint replacement may change Route exposure but must not
	// silently retarget an existing immutable host/path identity.
	t.Parallel()
	at := time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 5)
	routeID := ids.NewAt(ids.KindRoute, at, 6)
	serviceID := ids.NewAt(ids.KindService, at, 7)
	record, err := etcd.NewRouteRecord(environmentID, core.Route{
		ID: routeID, Host: "app.example.com", Path: "/app/*",
		TargetServiceID: serviceID, TargetPort: 8080, Exposure: "public",
	})
	if err != nil {
		t.Fatalf("NewRouteRecord() error = %v", err)
	}
	current := etcd.Versioned[etcd.RouteRecord]{Record: record, Revision: 7, ReadRevision: 9}
	desired := record.Desired
	desired.Exposure = "internal"

	changes, err := prepareEnvironmentBlueprintRouteChanges(
		environmentID,
		[]core.Route{desired},
		[]etcd.Versioned[etcd.RouteRecord]{current},
	)
	if err != nil {
		t.Fatalf("prepareEnvironmentBlueprintRouteChanges() error = %v", err)
	}
	if len(changes) != 1 || changes[0].Current == nil || changes[0].Record.Desired.Exposure != "internal" {
		t.Fatalf("changes = %#v", changes)
	}

	retargeted := desired
	retargeted.TargetServiceID = ids.NewAt(ids.KindService, at, 8)
	if _, err := prepareEnvironmentBlueprintRouteChanges(
		environmentID,
		[]core.Route{retargeted},
		[]etcd.Versioned[etcd.RouteRecord]{current},
	); err == nil {
		t.Fatal("prepareEnvironmentBlueprintRouteChanges() accepted an immutable target change")
	}
}
