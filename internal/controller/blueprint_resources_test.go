package controller

import (
	"errors"
	"reflect"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestProjectServiceProjectionPreservesStableTopology(t *testing.T) {
	// Rationale: the durable Service projection must use reconciled ids while
	// retaining the topology fields needed by Routes and operator reads.
	t.Parallel()
	serviceID := blueprintResourceID(ids.KindService, 1)
	project := &composetypes.Project{Services: composetypes.Services{
		"api": {
			Image: "example/api:1", Command: composetypes.ShellCommand{"serve"}, Restart: "unless-stopped",
			Expose: []string{"8080"},
			Networks: map[string]*composetypes.ServiceNetworkConfig{
				"backend": {Aliases: []string{"api.internal"}}, "frontend": {},
			},
		},
	}}

	services, err := ProjectServiceProjection(project, ComposeIdentitySnapshot{
		Services: []ComposeResourceIdentity{{ID: serviceID, Name: "api"}},
	}, nil)
	if err != nil {
		t.Fatalf("ProjectServiceProjection() error = %v", err)
	}
	want := []core.Service{{
		ID: serviceID, Name: "api", Image: "example/api:1", Zones: []string{"backend", "frontend"},
		Command: []string{"serve"}, Aliases: map[string][]string{"backend": {"api.internal"}},
		Expose: []string{"8080"}, Restart: "unless-stopped", Replicas: 1,
	}}
	if !reflect.DeepEqual(services, want) {
		t.Fatalf("ProjectServiceProjection() = %#v, want %#v", services, want)
	}
}

// Rationale: release policy and lifecycle-phased dependencies are durable
// desired decisions even when they cannot be represented as native Compose.
func TestProjectServiceProjectionCarriesTypedGroundplaneServiceExtensions(t *testing.T) {
	t.Parallel()
	serviceID := blueprintResourceID(ids.KindService, 20)
	project := &composetypes.Project{Services: composetypes.Services{
		"api": {Image: "example/api:1"},
	}}
	services, err := ProjectServiceProjection(
		project,
		ComposeIdentitySnapshot{Services: []ComposeResourceIdentity{{ID: serviceID, Name: "api"}}},
		map[string]core.ServiceExtensionSpec{
			"api": {
				Release: &core.ServiceReleaseSpec{
					DefaultStrategy: core.StrategyBlueGreen,
					OnFailure:       core.OnFailureLeaveActive,
				},
				DependsOn: map[string]core.ServiceDependency{
					"migrate": {
						Condition: core.ServiceDependencyCompletedSuccessfully,
						Phases:    []core.ServiceDependencyPhase{core.ServiceDependencyPhaseDeploy},
					},
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("ProjectServiceProjection() error = %v", err)
	}
	if len(services) != 1 {
		t.Fatalf("ProjectServiceProjection() service count = %d, want 1", len(services))
	}
	service := services[0]
	if service.Strategy != core.StrategyBlueGreen || service.OnFailure != core.OnFailureLeaveActive {
		t.Fatalf("ProjectServiceProjection() release = strategy %q, on_failure %q", service.Strategy, service.OnFailure)
	}
	dependency, exists := service.DependsOn["migrate"]
	if !exists || dependency.Condition != "service_completed_successfully" ||
		len(dependency.Phases) != 1 || dependency.Phases[0] != "deploy" {
		t.Fatalf("ProjectServiceProjection() dependency = %#v, exists = %t", dependency, exists)
	}
}

func TestReconcileBlueprintRoutesResolvesTargetsAndPreservesIDs(t *testing.T) {
	// Rationale: authored labels are resolved once while immutable matches retain
	// their stable Route ids and omissions remain explicit removal work.
	t.Parallel()
	serviceID := blueprintResourceID(ids.KindService, 2)
	retainedID := blueprintResourceID(ids.KindRoute, 3)
	removedID := blueprintResourceID(ids.KindRoute, 4)
	newID := blueprintResourceID(ids.KindRoute, 5)
	changes, err := ReconcileBlueprintRoutes(
		[]core.RouteSpec{
			{Hostname: "app.example.com", Path: "/api/*", Target: "api", TargetPort: 8080, Exposure: "public"},
			{Path: "/jobs/*", Target: "api", TargetPort: 8080, Exposure: "internal"},
		},
		[]core.Service{{ID: serviceID, Name: "api"}},
		[]RouteIdentity{
			{ID: retainedID, Host: "app.example.com", Path: "/api/*"},
			{ID: removedID, Host: "old.example.com", Path: "/"},
		},
		func(kind ids.Kind) string {
			if kind != ids.KindRoute {
				t.Fatalf("allocator kind = %q", kind)
			}
			return newID
		},
	)
	if err != nil {
		t.Fatalf("ReconcileBlueprintRoutes() error = %v", err)
	}
	if len(changes.Current) != 2 || changes.Current[0].ID != newID ||
		changes.Current[0].TargetServiceID != serviceID || changes.Current[1].ID != retainedID ||
		!reflect.DeepEqual(changes.RemovedRouteIDs, []string{removedID}) {
		t.Fatalf("ReconcileBlueprintRoutes() = %#v", changes)
	}
}

func TestReconcileBlueprintRoutesRejectsUnknownAndDuplicateTargets(t *testing.T) {
	// Rationale: resolution fails before persistence when a target label is
	// missing or an authored match would violate the durable uniqueness key.
	t.Parallel()
	service := core.Service{ID: blueprintResourceID(ids.KindService, 6), Name: "api"}
	cases := []struct {
		name  string
		specs []core.RouteSpec
	}{
		{
			name:  "unknown target",
			specs: []core.RouteSpec{{Path: "/", Target: "missing", TargetPort: 8080, Exposure: "internal"}},
		},
		{
			name: "duplicate match",
			specs: []core.RouteSpec{
				{Path: "/", Target: "api", TargetPort: 8080, Exposure: "internal"},
				{Path: "/", Target: "api", TargetPort: 9000, Exposure: "internal"},
			},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := ReconcileBlueprintRoutes(test.specs, []core.Service{service}, nil, ids.New)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("ReconcileBlueprintRoutes() error = %v, want %q", err, errs.CodeValidationFailed)
			}
		})
	}
}

func TestReconcileBlueprintRoutesRejectsAllocatorIdentityReuse(t *testing.T) {
	// Rationale: a defective allocator cannot alias a new Route to an existing
	// durable Route primary.
	t.Parallel()
	routeID := blueprintResourceID(ids.KindRoute, 7)
	service := core.Service{ID: blueprintResourceID(ids.KindService, 8), Name: "api"}
	_, err := ReconcileBlueprintRoutes(
		[]core.RouteSpec{{Path: "/new", Target: "api", TargetPort: 8080, Exposure: "internal"}},
		[]core.Service{service},
		[]RouteIdentity{{ID: routeID, Host: "old.example.com", Path: "/"}},
		func(ids.Kind) string { return routeID },
	)
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("ReconcileBlueprintRoutes() error = %v, want %q", err, errs.CodeInternal)
	}
}

func blueprintResourceID(kind ids.Kind, tail uint16) string {
	return ids.NewAt(kind, time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC), int64(tail))
}
