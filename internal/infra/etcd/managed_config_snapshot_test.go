package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

// Rationale: a Component config's read revision must pin Service, Zone and
// Route desired-head joins after a concurrent publication, not just each
// collection's internal pagination. Runtime observations remain irrelevant.
func TestManagedConfigCollectionsRetainComponentReadRevision(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	services, store, environment, project := serviceRepositoryTestHierarchy(t)
	components, err := newComponentRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	component := componentRepositoryTestRecord(t, environment.Record.ID, 1710)
	if _, err := components.CreateEnvironmentComponent(ctx, environment, project, component); err != nil {
		t.Fatal(err)
	}
	service := serviceRepositoryTestDesired(1711, "api")
	record, err := testservices.NewServiceRecord(environment.Record.ID, service, "")
	if err != nil {
		t.Fatal(err)
	}
	seedServiceRepositoryTestRuntime(t, store, testservices.NewServiceRuntimeRecord(record))
	zone := zoneRepositoryTestZone(environment.Record.ID, 930, "edge")
	route := core.Route{
		ID: ids.NewAt(ids.KindRoute, serviceRecordTestTime(), 1712), Host: "app.example.com", Path: "/",
		TargetServiceID: service.ID, TargetPort: 8080, Exposure: "internal",
	}
	initial := withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environment.Record.ID, RevisionID: ids.NewAt(ids.KindTask, serviceRecordTestTime(), 1713),
		RenderGeneration: 1,
		DesiredServices: []testservices.EnvironmentServiceProjection{
			{EnvironmentID: environment.Record.ID, Desired: service},
		},
		DesiredZones: []testenvironmentprojection.EnvironmentZoneProjection{
			{EnvironmentID: environment.Record.ID, Desired: zone},
		},
		DesiredRoutes: []testenvironmentprojection.EnvironmentRouteProjection{{
			EnvironmentID: environment.Record.ID, Desired: route, DesiredGeneration: 1,
		}},
	})
	seedServiceRepositoryTestDesiredProjection(t, store, initial)
	read, err := components.GetComponent(ctx, component.Desired.ID)
	if err != nil {
		t.Fatal(err)
	}
	if read.ReadRevision <= read.Revision {
		t.Fatalf("fixture requires a read revision newer than Component modification: %#v", read)
	}
	first, err := services.ListServices(
		ctx,
		environment.Record.ID,
		testkeyvalue.PageRequest{Revision: read.ReadRevision},
	)
	if err != nil || len(first.Items) != 1 || first.Items[0].Record.Desired.Name != "api" {
		t.Fatalf("Service snapshot = %#v, %v", first, err)
	}
	latest := withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environment.Record.ID, RevisionID: ids.NewAt(ids.KindTask, serviceRecordTestTime(), 1714),
		RenderGeneration: 2,
	})
	seedServiceRepositoryTestDesiredProjection(t, store, latest)
	zones, err := newZoneRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := newRouteRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	fixedZones, err := zones.ListZones(
		ctx,
		environment.Record.ID,
		testkeyvalue.PageRequest{Revision: read.ReadRevision},
	)
	if err != nil || len(fixedZones.Items) != 1 || fixedZones.Items[0].Record.Desired != zone ||
		fixedZones.Revision != read.ReadRevision || fixedZones.Items[0].ReadRevision != read.ReadRevision {
		t.Fatalf("Zone snapshot = %#v, %v", fixedZones, err)
	}
	fixedRoutes, err := routes.ListRoutes(
		ctx,
		environment.Record.ID,
		testkeyvalue.PageRequest{Revision: read.ReadRevision},
	)
	if err != nil || len(fixedRoutes.Items) != 1 || fixedRoutes.Items[0].Record.Desired != route ||
		fixedRoutes.Revision != read.ReadRevision || fixedRoutes.Items[0].ReadRevision != read.ReadRevision {
		t.Fatalf("Route snapshot = %#v, %v", fixedRoutes, err)
	}
	currentRoutes, err := routes.ListRoutes(ctx, environment.Record.ID, testkeyvalue.PageRequest{})
	if err != nil || len(currentRoutes.Items) != 0 || currentRoutes.Revision <= read.ReadRevision {
		t.Fatalf("latest Routes = %#v, %v", currentRoutes, err)
	}
	last, err := components.GetComponent(ctx, component.Desired.ID)
	if err != nil || last.ReadRevision != currentRoutes.Revision || last.Revision != read.Revision {
		t.Fatalf("preview reads changed durable state: %#v, %v", last, err)
	}
}
