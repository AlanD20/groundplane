package monitoring

import (
	"context"
	"testing"
	"time"

	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/monitoringstore"
	"github.com/AlanD20/groundplane/pkg/api"
)

func TestEnvironmentHistorySurvivesUnavailableServiceObservation(t *testing.T) {
	t.Parallel()
	inventory := newInventoryFixture(t)
	fixture := environmentHistoryFixture{inventory: inventory, historyRead: make(chan struct{})}
	fixture.aggregates = []metrics.AggregatePoint{{
		Point:    metrics.MetricPoint{At: inventory.at},
		Bucket:   metrics.Bucket{Start: inventory.at, End: inventory.at.Add(metrics.SampleInterval)},
		Coverage: metrics.Coverage{Eligible: 1, Sampled: 1},
	}}
	reader := &Reader{
		hierarchy: &fixture, services: &fixture, observations: &fixture, history: &fixture,
		workloads: &WorkloadCollector{}, now: func() time.Time { return inventory.at.Add(metrics.SampleInterval) },
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	response, err := reader.Show(ctx, Query{Kind: "environment", ID: inventory.workloadEnvironment, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if response.HistoryStatus != "available" || response.Environment == nil {
		t.Fatalf("retained history disappeared while Service observation was unavailable: %s", response.HistoryStatus)
	}
	value := response.Environment
	if len(value.Services.Items) != 1 ||
		value.Services.Items[0].Observation.State != api.ServiceObservationUnavailable {
		t.Fatalf("expected one unavailable Service observation, got %+v", value.Services.Items)
	}
	if len(value.History) == 0 {
		t.Fatal("expected retained history buckets")
	}
	last := value.History[len(value.History)-1]
	if !last.Start.Equal(inventory.at) || last.Coverage.SampledResources != 1 {
		t.Fatalf("expected the retained sample at %s, got %+v", inventory.at, last)
	}
}

type environmentHistoryFixture struct {
	ReadHierarchy
	ReadServices
	historyFixture
	inventory   *inventoryFixture
	historyRead chan struct{}
}

func (fixture *environmentHistoryFixture) GetEnvironment(
	context.Context, string,
) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error) {
	return versionedInventoryRecord(fixture.inventory.revision, hierarchyrecord.EnvironmentRecord{
		ID: fixture.inventory.workloadEnvironment, ProjectID: fixture.inventory.workloadProject,
	}), nil
}

func (fixture *environmentHistoryFixture) GetProject(
	context.Context, string,
) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error) {
	return versionedInventoryRecord(fixture.inventory.revision, hierarchyrecord.ProjectRecord{
		ID: fixture.inventory.workloadProject, TenantID: fixture.inventory.tenantID,
		Kind: hierarchyrecord.ProjectKindTenant,
	}), nil
}

func (fixture *environmentHistoryFixture) ListServices(
	ctx context.Context, id string, page etcdstore.PageRequest,
) (etcdstore.Page[servicerecord.ServiceRecord], error) {
	return fixture.inventory.ListServices(ctx, id, page)
}

func (fixture *environmentHistoryFixture) ObserveServices(
	ctx context.Context, services []etcdstore.Versioned[servicerecord.ServiceRecord],
) []api.ServiceObservation {
	select {
	case <-fixture.historyRead:
	case <-ctx.Done():
	}
	observations := make([]api.ServiceObservation, len(services))
	for index := range observations {
		observations[index].State = api.ServiceObservationUnavailable
	}
	return observations
}

func (fixture *environmentHistoryFixture) ReadWorkloadAggregate(
	context.Context, monitoringstore.AggregateScope, time.Time, time.Time, time.Duration,
) ([]metrics.AggregatePoint, error) {
	close(fixture.historyRead)
	return fixture.aggregates, nil
}
