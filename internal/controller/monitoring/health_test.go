package monitoring

import (
	"context"
	"testing"
	"time"

	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/internal/core"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/api"
)

type healthFixture struct {
	page         InventoryPage
	observations map[string]api.ServiceObservation
	calls        int
}

func (fixture *healthFixture) Page(context.Context, string, int) (InventoryPage, error) {
	return fixture.page, nil
}

func (fixture *healthFixture) ObserveServices(
	_ context.Context,
	services []etcdstore.Versioned[servicerecord.ServiceRecord],
) []api.ServiceObservation {
	fixture.calls++
	result := make([]api.ServiceObservation, len(services))
	for index, service := range services {
		result[index] = fixture.observations[service.Record.Desired.ID]
	}
	return result
}

// Scope health cannot turn planned stops or foreign failures into a degraded
// Project, and cached reads cannot manufacture fresh evidence or Agent calls.
func TestParentHealthKeepsOwnershipIntentAndExpiry(t *testing.T) {
	at := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	now := at
	fixture := &healthFixture{observations: make(map[string]api.ServiceObservation)}
	add := func(tenant, project, environment, id string, class metrics.OwnerClass, intent core.ServiceRuntimeIntent, state api.ServiceObservationState) {
		fixture.page.Items = append(fixture.page.Items, InventoryTarget{
			Ownership: metrics.Ownership{
				TenantID:      tenant,
				ProjectID:     project,
				EnvironmentID: environment,
				ServiceID:     id,
				Class:         class,
			},
			Service: etcdstore.Versioned[servicerecord.ServiceRecord]{Record: servicerecord.ServiceRecord{
				Desired: core.Service{ID: id}, Runtime: core.ServiceRuntime{RuntimeIntent: intent},
			}},
		})
		expires := at.Add(15 * time.Second)
		fixture.observations[id] = api.ServiceObservation{State: state, ObservedAt: &at, ExpiresAt: &expires}
	}
	add(
		"tenant",
		"project",
		"environment",
		"healthy",
		metrics.OwnerWorkload,
		core.ServiceRuntimeIntentRunning,
		api.ServiceObservationHealthy,
	)
	add(
		"tenant",
		"project",
		"environment",
		"planned-stop",
		metrics.OwnerWorkload,
		core.ServiceRuntimeIntentStopped,
		api.ServiceObservationStopped,
	)
	add(
		"another",
		"other-project",
		"other-environment",
		"foreign-failure",
		metrics.OwnerWorkload,
		core.ServiceRuntimeIntentRunning,
		api.ServiceObservationFailed,
	)
	add(
		"",
		"backing",
		"backing-environment",
		"database",
		metrics.OwnerBacking,
		core.ServiceRuntimeIntentRunning,
		api.ServiceObservationFailed,
	)
	sampler, err := NewHealthSampler(fixture, fixture, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := sampler.Collect(t.Context()); err != nil {
		t.Fatal(err)
	}
	scope := HealthScope{TenantID: "tenant", ProjectID: "project"}
	if state := sampler.State(scope); state != "healthy" {
		t.Fatalf("planned stop/foreign failure poisoned Project: %s", state)
	}
	calls := fixture.calls
	for range 3 {
		_ = sampler.State(scope)
	}
	if calls != fixture.calls {
		t.Fatal("a viewer caused another Agent observation")
	}
	fixture.page.Items[1].Service.Record.Runtime.RuntimeIntent = core.ServiceRuntimeIntentRunning
	if err := sampler.Collect(t.Context()); err != nil {
		t.Fatal(err)
	}
	if state := sampler.State(scope); state != "degraded" {
		t.Fatalf("unexpected stop = %s", state)
	}
	fixture.page.Items[1].Service.Record.Runtime.RuntimeIntent = core.ServiceRuntimeIntentStopped
	if err := sampler.Collect(t.Context()); err != nil {
		t.Fatal(err)
	}
	now = at.Add(15 * time.Second)
	if state := sampler.State(scope); state != "unavailable" {
		t.Fatalf("expired Project stayed green: %s", state)
	}
}

func TestParentHealthWithholdsPartialInventoryAndExpiresEmptyScope(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	fixture := &healthFixture{page: InventoryPage{Next: "continued"}}
	sampler, err := NewHealthSampler(fixture, fixture, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := sampler.Collect(t.Context()); err != nil {
		t.Fatal(err)
	}
	if state := sampler.State(HealthScope{TenantID: "tenant"}); state != "unavailable" {
		t.Fatalf("partial inventory = %s", state)
	}
	fixture.page.Next = ""
	if err := sampler.Collect(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := sampler.Collect(t.Context()); err != nil {
		t.Fatal(err)
	}
	if state := sampler.State(HealthScope{TenantID: "tenant"}); state != "absent" {
		t.Fatalf("fresh empty inventory = %s", state)
	}
	now = now.Add(time.Minute)
	if state := sampler.State(HealthScope{TenantID: "tenant"}); state != "unavailable" {
		t.Fatalf("stale empty inventory = %s", state)
	}
}
