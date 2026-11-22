package monitoring

import (
	"context"
	"strings"
	"testing"
	"time"

	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/internal/infra/hoststats"
	"github.com/AlanD20/groundplane/internal/infra/monitoringstore"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Lost/corrupt derived history must not hide a valid live Host reading, refresh
// its timestamp, or make monitoring a prerequisite for Controller operation.
func TestMonitoringHostReadSurvivesUnavailableHistoryAndExpires(t *testing.T) {
	at := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	source := &hostFixture{value: hoststats.UsageSnapshot{At: at, BootID: "boot",
		Capacity: hoststats.Snapshot{
			CPUCores: 4,
			Memory:   hoststats.Resource{Total: 4096, Used: 1024},
			Disk:     hoststats.Resource{Total: 8192, Used: 1024},
		}}}
	host, err := NewHostSampler(source, LocalHost)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Collect(t.Context()); err != nil {
		t.Fatal(err)
	}
	now := at.Add(time.Second)
	reader := &Reader{host: host, now: func() time.Time { return now }}
	response, err := reader.Show(t.Context(), Query{Kind: "host", ID: LocalHost})
	if err != nil {
		t.Fatal(err)
	}
	if response.HistoryStatus != "unavailable" || response.Host == nil || response.Host.Live.Status != "fresh" ||
		response.Host.Live.Values.MemoryUsedBytes == nil || *response.Host.Live.Values.MemoryUsedBytes.Average != 1024 {
		t.Fatalf("live Host was lost with history: %#v", response)
	}
	if response.Host.Live.ObservedAt == nil || !response.Host.Live.ObservedAt.Equal(at) {
		t.Fatal("read refreshed sample timestamp")
	}
	now = now.Add(time.Minute)
	response, err = reader.Show(t.Context(), Query{Kind: "host", ID: LocalHost})
	if err != nil || response.Host.Live.Status != "stale" {
		t.Fatalf("old Host reading = %#v, %v", response.Host, err)
	}
	if !response.Host.Live.ObservedAt.Equal(at) {
		t.Fatal("stale reading timestamp changed")
	}
}

// Parent totals need complete aligned inventories and must not charge shared
// Backing infrastructure or a different Tenant to the selected owner.
func TestMonitoringAggregateRejectsIncompleteInventoryAndForeignOwners(t *testing.T) {
	at := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	makeSample := func(tenant, project, service string, class metrics.OwnerClass, used uint64) ServiceSample {
		point := metrics.MetricPoint{At: at, MemoryWorkingSetBytes: &used}
		return ServiceSample{
			Ownership: metrics.Ownership{
				TenantID:      tenant,
				ProjectID:     project,
				EnvironmentID: "env",
				ServiceID:     service,
				Class:         class,
			},
			Point:     point,
			Summaries: PointSummaries(point, 1, 1),
			Available: true,
		}
	}
	snapshot := CollectionSnapshot{At: at, Complete: true, Services: []ServiceSample{
		makeSample("tenant", "project", "one", metrics.OwnerWorkload, 100),
		makeSample("tenant", "other-project", "two", metrics.OwnerWorkload, 200),
		makeSample("", "backing", "database", metrics.OwnerBacking, 9000),
		makeSample("another-tenant", "unrelated", "three", metrics.OwnerWorkload, 8000),
	}}
	otherEnvironment := makeSample("tenant", "project", "four", metrics.OwnerWorkload, 50)
	otherEnvironment.Ownership.EnvironmentID = "another-env"
	snapshot.Services = append(snapshot.Services, otherEnvironment)
	scope := monitoringstore.AggregateScope{TenantID: "tenant", ProjectID: "project"}
	all := aggregateLive(snapshot, monitoringstore.AggregateScope{TenantID: "tenant"}, at)
	project := aggregateLive(snapshot, scope, at)
	scope.EnvironmentID = "env"
	environment := aggregateLive(snapshot, scope, at)
	if all.Values.MemoryWorkingSetBytes == nil || *all.Values.MemoryWorkingSetBytes.Average != 350 ||
		all.Coverage.EligibleResources != 3 {
		t.Fatalf("Tenant total charged wrong ownership: %#v", all)
	}
	if project.Values.MemoryWorkingSetBytes == nil || *project.Values.MemoryWorkingSetBytes.Average != 150 {
		t.Fatalf("Project total = %#v", project)
	}
	if environment.Values.MemoryWorkingSetBytes == nil || *environment.Values.MemoryWorkingSetBytes.Average != 100 ||
		environment.Coverage.EligibleResources != 1 {
		t.Fatalf("Environment total charged the sibling Environment: %#v", environment)
	}
	snapshot.Complete = false
	if partial := aggregateLive(snapshot, scope, at); partial.Status != "unavailable" ||
		partial.Values.MemoryWorkingSetBytes != nil {
		t.Fatalf("partial inventory claimed a total: %#v", partial)
	}
}

func TestMonitoringContainerPagesStayScopedAndKeepStorageSampleTime(t *testing.T) {
	at := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	reader := &Reader{now: func() time.Time { return at }}
	used := uint64(1234)
	sample := ServiceSample{
		Ownership:          metrics.Ownership{ServiceID: "svc-one"},
		Available:          true,
		ContainersComplete: true,
		Point:              metrics.MetricPoint{At: at},
		StorageObservedAt: at.Add(
			-45 * time.Second,
		),
		Volumes: []metrics.VolumeUsage{{VolumeID: "volume", UsedBytes: &used}},
		Containers: []ContainerSample{
			{ID: strings.Repeat("b", 64), ObservedAt: at},
			{ID: strings.Repeat("a", 64), ObservedAt: at},
		},
	}
	first, err := reader.serviceLive(sample, Query{Limit: 1})
	if err != nil || len(first.Containers.Items) != 1 || first.Containers.NextCursor == "" ||
		first.Containers.Items[0].ID != strings.Repeat("a", 64) {
		t.Fatalf("first container page: %#v, %v", first, err)
	}
	if !first.Volumes[0].ObservedAt.Equal(sample.StorageObservedAt) {
		t.Fatal("storage was falsely refreshed with live CPU")
	}
	second, err := reader.serviceLive(sample, Query{Limit: 1, Cursor: first.Containers.NextCursor})
	if err != nil || len(second.Containers.Items) != 1 || second.Containers.NextCursor != "" ||
		second.Containers.Items[0].ID != strings.Repeat("b", 64) {
		t.Fatalf("second container page: %#v, %v", second, err)
	}
	sample.Ownership.ServiceID = "svc-two"
	_, err = reader.serviceLive(sample, Query{Limit: 1, Cursor: first.Containers.NextCursor})
	if kind, _ := errs.KindOf(err); kind != errs.KindMalformedRequest {
		t.Fatalf("cross-Service cursor was accepted: %v", err)
	}
	sample.ContainersComplete = false
	limited, err := reader.serviceLive(sample, Query{Limit: 1})
	if err != nil || limited.ContainersStatus != "unavailable" || len(limited.Containers.Items) != 0 {
		t.Fatalf("bounded cache claimed a complete container list: %#v, %v", limited, err)
	}
	sample.ContainersComplete, sample.Available = true, false
	unknown, err := reader.serviceLive(sample, Query{Limit: 1})
	if err != nil || unknown.ContainersStatus != "unavailable" || len(unknown.Containers.Items) != 0 {
		t.Fatalf("missing runtime evidence claimed an empty complete list: %#v, %v", unknown, err)
	}
	if _, err := reader.Show(context.Background(), Query{Kind: "host", ID: "foreign"}); err == nil {
		t.Fatal("non-local Host selector accepted")
	}
}
