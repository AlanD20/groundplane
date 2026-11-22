package monitoring

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/internal/infra/hoststats"
)

func TestHostDetailsResetCountersWithoutDuplicatingIdentities(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 11, 22, 12, 0, 10, 0, time.UTC)
	previous := hoststats.UsageSnapshot{
		At: at.Add(-10 * time.Second), BootID: "boot", RootDeviceMajor: 8, RootDeviceMinor: 1,
		Interfaces: []hoststats.InterfaceUsage{
			{Name: "eth0", Physical: true, ReceiveBytes: 100, TransmitBytes: 200},
			{Name: "bridge0", ReceiveBytes: 10, TransmitBytes: 20},
		},
		Devices: []hoststats.DeviceUsage{
			{Name: "sda1", Major: 8, Minor: 1, ReadBytes: 100, WriteBytes: 200},
			{Name: "sdb", Major: 8, Minor: 16, ReadBytes: 100, WriteBytes: 100},
		},
	}
	sample := HostSample{Observed: hoststats.UsageSnapshot{
		At: at, BootID: "boot", RootDeviceMajor: 8, RootDeviceMinor: 1,
		Interfaces: []hoststats.InterfaceUsage{
			{Name: "eth0", Physical: true, ReceiveBytes: 50, TransmitBytes: 250},
			{Name: "bridge0", ReceiveBytes: 30, TransmitBytes: 50},
			{Name: "dup", Physical: true, ReceiveBytes: 100},
			{Name: "dup", Physical: true, ReceiveBytes: 200},
		},
		Devices: []hoststats.DeviceUsage{
			{Name: "sda1", Major: 8, Minor: 1, Partition: true, ReadBytes: 200, WriteBytes: 400},
			{Name: "sdb", Major: 8, Minor: 16, ReadBytes: 50, WriteBytes: 150},
			{Name: "duplicate", Major: 8, Minor: 32, ReadBytes: 10},
			{Name: "duplicate-again", Major: 8, Minor: 32, ReadBytes: 20},
		},
	}, Previous: &previous}

	interfaces, devices := HostDetails(sample)
	if len(interfaces) != 3 || interfaces[1].Name != "dup" || interfaces[1].ReceiveBytesPerSecond != nil {
		t.Fatalf("interfaces were duplicated or summed: %#v", interfaces)
	}
	if interfaces[0].Name != "bridge0" || interfaces[0].ReceiveBytesPerSecond == nil ||
		*interfaces[0].ReceiveBytesPerSecond != 2 || interfaces[2].Name != "eth0" ||
		interfaces[2].ReceiveBytesPerSecond != nil {
		t.Fatalf("interface reset handling = %#v", interfaces)
	}
	if len(devices) != 3 || !devices[0].RootFilesystem || devices[0].ReadBytesPerSecond == nil ||
		*devices[0].ReadBytesPerSecond != 10 || devices[1].ReadBytesPerSecond != nil ||
		devices[2].ReadBytesPerSecond != nil {
		t.Fatalf("device reset handling = %#v", devices)
	}
}

func TestPublicHostLivePreservesFreshnessAndMissingValues(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	memory := uint64(1024)
	sample := HostSample{
		Observed: hoststats.UsageSnapshot{At: at},
		Point:    metrics.MetricPoint{At: at, MemoryUsedBytes: &memory},
	}
	live := PublicHostLive(sample, at.Add(HostLiveFreshness-time.Nanosecond))
	if live.Status != "fresh" || live.ObservedAt == nil || live.Values.MemoryUsedBytes == nil ||
		live.Values.CPUCores != nil {
		t.Fatalf("fresh live host = %#v", live)
	}
	if stale := PublicHostLive(sample, at.Add(HostLiveFreshness)); stale.Status != "stale" {
		t.Fatalf("stale host status = %q", stale.Status)
	}
}

func TestAggregateSummariesUseAlignedTotalWithoutAddingChildExtrema(t *testing.T) {
	t.Parallel()
	total := 10.0
	childMinimum, childMaximum := 1.0, 6.0
	values := AggregateSummaries(metrics.MetricPoint{CPUCores: &total}, []metrics.MetricSummaries{
		{CPUCores: metrics.MetricSummary{
			Average: &childMinimum, Minimum: &childMinimum, Maximum: &childMaximum,
			ContributorValid: 2, ContributorExpected: 3,
		}},
		{CPUCores: metrics.MetricSummary{
			Average: &childMaximum, Minimum: &childMinimum, Maximum: &childMaximum,
			ContributorValid: 1, ContributorExpected: 1,
		}},
	})
	if values.CPUCores.Average == nil || *values.CPUCores.Average != total ||
		*values.CPUCores.Minimum != total || *values.CPUCores.Maximum != total ||
		values.CPUCores.ContributorValid != 3 || values.CPUCores.ContributorExpected != 4 {
		t.Fatalf("aggregate CPU summary = %#v", values.CPUCores)
	}
}

func TestHostLoopKeepsLiveSamplingWhenHistoryIsUnavailable(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	source := &hostFixture{value: hoststats.UsageSnapshot{
		At: at, BootID: "boot", RootDeviceMajor: 8, RootDeviceMinor: 1,
		Capacity: hoststats.Snapshot{
			CPUCores: 1,
			Memory:   hoststats.Resource{Total: 2, Used: 1},
			Disk:     hoststats.Resource{Total: 2, Used: 1},
		},
	}}
	sampler, err := NewHostSampler(source, "local")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = sampler.Run(ctx, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want parent cancellation", err)
	}
	if _, found := sampler.Current(); !found {
		t.Fatal("Run() rejected live sampling because history was unavailable")
	}
}
