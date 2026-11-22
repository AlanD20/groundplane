package monitoring

import (
	"math"
	"testing"
	"time"
)

// Rationale: cumulative counters can reset or cross an authority/gap boundary;
// joining those segments would publish invented negative or averaged rates.
func TestDerivePointSegmentsCountersAndPreservesGauges(t *testing.T) {
	at := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	source := SourceFingerprint{1}
	cpu, memoryUsage := uint64(1_000_000_000), uint64(900)
	inactive, memoryLimit, receive, transmit := uint64(200), uint64(2_000), uint64(1_000), uint64(2_000)
	blockRead, blockWrite := uint64(100), uint64(50)
	previous := RawContainerSample{
		At: at, ContainerID: "container-a", Source: source,
		CPUTotalNanoseconds: &cpu, MemoryUsageBytes: &memoryUsage, MemoryInactiveFileBytes: &inactive,
		MemoryLimitBytes: &memoryLimit, NetworkReceiveBytes: &receive, NetworkTransmitBytes: &transmit,
		BlockReadBytes: &blockRead, BlockWriteBytes: &blockWrite,
	}
	current := previous
	current.At = at.Add(15 * time.Second)
	currentCPU, currentMemory := cpu+3_000_000_000, uint64(1_200)
	currentReceive, currentTransmit := receive+1_500, transmit+3_000
	current.CPUTotalNanoseconds, current.MemoryUsageBytes = &currentCPU, &currentMemory
	current.NetworkReceiveBytes, current.NetworkTransmitBytes = &currentReceive, &currentTransmit
	currentBlockRead, currentBlockWrite := uint64(250), uint64(350)
	current.BlockReadBytes, current.BlockWriteBytes = &currentBlockRead, &currentBlockWrite
	point, reason, err := DerivePoint(&previous, current, MaximumSampleGap)
	if err != nil {
		t.Fatal(err)
	}
	if reason != SegmentContinued || point.CPUCores == nil || math.Abs(*point.CPUCores-0.2) > 0.000001 ||
		point.NetworkReceiveBytesPerSecond == nil || *point.NetworkReceiveBytesPerSecond != 100 ||
		point.MemoryWorkingSetBytes == nil || *point.MemoryWorkingSetBytes != 1_000 ||
		point.BlockReadBytesPerSecond == nil || *point.BlockReadBytesPerSecond != 10 {
		t.Fatalf("unexpected continuous point: reason=%s point=%+v", reason, point)
	}

	reset := current
	reset.At = current.At.Add(SampleInterval)
	resetCPU := uint64(1)
	reset.CPUTotalNanoseconds = &resetCPU
	point, reason, err = DerivePoint(&current, reset, MaximumSampleGap)
	if err != nil || reason != SegmentCounterReset || point.CPUCores != nil || point.MemoryWorkingSetBytes == nil {
		t.Fatalf("counter reset crossed a segment: reason=%s point=%+v err=%v", reason, point, err)
	}

	changed := current
	changed.At = current.At.Add(SampleInterval)
	changed.Source = SourceFingerprint{2}
	_, reason, err = DerivePoint(&current, changed, MaximumSampleGap)
	if err != nil || reason != SegmentSourceChange {
		t.Fatalf("source change was not segmented: reason=%s err=%v", reason, err)
	}

	gapped := current
	gapped.At = current.At.Add(MaximumSampleGap + time.Nanosecond)
	_, reason, err = DerivePoint(&current, gapped, MaximumSampleGap)
	if err != nil || reason != SegmentGap {
		t.Fatalf("long gap was not segmented: reason=%s err=%v", reason, err)
	}
}

// Rationale: host CPU ticks are unit-independent only as a busy/total ratio;
// treating them as nanoseconds would make utilization depend on kernel USER_HZ.
func TestDeriveHostPointUsesTickRatioAndCapacity(t *testing.T) {
	at := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	memoryUsed, memoryTotal := uint64(4_000), uint64(8_000)
	diskUsed, diskTotal := uint64(10_000), uint64(20_000)
	receive, transmit := uint64(100), uint64(200)
	previous := RawHostSample{
		At: at, HostID: "host-a", Boot: SourceFingerprint{1}, LogicalCPUCores: 4,
		CPUTotalTicks: 100, CPUBusyTicks: 20, NetworkInterfaces: SourceFingerprint{2},
		NetworkReceiveBytes: &receive, NetworkTransmitBytes: &transmit,
	}
	current := previous
	current.At = at.Add(5 * time.Second)
	current.CPUTotalTicks, current.CPUBusyTicks = 200, 70
	currentReceive, currentTransmit := uint64(600), uint64(700)
	current.NetworkReceiveBytes, current.NetworkTransmitBytes = &currentReceive, &currentTransmit
	current.MemoryUsedBytes, current.MemoryTotalBytes = &memoryUsed, &memoryTotal
	current.DiskUsedBytes, current.DiskTotalBytes = &diskUsed, &diskTotal
	point, reason, err := DeriveHostPoint(&previous, current, MaximumSampleGap)
	if err != nil || reason != SegmentContinued || point.CPUUtilizationRatio == nil ||
		*point.CPUUtilizationRatio != 0.5 || point.CPUCores == nil || *point.CPUCores != 2 ||
		point.NetworkReceiveBytesPerSecond == nil || *point.NetworkReceiveBytesPerSecond != 100 ||
		point.DiskUsedBytes == nil || *point.DiskUsedBytes != diskUsed {
		t.Fatalf("unexpected Host point: reason=%s point=%+v err=%v", reason, point, err)
	}

	withoutNetwork := current
	withoutNetwork.At = current.At.Add(5 * time.Second)
	withoutNetwork.CPUTotalTicks, withoutNetwork.CPUBusyTicks = 300, 120
	withoutNetwork.NetworkInterfaces = SourceFingerprint{}
	withoutNetwork.NetworkReceiveBytes, withoutNetwork.NetworkTransmitBytes = nil, nil
	point, reason, err = DeriveHostPoint(&current, withoutNetwork, MaximumSampleGap)
	if err != nil || reason != SegmentSourceChange || point.NetworkReceiveBytesPerSecond != nil {
		t.Fatalf("Host network source change was not segmented: reason=%s point=%+v err=%v", reason, point, err)
	}
	if point.CPUCores == nil || point.MemoryUsedBytes == nil {
		t.Fatalf("missing Host network hid unrelated metrics: point=%+v", point)
	}
}

func TestDerivePointKeepsMemoryWhenCountersAreUnavailable(t *testing.T) {
	at := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	usage, inactive := uint64(1_000), uint64(100)
	point, reason, err := DerivePoint(nil, RawContainerSample{
		At: at, ContainerID: "container-a", Source: SourceFingerprint{1},
		MemoryUsageBytes: &usage, MemoryInactiveFileBytes: &inactive,
	}, MaximumSampleGap)
	if err != nil || reason != SegmentFirstSample || point.CPUCores != nil ||
		point.NetworkReceiveBytesPerSecond != nil || point.MemoryWorkingSetBytes == nil ||
		*point.MemoryWorkingSetBytes != 900 {
		t.Fatalf("optional counters hid memory: reason=%s point=%+v err=%v", reason, point, err)
	}
}

// Rationale: malformed numerics must be rejected before SQLite; allowing NaN,
// infinity, or impossible host capacity ratios poisons every later rollup.
func TestMeasurementValidationRejectsInvalidNumerics(t *testing.T) {
	at := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	nan := math.NaN()
	service := measurement("service-a", OwnerWorkload, at, &nan, "", nil)
	if err := ValidateMeasurement(service); err == nil {
		t.Fatal("service NaN was accepted")
	}
	ratio := 1.1
	used, total := uint64(101), uint64(100)
	host := HostMeasurement{
		HostID: "host-a",
		Point:  MetricPoint{At: at, CPUUtilizationRatio: &ratio, MemoryUsedBytes: &used, MemoryTotalBytes: &total},
	}
	if err := ValidateHostMeasurement(host); err == nil {
		t.Fatal("invalid host ratio and capacity were accepted")
	}
}

// Rationale: parent usage owns workloads only, and a stable shared Volume must
// be counted once even when several Services mount it.
func TestAggregateWorkloadsExcludesBackingAndDeduplicatesVolumes(t *testing.T) {
	at := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	one, two, hundred, huge := 1.0, 2.0, uint64(100), uint64(10_000)
	measurements := []ServiceMeasurement{
		measurement("service-a", OwnerWorkload, at, &one, "volume-shared", &hundred),
		measurement("service-b", OwnerWorkload, at, &two, "volume-shared", &hundred),
		measurement("backing-a", OwnerBacking, at, &two, "volume-backing", &huge),
	}
	aggregate, err := AggregateWorkloads(measurements)
	if err != nil {
		t.Fatal(err)
	}
	if aggregate.Point.CPUCores == nil || *aggregate.Point.CPUCores != 3 ||
		aggregate.Point.PersistentVolumeUsedBytes == nil || *aggregate.Point.PersistentVolumeUsedBytes != 100 ||
		aggregate.Coverage.Eligible != 2 || aggregate.Coverage.Sampled != 2 ||
		aggregate.Coverage.VolumeEligible != 1 || aggregate.Coverage.VolumeSampled != 1 {
		t.Fatalf("unexpected workload aggregate: %+v", aggregate)
	}
}

func measurement(
	serviceID string,
	class OwnerClass,
	at time.Time,
	cpu *float64,
	volumeID string,
	used *uint64,
) ServiceMeasurement {
	return ServiceMeasurement{
		Ownership: Ownership{
			TenantID: "tenant-a", ProjectID: "project-a", EnvironmentID: "environment-a",
			ServiceID: serviceID, Class: class,
		},
		Point: MetricPoint{At: at, CPUCores: cpu}, Available: true,
		Volumes: []VolumeUsage{{VolumeID: volumeID, UsedBytes: used}},
	}
}
