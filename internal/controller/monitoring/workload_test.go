package monitoring

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	localagentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestWorkloadCollectorRejectsChangedAgentAndSourceFences(t *testing.T) {
	for _, test := range []struct {
		name        string
		agents      *workloadTestAgents
		recheck     bool
		wantRecheck bool
	}{
		{
			name: "agent enrollment changed",
			agents: &workloadTestAgents{records: []etcdstore.Versioned[localagentrecord.LocalAgentRecord]{
				workloadAgent(1), workloadAgent(2),
			}},
			recheck: true,
		},
		{
			name: "serving source changed",
			agents: &workloadTestAgents{records: []etcdstore.Versioned[localagentrecord.LocalAgentRecord]{
				workloadAgent(1), workloadAgent(1),
			}},
			recheck: false, wantRecheck: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sources := &workloadTestSources{recheck: test.recheck, expected: 1}
			collector := newWorkloadTestCollector(
				t,
				test.agents,
				sources,
				&workloadTestChannel{},
				nil,
				fixedWorkloadClock(),
			)
			if err := collector.Collect(t.Context()); err != nil {
				t.Fatalf("Collect() error = %v", err)
			}
			sample, found := collector.CurrentService("service")
			if !found || sample.Available || len(sample.Containers) != 0 {
				t.Fatalf("stale measurement was accepted: found=%v sample=%+v", found, sample)
			}
			if sources.rechecks > 0 != test.wantRecheck {
				t.Fatalf("Recheck calls = %d, want called %v", sources.rechecks, test.wantRecheck)
			}
		})
	}
}

func TestWorkloadCollectorStartsNewSegmentOnSameContainerProcessChange(t *testing.T) {
	clock := &workloadSteppingClock{current: time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)}
	channel := &workloadTestChannel{}
	collector := newWorkloadTestCollector(
		t,
		&workloadTestAgents{records: []etcdstore.Versioned[localagentrecord.LocalAgentRecord]{workloadAgent(1)}},
		&workloadTestSources{recheck: true, expected: 1},
		channel,
		nil,
		clock.Now,
	)
	channel.restart.Store(0)
	if err := collector.Collect(t.Context()); err != nil {
		t.Fatalf("first Collect() error = %v", err)
	}
	first, _ := collector.CurrentService("service")
	if !first.Point.At.Equal(clock.current) {
		t.Fatalf("live point time = %v, want exact collection time %v", first.Point.At, clock.current)
	}
	clock.Advance(5 * time.Second)
	if err := collector.Collect(t.Context()); err != nil {
		t.Fatalf("second Collect() error = %v", err)
	}
	continued, _ := collector.CurrentService("service")
	if len(continued.Containers) != 1 || continued.Containers[0].Segment != metrics.SegmentContinued ||
		continued.Containers[0].Point.CPUCores == nil {
		t.Fatalf("continued counter was not derived: %+v", continued.Containers)
	}
	channel.startedAt.Store(2)
	clock.Advance(5 * time.Second)
	if err := collector.Collect(t.Context()); err != nil {
		t.Fatalf("manual restart Collect() error = %v", err)
	}
	restarted, _ := collector.CurrentService("service")
	if len(restarted.Containers) != 1 || restarted.Containers[0].Segment != metrics.SegmentSourceChange ||
		restarted.Containers[0].Point.CPUCores != nil ||
		restarted.Containers[0].Point.NetworkReceiveBytesPerSecond != nil {
		t.Fatalf("manual restart joined the previous counter segment: %+v", restarted.Containers)
	}
	clock.Advance(5 * time.Second)
	if err := collector.Collect(t.Context()); err != nil {
		t.Fatalf("post-restart Collect() error = %v", err)
	}
	resumed, _ := collector.CurrentService("service")
	if len(resumed.Containers) != 1 || resumed.Containers[0].Segment != metrics.SegmentContinued ||
		resumed.Containers[0].Point.CPUCores == nil ||
		resumed.Containers[0].Point.NetworkReceiveBytesPerSecond == nil {
		t.Fatalf("new process counter segment did not resume: %+v", resumed.Containers)
	}
	channel.restart.Store(1)
	clock.Advance(5 * time.Second)
	if err := collector.Collect(t.Context()); err != nil {
		t.Fatalf("policy restart Collect() error = %v", err)
	}
	policyRestarted, _ := collector.CurrentService("service")
	if len(policyRestarted.Containers) != 1 ||
		policyRestarted.Containers[0].Segment != metrics.SegmentSourceChange ||
		policyRestarted.Containers[0].Point.CPUCores != nil {
		t.Fatalf("restart count change joined the previous counter segment: %+v", policyRestarted.Containers)
	}
}

func TestWorkloadCollectorKeepsPartialMetricsUnavailableWithCoverage(t *testing.T) {
	sources := &workloadTestSources{recheck: true, expected: 2}
	collector := newWorkloadTestCollector(
		t,
		&workloadTestAgents{records: []etcdstore.Versioned[localagentrecord.LocalAgentRecord]{workloadAgent(1)}},
		sources,
		&workloadTestChannel{containers: 1},
		nil,
		fixedWorkloadClock(),
	)
	if err := collector.Collect(t.Context()); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	sample, _ := collector.CurrentService("service")
	coverage := sample.Summaries.MemoryWorkingSetBytes
	if !sample.Available || sample.ObservedReplicas != 1 || sample.ExpectedReplicas != 2 ||
		sample.Point.MemoryWorkingSetBytes != nil || coverage.ContributorValid != 1 || coverage.ContributorExpected != 2 {
		t.Fatalf("partial Service was presented as a complete total: %+v coverage=%+v", sample, coverage)
	}
	copy, _ := collector.CurrentService("service")
	copy.Containers[0].Point.MemoryWorkingSetBytes = nil
	again, _ := collector.CurrentService("service")
	if again.Containers[0].Point.MemoryWorkingSetBytes == nil {
		t.Fatal("CurrentService returned mutable collector storage")
	}
}

func TestWorkloadCollectorMarksContinuedInventoryPageIncomplete(t *testing.T) {
	collector := newWorkloadTestCollector(
		t,
		&workloadTestAgents{records: []etcdstore.Versioned[localagentrecord.LocalAgentRecord]{workloadAgent(1)}},
		&workloadTestSources{recheck: true, expected: 1},
		&workloadTestChannel{},
		nil,
		fixedWorkloadClock(),
	)
	collector.inventory = &workloadTestInventory{next: "continued"}
	if err := collector.Collect(t.Context()); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	snapshot, found := collector.Snapshot()
	if !found || snapshot.Complete || len(snapshot.Services) != 1 {
		t.Fatalf("continued inventory was presented as a complete aggregate: %+v", snapshot)
	}
}

func TestWorkloadRunPersistsOnlyClosedCompleteBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	history := &workloadTestHistory{written: make(chan []metrics.ServiceMeasurement, 1), cancel: cancel}
	var ticks atomic.Int64
	base := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return base.Add(time.Duration(ticks.Add(1)-1) * 5 * time.Second) }
	collector := newWorkloadTestCollector(
		t,
		&workloadTestAgents{records: []etcdstore.Versioned[localagentrecord.LocalAgentRecord]{workloadAgent(1)}},
		&workloadTestSources{recheck: true, expected: 1, storage: true},
		&workloadTestChannel{observedBase: base},
		history,
		clock,
	)
	collector.config.LiveInterval = time.Millisecond
	done := make(chan error, 1)
	go func() { done <- collector.Run(ctx, slog.New(slog.DiscardHandler)) }()
	select {
	case batch := <-history.written:
		if len(batch) != 1 || !batch[0].Point.At.Equal(base.Add(10*time.Second)) ||
			batch[0].Point.WritableLayerBytes == nil || len(batch[0].Volumes) != 1 ||
			batch[0].Volumes[0].UsedBytes == nil {
			t.Fatalf("closed batch = %+v", batch)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not persist the closed collection bucket")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
}

func TestWorkloadCollectorDoesNotCarryStorageAcrossRuntimeSource(t *testing.T) {
	base := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	clock := &workloadSteppingClock{current: base}
	sources := &workloadTestSources{
		recheck: true, expected: 1, storage: true, fingerprint: metrics.SourceFingerprint{1},
	}
	collector := newWorkloadTestCollector(
		t,
		&workloadTestAgents{records: []etcdstore.Versioned[localagentrecord.LocalAgentRecord]{workloadAgent(1)}},
		sources,
		&workloadTestChannel{observedBase: base},
		nil,
		clock.Now,
	)
	if err := collector.Collect(t.Context()); err != nil {
		t.Fatalf("storage Collect() error = %v", err)
	}
	stored, _ := collector.CurrentService("service")
	if stored.StorageObservedAt.IsZero() || len(stored.Volumes) != 1 {
		t.Fatalf("storage evidence missing: %+v", stored)
	}
	sources.fingerprint = metrics.SourceFingerprint{2}
	clock.Advance(5 * time.Second)
	if err := collector.Collect(t.Context()); err != nil {
		t.Fatalf("replacement Collect() error = %v", err)
	}
	replaced, _ := collector.CurrentService("service")
	if !replaced.StorageObservedAt.IsZero() || len(replaced.Volumes) != 1 ||
		replaced.Volumes[0].UsedBytes != nil || replaced.Point.WritableLayerBytes != nil {
		t.Fatalf("old runtime storage crossed a source replacement: %+v", replaced)
	}
}

func TestWorkloadCollectorWaitsForNextUTCStorageSlot(t *testing.T) {
	base := time.Date(2026, 11, 22, 12, 0, 37, 0, time.UTC)
	clock := &workloadSteppingClock{current: base}
	channel := &workloadTestChannel{observedBase: base}
	collector := newWorkloadTestCollector(
		t,
		&workloadTestAgents{records: []etcdstore.Versioned[localagentrecord.LocalAgentRecord]{workloadAgent(1)}},
		&workloadTestSources{recheck: true, expected: 1, storage: true},
		channel,
		nil,
		clock.Now,
	)
	if err := collector.Collect(t.Context()); err != nil {
		t.Fatalf("off-slot Collect() error = %v", err)
	}
	offSlot, _ := collector.CurrentService("service")
	if channel.storageCalls.Load() != 0 || !offSlot.StorageObservedAt.IsZero() {
		t.Fatalf("startup minted off-slot storage: calls=%d sample=%+v", channel.storageCalls.Load(), offSlot)
	}
	channel.observedNow = clock.Now
	clock.Advance(25 * time.Second)
	if err := collector.Collect(t.Context()); err != nil {
		t.Fatalf("next-slot Collect() error = %v", err)
	}
	onSlot, _ := collector.CurrentService("service")
	if channel.storageCalls.Load() != 1 || onSlot.StorageObservedAt.IsZero() || onSlot.Volumes[0].UsedBytes == nil {
		t.Fatalf("UTC storage slot was not collected: calls=%d sample=%+v", channel.storageCalls.Load(), onSlot)
	}
}

func TestWorkloadCollectorRestartsAfterExpiredInventoryCursor(t *testing.T) {
	collector := newWorkloadTestCollector(
		t,
		&workloadTestAgents{records: []etcdstore.Versioned[localagentrecord.LocalAgentRecord]{workloadAgent(1)}},
		&workloadTestSources{recheck: true, expected: 1},
		&workloadTestChannel{},
		nil,
		fixedWorkloadClock(),
	)
	collector.inventory, collector.cursor = &workloadExpiringInventory{}, "expired"
	if err := collector.Collect(t.Context()); err == nil {
		t.Fatal("Collect() accepted an expired inventory cursor")
	}
	collector.mu.RLock()
	cursor := collector.cursor
	collector.mu.RUnlock()
	if cursor != "" {
		t.Fatalf("expired cursor retained as %q", cursor)
	}
	if err := collector.Collect(t.Context()); err != nil {
		t.Fatalf("fresh inventory restart error = %v", err)
	}
}

func TestUintTotalOverflowIsSticky(t *testing.T) {
	maximum, one := ^uint64(0), uint64(1)
	var total uintTotal
	total.add(&maximum)
	total.add(&one)
	total.add(&one)
	value, summary := total.result(3)
	if value != nil || summary.ContributorValid != 3 || summary.Valid != 0 {
		t.Fatalf("overflow recovered into a fabricated total: value=%v summary=%+v", value, summary)
	}
}

func TestWorkloadCollectorTreatsMissingServiceRowAsUnavailable(t *testing.T) {
	collector := &WorkloadCollector{previous: make(map[string]metrics.RawContainerSample)}
	sample, raw := collector.serviceSample(
		time.Now(),
		WorkloadCapture{
			Target: InventoryTarget{Ownership: metrics.Ownership{ServiceID: "service"}},
			Source: CapturedWorkload{Expected: 1, Fingerprint: metrics.SourceFingerprint{1}},
		},
		nil,
		nil,
	)
	if sample.Available || len(sample.Containers) != 0 || len(raw) != 0 {
		t.Fatalf("missing result row was accepted: sample=%+v raw=%v", sample, raw)
	}
}

type workloadTestInventory struct{ next string }

func (inventory *workloadTestInventory) Page(context.Context, string, int) (InventoryPage, error) {
	return InventoryPage{Revision: 7, Next: inventory.next, Items: []InventoryTarget{{
		Ownership: metrics.Ownership{
			TenantID: "tenant", ProjectID: "project", EnvironmentID: "environment",
			ServiceID: "service", Class: metrics.OwnerWorkload,
		},
	}}}, nil
}

type workloadExpiringInventory struct{}

func (*workloadExpiringInventory) Page(_ context.Context, cursor string, _ int) (InventoryPage, error) {
	if cursor != "" {
		return InventoryPage{}, errs.New(errs.KindCursorExpired, "inventory compacted")
	}
	return (&workloadTestInventory{}).Page(context.Background(), "", 1)
}

type workloadTestSources struct {
	recheck     bool
	expected    uint32
	rechecks    int
	storage     bool
	fingerprint metrics.SourceFingerprint
}

func (source *workloadTestSources) Capture(context.Context, InventoryTarget) (CapturedWorkload, bool) {
	fingerprint := source.fingerprint
	if fingerprint == (metrics.SourceFingerprint{}) {
		fingerprint = metrics.SourceFingerprint{1}
	}
	var volumes []*agentpb.MonitoringVolumeTarget
	if source.storage {
		volumes = []*agentpb.MonitoringVolumeTarget{{VolumeId: "volume", EnvironmentId: "environment"}}
	}
	return CapturedWorkload{
		ObservationTarget: &agentpb.ServiceObservationTarget{
			EnvironmentId: "environment", ServiceId: "service", ReleaseId: "release",
		},
		VolumeTargets: volumes, Expected: source.expected, Fingerprint: fingerprint,
	}, true
}

func (source *workloadTestSources) Recheck(_ context.Context, values []WorkloadCapture) []bool {
	source.rechecks++
	result := make([]bool, len(values))
	for index := range result {
		result[index] = source.recheck
	}
	return result
}

type workloadTestAgents struct {
	mu      sync.Mutex
	records []etcdstore.Versioned[localagentrecord.LocalAgentRecord]
	index   int
}

func (agents *workloadTestAgents) GetSingleton(context.Context) (
	etcdstore.Versioned[localagentrecord.LocalAgentRecord], error,
) {
	agents.mu.Lock()
	defer agents.mu.Unlock()
	index := min(agents.index, len(agents.records)-1)
	agents.index++
	return agents.records[index], nil
}

func workloadAgent(revision int64) etcdstore.Versioned[localagentrecord.LocalAgentRecord] {
	return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{
		Revision: revision,
		Record: localagentrecord.LocalAgentRecord{
			ID: "agent", Generation: 1, Phase: localagentrecord.LocalAgentPhaseReady,
		},
	}
}

type workloadTestChannel struct {
	calls        atomic.Uint64
	restart      atomic.Uint64
	startedAt    atomic.Int64
	storageCalls atomic.Uint64
	containers   int
	containerID  string
	observedBase time.Time
	observedNow  func() time.Time
}

func (channel *workloadTestChannel) CollectMonitoring(
	_ context.Context,
	_ string,
	targets []*agentpb.ServiceObservationTarget,
	volumes []*agentpb.MonitoringVolumeTarget,
	storage bool,
) (*agentpb.MonitoringSamplesResult, error) {
	call := channel.calls.Add(1)
	if storage {
		channel.storageCalls.Add(1)
	}
	count := channel.containers
	if count == 0 {
		count = 1
	}
	id := channel.containerID
	if id == "" {
		id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	}
	result := &agentpb.MonitoringSamplesResult{Services: make([]*agentpb.MonitoringServiceSample, len(targets))}
	observedAt := time.Unix(0, int64(call)*int64(5*time.Second)).UTC()
	if channel.observedNow != nil {
		observedAt = channel.observedNow()
	} else if !channel.observedBase.IsZero() {
		observedAt = channel.observedBase.Add(time.Duration(call) * 5 * time.Second)
	}
	for index, target := range targets {
		row := &agentpb.MonitoringServiceSample{ServiceId: target.ServiceId, ReleaseId: target.ReleaseId}
		for ordinal := 0; ordinal < count; ordinal++ {
			cpu, memory, inactive := call*1_000_000_000+uint64(ordinal), uint64(1024), uint64(24)
			receive, transmit := call*1_000+uint64(ordinal), call*2_000+uint64(ordinal)
			startedAt := channel.startedAt.Load()
			if startedAt == 0 {
				startedAt = 1
			}
			container := &agentpb.MonitoringContainerSample{
				ContainerId: id,
				Replica:     uint32(ordinal + 1), ObservedAtUnixNano: observedAt.UnixNano(),
				RestartCount: channel.restart.Load(), StartedAtUnixNano: startedAt, CpuTotalNanoseconds: &cpu,
				MemoryUsageBytes: &memory, MemoryInactiveFileBytes: &inactive,
				NetworkReceiveBytes: &receive, NetworkTransmitBytes: &transmit,
			}
			if storage {
				writable := uint64(100 + call)
				container.WritableLayerBytes = &writable
				for _, volume := range volumes {
					container.VolumeIds = append(container.VolumeIds, volume.VolumeId)
				}
			}
			row.Containers = append(row.Containers, container)
		}
		result.Services[index] = row
	}
	if storage {
		for _, volume := range volumes {
			used := uint64(200 + call)
			result.Volumes = append(result.Volumes, &agentpb.MonitoringVolumeSample{
				VolumeId: volume.VolumeId, ObservedAtUnixNano: observedAt.UnixNano(), UsedBytes: &used,
			})
		}
	}
	return result, nil
}

type workloadTestHistory struct {
	written chan []metrics.ServiceMeasurement
	cancel  context.CancelFunc
}

func (history *workloadTestHistory) AppendBatch(_ context.Context, values []metrics.ServiceMeasurement) error {
	history.written <- values
	history.cancel()
	return nil
}

type workloadSteppingClock struct {
	mu      sync.Mutex
	current time.Time
}

func (clock *workloadSteppingClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.current
}

func (clock *workloadSteppingClock) Advance(value time.Duration) {
	clock.mu.Lock()
	clock.current = clock.current.Add(value)
	clock.mu.Unlock()
}

func fixedWorkloadClock() func() time.Time {
	return func() time.Time { return time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC) }
}

func newWorkloadTestCollector(
	t *testing.T,
	agents WorkloadAgents,
	sources WorkloadSources,
	channel WorkloadChannel,
	history WorkloadHistory,
	now func() time.Time,
) *WorkloadCollector {
	t.Helper()
	collector, err := NewWorkloadCollector(
		&workloadTestInventory{}, sources, agents, channel, history,
		DefaultWorkloadConfig(), now,
	)
	if err != nil {
		t.Fatalf("NewWorkloadCollector() error = %v", err)
	}
	return collector
}
