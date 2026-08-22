package app

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type fakeStaleAgentReader struct {
	record etcd.Versioned[etcd.LocalAgentRecord]
	err    error
}

func (reader *fakeStaleAgentReader) GetSingleton(
	context.Context,
) (etcd.Versioned[etcd.LocalAgentRecord], error) {
	return reader.record, reader.err
}

type fakeStaleAgentTaskTimeout struct {
	agentID    string
	generation uint64
	maximum    int32
	terminalAt time.Time
	calls      int
}

func (tasks *fakeStaleAgentTaskTimeout) TimeoutAgentAssignments(
	_ context.Context,
	agentID string,
	generation uint64,
	maximum int32,
	terminalAt time.Time,
) (int, error) {
	tasks.calls++
	tasks.agentID = agentID
	tasks.generation = generation
	tasks.maximum = maximum
	tasks.terminalAt = terminalAt
	return 2, nil
}

func TestStaleAgentTaskMaintenanceUsesExactReadyBoundary(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	reader := &fakeStaleAgentReader{record: etcd.Versioned[etcd.LocalAgentRecord]{Record: etcd.LocalAgentRecord{
		ID: runtimeAdapterAgentID, Generation: 7, Phase: etcd.LocalAgentPhaseReady,
		Config: etcd.LocalAgentConfig{PullIntervalSeconds: 10, MaxConcurrentTasks: 4},
	}}}
	registry := agentchannel.NewRegistry()
	session, err := registry.Open(context.Background(), runtimeAdapterAgentID, 7)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer session.Close()
	if err := session.RecordReady(now.Add(-30*time.Second), 2, "v0.4.2"); err != nil {
		t.Fatalf("RecordReady() error = %v", err)
	}
	tasks := &fakeStaleAgentTaskTimeout{}
	maintenance, err := newStaleAgentTaskMaintenance(reader, registry, tasks)
	if err != nil {
		t.Fatalf("newStaleAgentTaskMaintenance() error = %v", err)
	}

	if count, err := maintenance.ExpireStaleAgentTasks(context.Background(), now); err != nil || count != 0 {
		t.Fatalf("ExpireStaleAgentTasks(at boundary) count/error = %d/%v", count, err)
	}
	staleNow := now.Add(time.Nanosecond)
	if count, err := maintenance.ExpireStaleAgentTasks(context.Background(), staleNow); err != nil || count != 2 {
		t.Fatalf("ExpireStaleAgentTasks(after boundary) count/error = %d/%v", count, err)
	}
	if tasks.calls != 1 || tasks.agentID != runtimeAdapterAgentID || tasks.generation != 7 ||
		tasks.maximum != 4 || !tasks.terminalAt.Equal(staleNow) {
		t.Fatalf("timeout request = %#v", tasks)
	}
}

func TestStaleAgentTaskMaintenanceRequiresObservedMatchingReady(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	reader := &fakeStaleAgentReader{record: etcd.Versioned[etcd.LocalAgentRecord]{Record: etcd.LocalAgentRecord{
		ID: runtimeAdapterAgentID, Generation: 7, Phase: etcd.LocalAgentPhaseReady,
		Config: etcd.LocalAgentConfig{PullIntervalSeconds: 5, MaxConcurrentTasks: 2},
	}}}
	registry := agentchannel.NewRegistry()
	tasks := &fakeStaleAgentTaskTimeout{}
	maintenance, err := newStaleAgentTaskMaintenance(reader, registry, tasks)
	if err != nil {
		t.Fatalf("newStaleAgentTaskMaintenance() error = %v", err)
	}
	if count, err := maintenance.ExpireStaleAgentTasks(context.Background(), now); err != nil || count != 0 {
		t.Fatalf("ExpireStaleAgentTasks(no session) count/error = %d/%v", count, err)
	}
	session, err := registry.Open(context.Background(), runtimeAdapterAgentID, 6)
	if err != nil {
		t.Fatalf("Open(old generation) error = %v", err)
	}
	defer session.Close()
	if err := session.RecordReady(now.Add(-time.Hour), 1, "v0.4.2"); err != nil {
		t.Fatalf("RecordReady(old generation) error = %v", err)
	}
	if count, err := maintenance.ExpireStaleAgentTasks(context.Background(), now); err != nil || count != 0 {
		t.Fatalf("ExpireStaleAgentTasks(old generation) count/error = %d/%v", count, err)
	}
	if tasks.calls != 0 {
		t.Fatalf("timeout calls = %d, want zero", tasks.calls)
	}
}
