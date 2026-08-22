package app

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func TestLocalAgentReadServiceProjectsDurableAndLiveState(t *testing.T) {
	t.Parallel()

	lastReady := time.Date(2026, 8, 22, 10, 28, 0, 0, time.FixedZone("test", 2*60*60))
	health := localagent.Health{
		Agent: localagent.Agent{
			ID:         runtimeAdapterAgentID,
			Generation: 7,
			Phase:      localagent.PhaseReady,
			Config: localagent.Config{
				PullIntervalSeconds: 2,
				MaxConcurrentTasks:  3,
				Labels:              map[string]string{"arch": "arm64"},
			},
		},
		Healthy:   true,
		LastReady: lastReady,
		Version:   "v0.4.2",
	}
	reader := &fakeLocalAgentHealthReader{health: health}
	assignments := &fakeLocalAgentReadAssignments{result: []etcd.TaskAssignment{{}, {}}}
	service, err := newLocalAgentReadService(reader, assignments, "qa-workload-groundplane")
	if err != nil {
		t.Fatalf("newLocalAgentReadService() error = %v", err)
	}

	agent, err := service.GetAgent(context.Background(), runtimeAdapterAgentID)
	if err != nil {
		t.Fatalf("GetAgent() error = %v", err)
	}
	if agent.Status != apiTypes.AgentHealthy || agent.InFlight != 2 || agent.Host != "qa-workload-groundplane" {
		t.Fatalf("GetAgent() = %#v", agent)
	}
	if agent.Version == nil || *agent.Version != "v0.4.2" {
		t.Fatalf("GetAgent().Version = %#v", agent.Version)
	}
	if agent.LastReportAt == nil || !agent.LastReportAt.Equal(lastReady.UTC()) {
		t.Fatalf("GetAgent().LastReportAt = %#v", agent.LastReportAt)
	}
	agent.Labels["arch"] = "changed"
	if health.Agent.Config.Labels["arch"] != "arm64" {
		t.Fatal("GetAgent() leaked the durable labels map")
	}
}

func TestProjectAgentStatusIsClosed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		health  localagent.Health
		want    apiTypes.AgentStatus
		wantErr bool
	}{
		{name: "provisioning", health: localagent.Health{Agent: localagent.Agent{Phase: localagent.PhaseProvisioning}}, want: apiTypes.AgentPending},
		{name: "ready online", health: localagent.Health{Agent: localagent.Agent{Phase: localagent.PhaseReady}, Healthy: true}, want: apiTypes.AgentHealthy},
		{name: "ready offline", health: localagent.Health{Agent: localagent.Agent{Phase: localagent.PhaseReady}}, want: apiTypes.AgentDegraded},
		{name: "deleting", health: localagent.Health{Agent: localagent.Agent{Phase: localagent.PhaseDeleting}}, want: apiTypes.AgentStopped},
		{name: "invalid", health: localagent.Health{}, wantErr: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := projectAgentStatus(test.health)
			if (err != nil) != test.wantErr || got != test.want {
				t.Fatalf("projectAgentStatus() = %q, %v; want %q, error %t", got, err, test.want, test.wantErr)
			}
		})
	}
}

type fakeLocalAgentHealthReader struct {
	health localagent.Health
}

func (reader *fakeLocalAgentHealthReader) ListHealth(context.Context) ([]localagent.Health, error) {
	return []localagent.Health{reader.health}, nil
}

func (reader *fakeLocalAgentHealthReader) Health(context.Context, string) (localagent.Health, error) {
	return reader.health, nil
}

type fakeLocalAgentReadAssignments struct {
	result []etcd.TaskAssignment
}

func (reader *fakeLocalAgentReadAssignments) ListAgentAssignments(
	context.Context,
	string,
	uint64,
	int32,
) ([]etcd.TaskAssignment, error) {
	return reader.result, nil
}
