package host

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/localdiag"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/hoststats"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type fakeHostStats struct {
	snapshot hoststats.Snapshot
}

func (fake fakeHostStats) Snapshot(context.Context) (hoststats.Snapshot, error) {
	return fake.snapshot, nil
}

type fakeHostDocker struct {
	version string
	err     error
}

func (fake fakeHostDocker) DockerVersion(context.Context) (string, error) {
	return fake.version, fake.err
}

type fakeHostAgentHealth struct {
	health []localagent.Health
}

func (fake fakeHostAgentHealth) ListHealth(context.Context) ([]localagent.Health, error) {
	return append([]localagent.Health(nil), fake.health...), nil
}

func TestHostSystemSourceAppliesLockedFormatting(t *testing.T) {
	// Rationale: every public Host surface must format the same raw machine
	// values without browser- or CLI-specific rounding.
	t.Parallel()

	source := &SystemSource{
		system: fakeHostStats{snapshot: hoststats.Snapshot{
			Hostname: "groundplane-test-1", Arch: "amd64", OS: "Ubuntu 24.04.3 LTS",
			Uptime: 34*24*time.Hour + 5*time.Hour, CPUModel: "Example CPU", CPUCores: 4, LoadOne: 1.5,
			Memory: hoststats.Resource{Total: 8 << 30, Used: 5 << 30},
			Disk:   hoststats.Resource{Total: 256 << 30, Used: 96 << 30},
			Swap:   hoststats.Resource{Total: 4 << 30, Used: 256 << 20},
		}},
		docker: fakeHostDocker{version: "29.1.3"},
	}
	got, err := source.SystemSnapshot(context.Background())
	if err != nil {
		t.Fatalf("SystemSnapshot() error = %v", err)
	}
	if got.Uptime != "34 days" || got.CPU.Load != 38 || got.Memory.Total != "8 GiB" ||
		got.Memory.Used != "5 GiB" || got.Memory.UsedPct != 63 || got.Swap.Used != "256 MiB" {
		t.Fatalf("SystemSnapshot() = %#v", got)
	}

	source.docker = fakeHostDocker{err: errors.New("private socket diagnostic")}
	got, err = source.SystemSnapshot(context.Background())
	if err != nil || got.Docker != hostUnavailable {
		t.Fatalf("Docker failure projection = %#v, %v", got, err)
	}
}

func TestHostEtcdSourceProjectsSafeDependencyStates(t *testing.T) {
	// Rationale: an etcd outage is Host health data and must not disclose
	// configured endpoints or turn the whole read into a raw transport error.
	t.Parallel()

	dbSize := int64(18 << 20)
	healthy := &EtcdSource{
		endpoints: []string{"private-endpoint"},
		probe: func(context.Context, []string) ([]localdiag.EtcdEndpoint, error) {
			return []localdiag.EtcdEndpoint{{Healthy: true, DBSizeBytes: &dbSize}}, nil
		},
	}
	got, err := healthy.EtcdSnapshot(context.Background())
	if err != nil || got.Node != hostEtcdNodeLabel || got.Status != apiTypes.HealthHealthy || got.DBSize != "18 MiB" {
		t.Fatalf("healthy EtcdSnapshot() = %#v, %v", got, err)
	}

	failed := &EtcdSource{
		endpoints: []string{"private-endpoint"},
		probe: func(context.Context, []string) ([]localdiag.EtcdEndpoint, error) {
			return []localdiag.EtcdEndpoint{{Healthy: false}}, errs.New(
				errs.KindStorageUnavailable,
				"private endpoint diagnostic",
			)
		},
	}
	got, err = failed.EtcdSnapshot(context.Background())
	if err != nil || got.Status != apiTypes.HealthFailed || got.DBSize != hostUnavailable {
		t.Fatalf("failed EtcdSnapshot() = %#v, %v", got, err)
	}
}

func TestHostAgentSourceUsesDurableOrBootstrapConfig(t *testing.T) {
	// Rationale: Host must remain readable before enrollment and must switch to
	// the durable singleton config without exposing map-order instability.
	t.Parallel()

	fallback := localagent.Config{
		PullIntervalSeconds: 2,
		MaxConcurrentTasks:  1,
		Labels:              map[string]string{"host": "local"},
	}
	absent := &AgentSource{health: fakeHostAgentHealth{}, fallback: fallback}
	got, err := absent.AgentSnapshot(context.Background())
	if err != nil || got.Status != apiTypes.HealthStopped || got.PullInterval != "2s" || got.MaxConcurrent != 1 {
		t.Fatalf("absent AgentSnapshot() = %#v, %v", got, err)
	}

	present := &AgentSource{health: fakeHostAgentHealth{health: []localagent.Health{{
		Agent: localagent.Agent{
			Phase: localagent.PhaseReady,
			Config: localagent.Config{
				PullIntervalSeconds: 5, MaxConcurrentTasks: 3,
				Labels: map[string]string{"zone": "edge", "arch": "amd64"},
			},
		},
		Healthy: true,
	}}}, fallback: fallback}
	got, err = present.AgentSnapshot(context.Background())
	wantLabels := []string{"arch=amd64", "zone=edge"}
	if err != nil || got.Status != apiTypes.HealthHealthy || got.PullInterval != "5s" ||
		got.MaxConcurrent != 3 || !reflect.DeepEqual(got.Labels, wantLabels) {
		t.Fatalf("present AgentSnapshot() = %#v, %v", got, err)
	}
}
