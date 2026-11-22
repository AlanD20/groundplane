package monitoringreader

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

type sampleBody struct {
	io.Reader
	closed bool
}

func (body *sampleBody) Close() error { body.closed = true; return nil }

type sampleEngine struct {
	listed             []container.Summary
	inspected          container.InspectResponse
	changedRestart     bool
	changedStartedAt   bool
	reads, inspections int
	body               *sampleBody
}

func (engine *sampleEngine) ContainerList(
	context.Context,
	client.ContainerListOptions,
) (client.ContainerListResult, error) {
	return client.ContainerListResult{Items: engine.listed}, nil
}

func (engine *sampleEngine) ContainerInspect(
	context.Context,
	string,
	client.ContainerInspectOptions,
) (client.ContainerInspectResult, error) {
	engine.inspections++
	value := engine.inspected
	if engine.changedRestart && engine.inspections > 1 {
		value.RestartCount++
	}
	if engine.changedStartedAt && engine.inspections > 1 {
		value.State = &container.State{Status: "running", StartedAt: "2026-11-22T11:59:30Z"}
	}
	return client.ContainerInspectResult{Container: value}, nil
}

func (engine *sampleEngine) ContainerStats(
	context.Context,
	string,
	client.ContainerStatsOptions,
) (client.ContainerStatsResult, error) {
	engine.reads++
	return client.ContainerStatsResult{Body: engine.body}, nil
}
func (*sampleEngine) DiskUsage(context.Context, client.DiskUsageOptions) (client.DiskUsageResult, error) {
	return client.DiskUsageResult{}, nil
}

func sampleFixture(t *testing.T) (*Reader, *sampleEngine, *agentpb.CollectMonitoringSamples) {
	t.Helper()
	const suffix = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	target := &agentpb.ServiceObservationTarget{EnvironmentId: "env_" + suffix, ServiceId: "svc_" + suffix,
		ReleaseId: "dep_" + suffix, RuntimeRole: "slot", Slot: "blue", Workloads: []*agentpb.ServiceWorkloadObservation{
			{PlanId: "plan_" + suffix, RenderGeneration: 3, ComposeName: "api-blue"},
		}}
	labels := map[string]string{"com.groundplane.managed": "true", "com.groundplane.kind": "service",
		"com.groundplane.environment-id": target.EnvironmentId, "com.groundplane.service-id": target.ServiceId,
		"com.groundplane.release-id": target.ReleaseId, "com.groundplane.runtime-role": "slot", "com.groundplane.slot": "blue",
		"com.groundplane.plan-id": target.Workloads[0].PlanId, "com.groundplane.render-generation": "3",
		"com.docker.compose.service": "api-blue", "com.docker.compose.oneoff": "False", "com.docker.compose.container-number": "1"}
	data := []byte(
		`{"id":"` + id + `","read":"2026-11-22T12:00:00Z","cpu_stats":{"cpu_usage":{"total_usage":200}},"memory_stats":{"usage":1024,"stats":{"inactive_file":256}}}`,
	)
	engine := &sampleEngine{listed: []container.Summary{{ID: id, Labels: labels}}, inspected: container.InspectResponse{
		ID: id, Config: &container.Config{Labels: labels},
		State:      &container.State{Status: "running", StartedAt: "2026-11-22T11:59:00Z"},
		HostConfig: &container.HostConfig{Resources: container.Resources{Memory: 2048, NanoCPUs: 500_000_000}},
	}, body: &sampleBody{Reader: strings.NewReader(string(data))}}
	reader, err := New(engine, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	return reader, engine, &agentpb.CollectMonitoringSamples{
		RequestId: suffix,
		Targets:   []*agentpb.ServiceObservationTarget{target},
	}
}

// Runtime labels alone must not permit sampling another render generation.
func TestCollectRejectsChangedOwnershipAndRestart(t *testing.T) {
	for _, mode := range []string{
		"valid", "wrong-plan", "duplicate", "restart-count-during-read", "restart-started-at-during-read",
		"stats-before-start",
	} {
		t.Run(mode, func(t *testing.T) {
			reader, engine, request := sampleFixture(t)
			switch mode {
			case "wrong-plan":
				engine.inspected.Config.Labels["com.groundplane.plan-id"] = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW"
			case "duplicate":
				engine.listed = append(engine.listed, engine.listed[0])
			case "restart-count-during-read":
				engine.changedRestart = true
			case "restart-started-at-during-read":
				engine.changedStartedAt = true
			case "stats-before-start":
				engine.inspected.State.StartedAt = "2026-11-22T12:00:01Z"
			}
			result, err := reader.Collect(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			row := result.Services[0]
			if mode != "valid" {
				if !row.Unavailable || len(row.Containers) != 0 {
					t.Fatal("unproven runtime produced monitoring samples")
				}
			} else {
				if row.Unavailable || len(row.Containers) != 1 {
					t.Fatal("owned runtime was not measured")
				}
				sample := row.Containers[0]
				startedAt := time.Date(2026, 11, 22, 11, 59, 0, 0, time.UTC).UnixNano()
				if *sample.CpuTotalNanoseconds != 200 || *sample.MemoryUsageBytes != 1024 || *sample.MemoryInactiveFileBytes != 256 ||
					*sample.MemoryLimitBytes != 2048 || *sample.CpuLimitCores != 0.5 || sample.StartedAtUnixNano != startedAt ||
					sample.NetworkReceiveBytes != nil || sample.BlockReadBytes != nil {
					t.Fatal("measurements lost values, limits or unsupported-field absence")
				}
			}
			if engine.reads > 0 && !engine.body.closed {
				t.Fatal("statistics response body remains open")
			}
			if mode == "wrong-plan" && engine.reads != 0 {
				t.Fatal("sampled a container outside captured authority")
			}
		})
	}
}

// cgroup variants and absent counters have different meanings from zero.
func TestStatisticsPreserveMissingMeasurements(t *testing.T) {
	now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	for _, field := range []string{"total_inactive_file", "inactive_file", "missing"} {
		data := `{"id":"test","read":"2026-11-22T12:00:00Z","memory_stats":{"usage":1024,"stats":{"` + field + `":128}},"networks":{}}`
		row, err := decodeStatistics(strings.NewReader(data), "test", now)
		if err != nil {
			t.Fatal(err)
		}
		if (row.MemoryInactiveFileBytes == nil) != (field == "missing") {
			t.Fatalf("%s working-set evidence lost", field)
		}
		if row.CpuTotalNanoseconds != nil || row.BlockReadBytes != nil || row.NetworkReceiveBytes == nil ||
			*row.NetworkReceiveBytes != 0 {
			t.Fatal("missing counters became zero or measured zero became absent")
		}
	}
	for _, data := range []string{`{}`, `{"id":"other","read":"2026-11-22T12:00:00Z"}`, `{"id":"test","read":"2026-11-22T11:59:00Z"}`} {
		if _, err := decodeStatistics(strings.NewReader(data), "test", now); err == nil {
			t.Fatal("unbound or stale sample was accepted")
		}
	}
}
