package monitoringwire

import (
	"math"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// A response cannot expand the captured resource selection or inject invalid
// numerical evidence that would poison persisted rates and chart aggregation.
func TestMeasurementValidationBindsResultToRequest(t *testing.T) {
	const id = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	request := &agentpb.CollectMonitoringSamples{RequestId: id, Targets: []*agentpb.ServiceObservationTarget{{
		EnvironmentId: "env_" + id, ServiceId: "svc_" + id, ReleaseId: "dep_" + id, RuntimeRole: "singleton",
		Workloads: []*agentpb.ServiceWorkloadObservation{
			{PlanId: "plan_" + id, RenderGeneration: 1, ComposeName: "api"},
		},
	}}}
	result := &agentpb.MonitoringSamplesResult{RequestId: id, Services: []*agentpb.MonitoringServiceSample{{
		ServiceId: "svc_" + id, ReleaseId: "dep_" + id, Containers: []*agentpb.MonitoringContainerSample{{
			ContainerId: strings.Repeat("a", 64), Replica: 1, ObservedAtUnixNano: 1,
		}},
	}}}
	if err := ValidateResult(request, result); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"foreign-service", "duplicate-container", "unknown-volume", "nonfinite-limit", "counter-inconsistency"} {
		t.Run(mode, func(t *testing.T) {
			changed := proto.CloneOf(result)
			row := changed.Services[0]
			sample := row.Containers[0]
			switch mode {
			case "foreign-service":
				row.ServiceId = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
			case "duplicate-container":
				row.Containers = append(row.Containers, proto.CloneOf(sample))
			case "unknown-volume":
				sample.VolumeIds = []string{"vol_" + id}
			case "nonfinite-limit":
				sample.CpuLimitCores = proto.Float64(math.Inf(1))
			case "counter-inconsistency":
				sample.MemoryUsageBytes, sample.MemoryInactiveFileBytes = proto.Uint64(1), proto.Uint64(2)
			}
			if ValidateResult(request, changed) == nil {
				t.Fatal("unbound or invalid measurement accepted")
			}
		})
	}
}
