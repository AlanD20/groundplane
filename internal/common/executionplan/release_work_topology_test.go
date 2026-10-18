package executionplan

import (
	"bytes"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// SVC-15/BP-04: native live-switch delivery must reject changes that require
// recreating the serving proxy, rather than silently retaining its old settings.
func TestNativeReleaseRejectsDisruptiveProxyTopology(t *testing.T) {
	attempt, input := nativeTopologyRelease(t)
	if _, err := OpenReleaseWorkArtifacts(attempt, input); err != nil {
		t.Fatalf("unchanged topology rejected: %v", err)
	}
	for _, change := range []struct{ name, network string }{
		{"alias", "shared:\n        aliases: [changed]"},
		{"attachment", "unrelated-network: {}"},
		{"restart policy", "shared: {}\n    restart: unless-stopped"},
		{"published socket", "shared: {}\n    ports:\n      - target: 8080\n        published: '8081'\n        protocol: tcp"},
	} {
		t.Run(change.name, func(t *testing.T) {
			candidate := proto.CloneOf(input)
			candidate.Candidate.CanonicalYaml = bytes.Replace(candidate.Candidate.CanonicalYaml,
				[]byte("      shared: {}"), []byte("      "+change.network), 1)
			digest := sha256.Sum256(candidate.Candidate.CanonicalYaml)
			candidate.Candidate.YamlSha256 = digest[:]
			if _, err := OpenReleaseWorkArtifacts(attempt, candidate); err == nil {
				t.Fatal("native live switch accepted a proxy-recreating change")
			}
			first := proto.CloneOf(candidate)
			first.Predecessor = &agentpb.ReleaseWorkPredecessor{Absent: true}
			if _, err := OpenReleaseWorkArtifacts(attempt, first); err != nil {
				t.Fatalf("first deployment rejected without a serving proxy: %v", err)
			}
			candidate.Strategy = agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE
			for _, workload := range candidate.Candidate.Services {
				if workload.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
					workload.Role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON
					workload.Slot = ""
					workload.ExpectedLabels = candidateRuntimeLabels(candidateRuntimePlanID, 7,
						candidateRuntimeServiceID, "singleton", candidateRuntimeReleaseID, "")
				}
			}
			if _, err := OpenReleaseWorkArtifacts(attempt, candidate); err != nil {
				t.Fatalf("explicit recreate rejected: %v", err)
			}
		})
	}
}

func nativeTopologyRelease(t *testing.T) (workexecution.Attempt, *agentpb.ReleaseWorkInput) {
	t.Helper()
	at := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	attempt := workexecution.Attempt{
		ID: ids.New(ids.KindAttempt), WorkItemID: ids.New(ids.KindWorkItem), TaskID: ids.New(ids.KindTask),
		Number: 1, Executor: workexecution.Identity{Executor: workexecution.Agent,
			HostID: ids.New(ids.KindHost), AgentID: ids.New(ids.KindAgent), Generation: 1},
		Direction: workexecution.Forward, AssignedAt: at, Deadline: at.Add(time.Minute),
		Input: workexecution.Input{ID: candidateRuntimePlanID, SHA256: strings.Repeat("a", 64)},
	}
	priorConfig := candidateRuntimeProxyConfig(candidateRuntimePriorRelease, "green", 6)
	candidate := candidateRuntimeFixtureArtifact(t, candidateRuntimeArtifactID, candidateRuntimePlanID,
		7, candidateRuntimeReleaseID, "blue", priorConfig, false)
	prior := candidateRuntimeFixtureArtifact(t, candidateRuntimePriorArtifact, candidateRuntimePriorPlanID,
		6, candidateRuntimePriorRelease, "green", priorConfig, false)
	return attempt, &agentpb.ReleaseWorkInput{
		EnvironmentId: candidateRuntimeEnvironmentID, ServiceId: candidateRuntimeServiceID,
		ReleaseId: candidateRuntimeReleaseID, RenderGeneration: 7, Candidate: candidate,
		Strategy: agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_BLUE_GREEN,
		Predecessor: &agentpb.ReleaseWorkPredecessor{ReleaseId: candidateRuntimePriorRelease,
			Target: "green", CurrentArtifact: nativeWitnessBytes(t, prior)},
		TrafficConfigurations: [][]byte{candidateRuntimeProxyConfig(candidateRuntimeReleaseID, "blue", 7)},
	}
}
