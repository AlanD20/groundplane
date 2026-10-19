package executionplan

import (
	"crypto/sha256"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// PrepareReleaseWorkRuntime projects the exact final runtime from native
// captured input. It is not execution proof. Settlement must pair these bytes
// with the acknowledged final serving observation and completed hooks.
func PrepareReleaseWorkRuntime(
	attempt workexecution.Attempt,
	release *agentpb.ReleaseWorkInput,
) (CandidateRuntime, error) {
	artifacts, err := OpenReleaseWorkArtifacts(attempt, release)
	if err != nil {
		return CandidateRuntime{}, err
	}
	source := artifacts.Candidate
	names := make([]string, len(artifacts.Workloads))
	for index, workload := range artifacts.Workloads {
		names[index] = workload.ComposeName
	}
	// Non-recreate strategies keep the actual stable proxy ownership. Merely
	// copying candidate labels would describe a container execution never created.
	if release.Strategy != agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE && artifacts.Prior != nil &&
		artifacts.Prior.Proxy != nil {
		source, err = retainCandidateRuntimeProxy(source, artifacts.Prior.Artifact, release.ServiceId, names)
		if err != nil {
			return CandidateRuntime{}, err
		}
	}
	current := proto.CloneOf(source)
	current.Services = nil
	for _, workload := range artifacts.Workloads {
		current.Services = append(current.Services, proto.CloneOf(workload))
	}
	target := artifacts.Workloads[0].Slot
	if artifacts.Rolling != nil {
		target = "rolling"
	} else if target == "" {
		target = "singleton"
	}
	result := CandidateRuntime{ServiceID: release.ServiceId, ReleaseID: release.ReleaseId, Target: target}
	proxy := releaseRuntimeService(
		source,
		release.ServiceId,
		"",
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
	)
	var before []byte
	if proxy != nil {
		if len(release.TrafficConfigurations) == 0 {
			return CandidateRuntime{}, invalidCandidateRuntimeProxySource()
		}
		before = slices.Clone(proxy.ProxyConfigJson)
		proxy = proto.CloneOf(proxy)
		proxy.ProxyConfigJson = slices.Clone(release.TrafficConfigurations[len(release.TrafficConfigurations)-1])
		digest := sha256.Sum256(proxy.ProxyConfigJson)
		proxy.ProxyConfigSha256 = slices.Clone(digest[:])
		generation, err := ProxyConfigGeneration(proxy.ProxyImplementation, proxy.ProxyConfigJson, release.ReleaseId)
		if err != nil {
			return CandidateRuntime{}, err
		}
		result.ProxyGeneration, result.ProxyConfigSHA256 = generation, slices.Clone(digest[:])
		current.Services = append(current.Services, proxy)
	}
	if err := sealCandidateRuntimeProjection(source, current, proxy, before); err != nil {
		return CandidateRuntime{}, err
	}
	result.CurrentArtifact, err = marshalCandidateRuntimeArtifact(current)
	if err != nil {
		return CandidateRuntime{}, err
	}
	if release.Strategy == agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_BLUE_GREEN && artifacts.Prior != nil &&
		candidateRuntimeOppositeSlots(target, artifacts.Prior.Target) {
		retained, _, _, err := projectCandidateRuntimeArtifact(
			artifacts.Prior.Artifact,
			release.ServiceId,
			artifacts.Prior.ReleaseID,
			artifacts.Prior.Target,
			false,
			nil,
		)
		if err != nil {
			return CandidateRuntime{}, err
		}
		result.RetainedPriorArtifact, err = marshalCandidateRuntimeArtifact(retained)
		if err != nil {
			return CandidateRuntime{}, err
		}
	}
	if err := ValidateNativePredecessorWitness(release.EnvironmentId, release.ServiceId, result.CurrentArtifact, result.RetainedPriorArtifact); err != nil {
		return CandidateRuntime{}, err
	}
	return result, nil
}
