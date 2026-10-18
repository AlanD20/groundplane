package executionplan

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// NativeServingRuntime is the complete captured serving set, decoded from a
// validated native witness. It cannot select newer desired state or authorize
// execution without the owning Task's restoration authority.
type NativeServingRuntime struct {
	Artifact  *agentpb.ComposeArtifact
	Retained  *agentpb.ComposeArtifact
	Workloads []*agentpb.ComposeService
	Proxy     *agentpb.ComposeService
	ReleaseID string
	Target    string
	Rolling   *agentpb.RollingReleaseEvidence
}

func OpenNativeServingRuntime(environmentID, serviceID string, current, retained []byte) (NativeServingRuntime, error) {
	if err := ValidateNativePredecessorWitness(environmentID, serviceID, current, retained); err != nil {
		return NativeServingRuntime{}, err
	}
	artifact, err := openNativePredecessorArtifact(environmentID, current)
	if err != nil {
		return NativeServingRuntime{}, err
	}
	workloads, proxy, err := nativePredecessorServices(artifact, serviceID)
	if err != nil {
		return NativeServingRuntime{}, err
	}
	result := NativeServingRuntime{Artifact: artifact, Workloads: workloads, Proxy: proxy,
		ReleaseID: nativePredecessorReleaseID(workloads[0]), Target: workloads[0].GetSlot()}
	if len(retained) != 0 {
		result.Retained, err = openNativePredecessorArtifact(environmentID, retained)
		if err != nil {
			return NativeServingRuntime{}, err
		}
	}
	if workloads[0].GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA {
		if result.Target == "" {
			result.Target = "singleton"
		}
		return result, nil
	}
	result.Target = "rolling"
	digest := sha256.Sum256(current)
	result.Rolling = &agentpb.RollingReleaseEvidence{ServiceId: serviceID, ReleaseId: result.ReleaseID,
		ArtifactId: artifact.GetArtifactId(), RuntimeSha256: slices.Clone(digest[:]),
		Replicas: uint32(len(workloads)), Compensated: true}
	if proxy != nil {
		generation, err := ProxyConfigGeneration(
			proxy.GetProxyImplementation(),
			proxy.GetProxyConfigJson(),
			result.ReleaseID,
		)
		if err != nil {
			return NativeServingRuntime{}, err
		}
		result.Rolling.ProxyGeneration = generation
		result.Rolling.ProxyConfigSha256 = slices.Clone(proxy.GetProxyConfigSha256())
	}
	return result, nil
}

// RollingCompletion preserves the distinction between forward acknowledgement
// and restoration when machine evidence becomes durable Controller state.
func RollingCompletion(value *agentpb.RollingReleaseEvidence) rollingrelease.Completion {
	return rollingrelease.Completion{ServiceID: value.GetServiceId(), ReleaseID: value.GetReleaseId(),
		ArtifactID: value.GetArtifactId(), RuntimeSHA256: hex.EncodeToString(value.GetRuntimeSha256()),
		Replicas: value.GetReplicas(), ProxyGeneration: value.GetProxyGeneration(),
		ProxyConfigSHA256: hex.EncodeToString(value.GetProxyConfigSha256()), Compensated: value.GetCompensated()}
}
