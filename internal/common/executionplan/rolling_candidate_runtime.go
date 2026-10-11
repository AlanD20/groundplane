package executionplan

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func ValidateRollingCompletion(runtime CandidateRuntime, evidence rollingrelease.Completion) error {
	if err := rollingrelease.ValidateCompletion(evidence); err != nil {
		return err
	}
	digest := sha256.Sum256(runtime.CurrentArtifact)
	if evidence.Compensated || runtime.Target != "rolling" || runtime.ServiceID != evidence.ServiceID ||
		runtime.ReleaseID != evidence.ReleaseID ||
		hex.EncodeToString(digest[:]) != evidence.RuntimeSHA256 ||
		runtime.ProxyGeneration != evidence.ProxyGeneration ||
		hex.EncodeToString(runtime.ProxyConfigSHA256) != evidence.ProxyConfigSHA256 ||
		len(runtime.RetainedPriorArtifact) != 0 {
		return errs.New(errs.KindStateConflict, "rolling completion differs from the captured final runtime")
	}
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(runtime.CurrentArtifact, artifact) != nil || artifact.GetArtifactId() != evidence.ArtifactID ||
		ValidateNativePredecessorWitness(
			artifact.GetOwnerId(),
			runtime.ServiceID,
			runtime.CurrentArtifact,
			nil,
		) != nil {
		return errs.New(errs.KindStateConflict, "rolling completion has no matching native runtime artifact")
	}
	workloads, _, err := nativePredecessorServices(artifact, runtime.ServiceID)
	if err != nil || uint32(len(workloads)) != evidence.Replicas ||
		workloads[0].GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA {
		return errs.New(errs.KindStateConflict, "rolling completion replica set differs from the captured runtime")
	}
	return nil
}

// The final rolling projection retains every candidate replica and the exact
// stable proxy that execution reused. It does not retain retired predecessors
// or claim that these prepared bytes have been acknowledged by the Agent.
func prepareRollingCandidateRuntime(member *agentpb.CandidateReleaseMember,
	artifacts map[string]*agentpb.ComposeArtifact,
) (CandidateRuntime, error) {
	plan, err := OpenRollingPlan(member)
	if err != nil {
		return CandidateRuntime{}, err
	}
	source := artifacts[member.GetCandidateArtifactId()]
	if source == nil {
		return CandidateRuntime{}, errs.New(errs.KindValidationFailed, "rolling candidate runtime artifact is missing")
	}
	workloadNames := make([]string, len(plan.Candidate))
	for index, replica := range plan.Candidate {
		if _, err := RollingReplicaService(source, plan.ServiceID, replica); err != nil {
			return CandidateRuntime{}, err
		}
		workloadNames[index] = replica.ComposeName
	}
	if len(plan.Prior) != 0 {
		prior := artifacts[plan.Prior[0].ArtifactID]
		if releaseRuntimeService(
			prior,
			plan.ServiceID,
			"",
			agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
		) != nil {
			source, err = retainCandidateRuntimeProxy(source, prior, plan.ServiceID, workloadNames)
			if err != nil {
				return CandidateRuntime{}, err
			}
		}
	}
	current := proto.CloneOf(source)
	current.Services = nil
	for _, replica := range plan.Candidate {
		workload, err := RollingReplicaService(source, plan.ServiceID, replica)
		if err != nil {
			return CandidateRuntime{}, err
		}
		current.Services = append(current.Services, proto.CloneOf(workload))
	}
	proxy := releaseRuntimeService(
		source,
		plan.ServiceID,
		"",
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
	)
	if (proxy != nil) != member.GetRolling().GetHasProxy() {
		return CandidateRuntime{}, invalidCandidateRuntimeProxySource()
	}
	result := CandidateRuntime{ServiceID: plan.ServiceID, ReleaseID: plan.ReleaseID, Target: "rolling"}
	var sourceProxyConfig []byte
	if proxy != nil {
		last := member.GetRolling().GetChanges()[len(member.GetRolling().GetChanges())-1]
		sourceProxyConfig = slices.Clone(proxy.GetProxyConfigJson())
		proxy = proto.CloneOf(proxy)
		proxy.ProxyConfigJson = slices.Clone(last.GetProxyConfigJson())
		proxy.ProxyConfigSha256 = slices.Clone(last.GetProxyConfigSha256())
		current.Services = append(current.Services, proxy)
		result.ProxyGeneration = last.GetProxyGeneration()
		result.ProxyConfigSHA256 = slices.Clone(last.GetProxyConfigSha256())
	}
	if err := sealCandidateRuntimeProjection(source, current, proxy, sourceProxyConfig); err != nil {
		return CandidateRuntime{}, err
	}
	result.CurrentArtifact, err = marshalCandidateRuntimeArtifact(current)
	if err != nil {
		return CandidateRuntime{}, err
	}
	if err := ValidateNativePredecessorWitness(current.GetOwnerId(), plan.ServiceID, result.CurrentArtifact, nil); err != nil {
		return CandidateRuntime{}, err
	}
	return result, nil
}
