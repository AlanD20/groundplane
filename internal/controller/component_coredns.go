package controller

import (
	"github.com/AlanD20/groundplane/internal/components/coredns"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ValidateCoreDNSComponent keeps the controller capability boundary explicit
// while leaving config semantics in the CoreDNS component package.
func ValidateCoreDNSComponent(component core.Component) error {
	return coredns.ValidateComponent(component)
}

// BuildCoreDNSTaskPlan is the controller entry point for the pure C13 plan
// builder. It has no persistence, Agent, or Docker side effects.
func BuildCoreDNSTaskPlan(component core.Component, environments []core.Environment, baseline []coredns.ResolverEndpoint) (coredns.TaskPlan, error) {
	return coredns.BuildTaskPlan(component, environments, baseline)
}

// CoreDNSApplyInput is the Controller-owned identity/proof bundle used to
// materialize the closed Agent ComponentApply payload.
type CoreDNSApplyInput struct {
	Plan                coredns.TaskPlan
	PlanID              string
	StepID              string
	RenderGeneration    uint64
	DesiredGeneration   uint64
	AgentID             string
	AgentGeneration     uint64
	ImageIndexRef       string
	ImageChildDigest    string
	Platform            string
	CandidateArtifact   *agentpb.ComposeArtifact
	PreviousArtifact    *agentpb.ComposeArtifact
	BaselineGeneration  uint64
	OwnershipGeneration uint64
	Mode                agentpb.CoreDNSApplyMode
	StaticProof         *agentpb.CoreDNSStaticProof
	ForwardProofs       []*agentpb.CoreDNSForwardProof
}

// BuildCoreDNSExecutionPlan creates the same sealed ExecutionPlan used by all
// other Controller operations, with a typed ComponentApply step and no generic
// map payload at the Agent boundary.
func BuildCoreDNSExecutionPlan(input CoreDNSApplyInput) (*ExecutionPlan, error) {
	if err := input.Plan.Validate(); err != nil {
		return nil, err
	}
	payload := &agentpb.CoreDNSConfigApply{
		ComponentId: input.Plan.ComponentID, ServiceId: input.Plan.ServiceID,
		DesiredGeneration: input.DesiredGeneration, RenderGeneration: input.RenderGeneration,
		AgentId: input.AgentID, AgentGeneration: input.AgentGeneration,
		ImageIndexRef: input.ImageIndexRef, ImageChildDigest: input.ImageChildDigest,
		Platform: input.Platform, CorefileSha256: append([]byte(nil), input.Plan.CorefileSHA256[:]...),
		CorefileLength: uint32(len(input.Plan.Corefile)), NormalizedInputSha256: append([]byte(nil), input.Plan.InputSHA256[:]...),
		BaselineGeneration: input.BaselineGeneration, OwnershipGeneration: input.OwnershipGeneration,
		Mode: input.Mode, StaticProof: input.StaticProof, ForwardProofs: input.ForwardProofs,
	}
	if input.CandidateArtifact != nil {
		payload.CandidateArtifactId = input.CandidateArtifact.ArtifactId
		payload.CandidateComposeSha256 = append([]byte(nil), input.CandidateArtifact.YamlSha256...)
	}
	if input.PreviousArtifact != nil {
		payload.PreviousArtifactId = input.PreviousArtifact.ArtifactId
		payload.PreviousComposeSha256 = append([]byte(nil), input.PreviousArtifact.YamlSha256...)
	}
	return BuildPlan(PlanBuildInput{
		PlanID: input.PlanID, RenderGeneration: input.RenderGeneration,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY, TargetID: input.Plan.ComponentID,
		Artifacts: compactCoreDNSArtifacts(input.CandidateArtifact, input.PreviousArtifact),
		Steps: []*agentpb.ExecutionStep{{StepId: input.StepID, TimeoutSeconds: 300,
			Payload: &agentpb.ExecutionStep_ComponentApply{ComponentApply: &agentpb.ComponentApply{
				ComponentPayload: &agentpb.ComponentApply_CorednsConfigApply{CorednsConfigApply: payload},
			}}}},
	})
}

func compactCoreDNSArtifacts(candidate *agentpb.ComposeArtifact, previous *agentpb.ComposeArtifact) []*agentpb.ComposeArtifact {
	if candidate == nil && previous == nil {
		return nil
	}
	if previous == nil || candidate == previous || candidate != nil && candidate.ArtifactId == previous.ArtifactId {
		return []*agentpb.ComposeArtifact{candidate}
	}
	if candidate == nil {
		return []*agentpb.ComposeArtifact{previous}
	}
	return []*agentpb.ComposeArtifact{candidate, previous}
}
