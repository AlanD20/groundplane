package taskplanning

import (
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type releaseMemberProcedure struct {
	Steps   []*agentpb.ExecutionStep
	Rolling *agentpb.RollingReleaseProcedure
}

// Both public preparation paths enter here. Single-target strategies retain
// their three forward actions; rolling compiles every captured replica phase.
func buildReleaseMemberProcedure(task etcd.TaskRecord, input etcd.ReleaseTaskRenderInput,
	member releaserender.ReleaseTaskRenderMember, layout ReleaseMemberStepIDs,
	artifacts []*agentpb.ComposeArtifact, priorArtifacts map[string]*agentpb.ComposeArtifact,
) (releaseMemberProcedure, error) {
	if err := layout.validate(member.Render); err != nil {
		return releaseMemberProcedure{}, err
	}
	if member.Render.Strategy != domain.StrategyRolling {
		task.Steps = layout.Records()
		input.Members = []releaserender.ReleaseTaskRenderMember{member}
		steps, err := buildSingleTargetReleaseSteps(task, input, priorArtifacts)
		return releaseMemberProcedure{Steps: steps}, err
	}
	plan := *member.Render.Rolling
	changes, err := plan.Changes()
	if err != nil {
		return releaseMemberProcedure{}, err
	}
	firstGeneration := uint64(0)
	if len(member.Render.ProxyPorts) != 0 {
		if member.Render.ProxyGeneration < uint64(len(changes)) {
			return releaseMemberProcedure{}, errs.New(
				errs.KindValidationFailed,
				"rolling final proxy generation does not cover every change",
			)
		}
		firstGeneration = member.Render.ProxyGeneration - uint64(len(changes)-1)
	}
	forward, err := BuildRollingForwardPlan(RollingForwardPlanInput{Plan: plan, Artifacts: artifacts,
		ProxyImplementation: member.Render.ProxyImplementation,
		StepIDs:             layout.Forward, ProxyPorts: member.Render.ProxyPorts, ProxyGeneration: firstGeneration})
	if err != nil {
		return releaseMemberProcedure{}, err
	}
	steps := append(forward.Steps, candidateRestorationSteps(member, layout, uint32(task.TimeoutSeconds))...)
	bindRetainedOrdinaryRecovery(member, steps)
	return releaseMemberProcedure{Steps: steps, Rolling: forward.Procedure}, nil
}

func candidateRestorationSteps(
	member releaserender.ReleaseTaskRenderMember,
	layout ReleaseMemberStepIDs,
	timeout uint32,
) []*agentpb.ExecutionStep {
	return []*agentpb.ExecutionStep{
		{StepId: layout.Probe, TimeoutSeconds: timeout,
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
			Payload: &agentpb.ExecutionStep_CandidateRestorationProbe{
				CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
					CandidateArtifactId: member.Render.ArtifactID, ServiceId: member.Render.ServiceID, CandidateReleaseId: member.Intent.ID,
				},
			}},
		{StepId: layout.Compensate, TimeoutSeconds: timeout, PrerequisiteStepId: layout.Forward[0],
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
			Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
				CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
					CandidateArtifactId: member.Render.ArtifactID, ServiceId: member.Render.ServiceID, CandidateReleaseId: member.Intent.ID,
				},
			}},
	}
}
