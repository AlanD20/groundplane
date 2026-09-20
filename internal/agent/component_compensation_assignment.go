package agent

import (
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"strconv"
)

func componentLifecycleComposeAssignment(
	assignment taskassignment.Assignment,
	compensationStep *agentpb.ExecutionStep,
) (taskassignment.Assignment, *agentpb.ExecutionStep, error) {
	if assignment.Plan == nil || compensationStep == nil {
		return taskassignment.Assignment{}, nil, errs.New(
			errs.KindInternal,
			"agent: Component lifecycle Compose compensation is invalid",
		)
	}
	artifactID := ""
	switch payload := compensationStep.GetPayload().(type) {
	case *agentpb.ExecutionStep_ComposeApply:
		artifactID = payload.ComposeApply.GetArtifactId()
	case *agentpb.ExecutionStep_ComposeRemove:
		artifactID = payload.ComposeRemove.GetArtifactId()
	default:
		return taskassignment.Assignment{}, nil, errs.New(errs.KindInternal, "agent: Component lifecycle Compose compensation is invalid")
	}
	artifact := composeArtifact(assignment.Plan, artifactID)
	planID, renderGeneration, authorityErr := componentComposeArtifactAuthority(artifact)
	if authorityErr != nil {
		return taskassignment.Assignment{}, nil, authorityErr
	}
	derived := proto.Clone(assignment.Plan).(*agentpb.ExecutionPlan)
	derived.PlanId = planID
	derived.RenderGeneration = renderGeneration
	derived.Operation = agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
	derived.ComponentLifecycleMode = agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UNSPECIFIED
	derived.ComponentRollbackObservation = nil
	derived.Artifacts = []*agentpb.ComposeArtifact{proto.Clone(artifact).(*agentpb.ComposeArtifact)}
	derived.Steps = []*agentpb.ExecutionStep{proto.Clone(compensationStep).(*agentpb.ExecutionStep)}
	derived.ScriptRunnerSnapshots = nil
	derived.ScriptRunnerProjections = nil
	derived.ScriptBodyArtifacts = nil
	derived.PlanHash = nil
	sealed, err := executionplan.Seal(derived)
	if err != nil {
		return taskassignment.Assignment{}, nil, errs.Wrap(errs.KindInternal, err)
	}
	assignment.Plan = sealed
	return assignment, sealed.GetSteps()[0], nil
}

func componentComposeArtifactAuthority(artifact *agentpb.ComposeArtifact) (string, uint64, error) {
	if artifact == nil || len(artifact.GetServices()) != 1 {
		return "", 0, errs.New(errs.KindInternal, "agent: Component lifecycle Compose artifact is invalid")
	}
	planID := ""
	renderGeneration := uint64(0)
	for _, label := range artifact.GetServices()[0].GetExpectedLabels() {
		switch label.GetKey() {
		case "com.groundplane.plan-id":
			planID = label.GetValue()
		case "com.groundplane.render-generation":
			renderGeneration, _ = strconv.ParseUint(label.GetValue(), 10, 64)
		}
	}
	if ids.Validate(ids.KindPlan, planID) != nil || renderGeneration == 0 {
		return "", 0, errs.New(errs.KindInternal, "agent: Component lifecycle Compose artifact authority is invalid")
	}
	return planID, renderGeneration, nil
}

func attemptedComponentComposeApply(
	plan *agentpb.ExecutionPlan,
	attempted map[string]bool,
) *agentpb.ComposeApply {
	for _, step := range plan.GetSteps() {
		if apply := step.GetComposeApply(); apply != nil && attempted[step.GetStepId()] {
			return apply
		}
	}
	return nil
}

func componentRollbackComposeArtifact(
	plan *agentpb.ExecutionPlan,
	candidateArtifactID string,
) *agentpb.ComposeArtifact {
	if plan == nil || len(plan.GetArtifacts()) != 2 {
		return nil
	}
	for _, artifact := range plan.GetArtifacts() {
		if artifact.GetArtifactId() != candidateArtifactID {
			return artifact
		}
	}
	return nil
}

func componentRollbackComposeAssignment(
	assignment taskassignment.Assignment,
	candidate *agentpb.ComposeApply,
	rollbackArtifact *agentpb.ComposeArtifact,
) (taskassignment.Assignment, *agentpb.ExecutionStep, error) {
	if assignment.Plan == nil || candidate == nil || rollbackArtifact == nil ||
		len(rollbackArtifact.GetServices()) != 1 {
		return taskassignment.Assignment{}, nil, errs.New(errs.KindInternal, "agent: Component rollback Compose input is invalid")
	}
	planID := ""
	renderGeneration := uint64(0)
	for _, label := range rollbackArtifact.GetServices()[0].GetExpectedLabels() {
		switch label.GetKey() {
		case "com.groundplane.plan-id":
			planID = label.GetValue()
		case "com.groundplane.render-generation":
			renderGeneration, _ = strconv.ParseUint(label.GetValue(), 10, 64)
		}
	}
	rollbackStep := &agentpb.ExecutionStep{
		StepId: ids.New(ids.KindStep), TimeoutSeconds: 30,
		Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
			ArtifactId:     rollbackArtifact.GetArtifactId(),
			ServiceIds:     append([]string(nil), candidate.GetServiceIds()...),
			ForceRecreate:  true,
			NoDependencies: true,
		}},
	}
	derived := proto.Clone(assignment.Plan).(*agentpb.ExecutionPlan)
	derived.PlanId = planID
	derived.RenderGeneration = renderGeneration
	derived.Operation = agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
	derived.ComponentLifecycleMode = agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UNSPECIFIED
	derived.ComponentRollbackObservation = nil
	derived.Artifacts = []*agentpb.ComposeArtifact{proto.Clone(rollbackArtifact).(*agentpb.ComposeArtifact)}
	derived.Steps = []*agentpb.ExecutionStep{rollbackStep}
	derived.PlanHash = nil
	sealed, err := executionplan.Seal(derived)
	if err != nil {
		return taskassignment.Assignment{}, nil, errs.Wrap(errs.KindInternal, err)
	}
	assignment.Plan = sealed
	return assignment, rollbackStep, nil
}
