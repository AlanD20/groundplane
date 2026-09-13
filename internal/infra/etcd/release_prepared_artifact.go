package etcd

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func releasePreparedArtifact(evidence ReleasePublicationEvidence) ([]byte, error) {
	plan, err := executionplan.Validate(evidence.Plan)
	if err != nil {
		return nil, err
	}
	if err := executionplan.CandidateReleaseDescriptorMatchesPlan(
		evidence.CandidateReleaseDescriptor,
		plan,
	); err != nil {
		return nil, err
	}
	operation := agentpb.PlanOperation_PLAN_OPERATION_UNSPECIFIED
	switch evidence.Task.Type {
	case TaskDeploy:
		operation = agentpb.PlanOperation_PLAN_OPERATION_DEPLOY
	case TaskRollback:
		operation = agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK
	}
	if evidence.Task.PlanID != plan.GetPlanId() ||
		evidence.Task.PlanHash != hex.EncodeToString(plan.GetPlanHash()) ||
		uint64(evidence.Task.RenderGeneration) != plan.GetRenderGeneration() ||
		evidence.Task.Target != plan.GetTargetId() ||
		operation != plan.GetOperation() ||
		len(evidence.Task.Steps) != len(plan.GetSteps()) {
		return nil, errs.New(errs.KindValidationFailed, "release Task does not match prepared execution plan")
	}
	for index, step := range plan.GetSteps() {
		if step == nil || evidence.Task.Steps[index].ID != step.GetStepId() {
			return nil, errs.New(errs.KindValidationFailed, "release Task steps do not match prepared execution plan")
		}
	}
	artifactID := evidence.Task.Params[TaskComposeArtifactParam]
	for _, artifact := range plan.GetArtifacts() {
		if artifact.GetArtifactId() != artifactID {
			continue
		}
		if artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
			artifact.GetOwnerId() != evidence.EnvironmentID {
			return nil, errs.New(errs.KindValidationFailed, "release prepared artifact owner differs from Task")
		}
		value, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
		if err != nil {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
		return value, nil
	}
	return nil, errs.New(errs.KindValidationFailed, "release prepared artifact is absent from execution plan")
}
