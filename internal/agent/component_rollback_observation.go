package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	componentaction "github.com/AlanD20/groundplane/internal/agent/componentaction"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (p *WorkerPool) observeComponentRollback(
	ctx context.Context,
	assignment taskassignment.Assignment,
) (*agentpb.DNSResolverObservationEvidence, error) {
	if assignment.Plan == nil || p.componentActions == nil {
		return nil, errs.New(errs.KindInternal, "agent: Component rollback observation runtime is unavailable")
	}
	action := assignment.Plan.GetComponentRollbackObservation()
	if action == nil || action.GetManagedConfigContent() || len(action.GetArtifactDigest()) != sha256.Size {
		return nil, errs.New(errs.KindInternal, "agent: Component rollback observation action is invalid")
	}
	step := &agentpb.ExecutionStep{
		TimeoutSeconds: 30,
		Payload: &agentpb.ExecutionStep_ComponentApply{
			ComponentApply: proto.Clone(action).(*agentpb.ComponentApply),
		},
	}
	if len(assignment.Plan.GetSteps()) != 0 {
		step.StepId = assignment.Plan.GetSteps()[0].GetStepId()
	}
	result, err := p.componentActions.ExecuteComponentAction(
		ctx,
		assignment,
		step,
		componentaction.ManagedConfigPayload{},
	)
	if err != nil {
		return nil, err
	}
	if result == nil || result.DNSResolverObservation == nil || result.ManagedConfig != nil {
		return nil, errs.New(errs.KindInternal, "agent: Component rollback observation result is invalid")
	}
	return result.DNSResolverObservation, nil
}

func (p *WorkerPool) observeManagedConfigRollback(
	ctx context.Context,
	assignment taskassignment.Assignment,
	managedConfigStep *agentpb.ExecutionStep,
) (*agentpb.DNSResolverObservationEvidence, error) {
	managed := managedConfigStep.GetComponentApply()
	if assignment.Plan == nil || managed == nil ||
		len(managed.GetExpectedPreviousArtifactDigest()) != sha256.Size {
		return nil, errs.New(errs.KindInternal, "agent: managed-config rollback observation input is invalid")
	}
	var observationStep *agentpb.ExecutionStep
	for _, step := range assignment.Plan.GetSteps() {
		action := step.GetComponentApply()
		if action == nil || action.GetManagedConfigContent() ||
			action.GetComponentId() != managed.GetComponentId() || action.GetArtifactId() != managed.GetArtifactId() ||
			action.GetGeneration() != managed.GetGeneration() ||
			!bytes.Equal(action.GetDefinitionDigest(), managed.GetDefinitionDigest()) ||
			!bytes.Equal(action.GetCatalogDigest(), managed.GetCatalogDigest()) {
			continue
		}
		if observationStep != nil {
			return nil, errs.New(errs.KindInternal, "agent: managed-config rollback observation action is ambiguous")
		}
		observationStep = proto.Clone(step).(*agentpb.ExecutionStep)
	}
	if observationStep == nil {
		return nil, errs.New(errs.KindInternal, "agent: managed-config rollback observation action is missing")
	}
	if rollbackArtifact := componentRollbackComposeArtifact(
		assignment.Plan,
		componentComposeApplyArtifactID(assignment.Plan),
	); rollbackArtifact != nil {
		clonedPlan := proto.Clone(assignment.Plan).(*agentpb.ExecutionPlan)
		clonedPlan.Artifacts = []*agentpb.ComposeArtifact{
			proto.Clone(rollbackArtifact).(*agentpb.ComposeArtifact),
		}
		assignment.Plan = clonedPlan
	}
	observationStep.GetComponentApply().ArtifactDigest = append(
		[]byte(nil),
		managed.GetExpectedPreviousArtifactDigest()...,
	)
	observationStep.GetComponentApply().ArtifactId = managed.GetExpectedPreviousArtifactId()
	observationStep.GetComponentApply().Generation = managed.GetExpectedPreviousGeneration()
	result, err := p.componentActions.ExecuteComponentAction(
		ctx,
		assignment,
		observationStep,
		componentaction.ManagedConfigPayload{},
	)
	if err != nil {
		return nil, err
	}
	if result == nil || result.DNSResolverObservation == nil || result.ManagedConfig != nil {
		return nil, errs.New(errs.KindInternal, "agent: managed-config rollback observation result is invalid")
	}
	return result.DNSResolverObservation, nil
}

func componentComposeApplyArtifactID(plan *agentpb.ExecutionPlan) string {
	for _, step := range plan.GetSteps() {
		if apply := step.GetComposeApply(); apply != nil {
			return apply.GetArtifactId()
		}
	}
	return ""
}
