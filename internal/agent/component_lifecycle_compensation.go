package agent

import (
	"context"
	"errors"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"time"
)

func (p *WorkerPool) compensateComponentLifecycle(
	assignment taskassignment.Assignment,
	managedConfigStep *agentpb.ExecutionStep,
	attemptedSteps []*agentpb.ExecutionStep,
) (*agentpb.DNSResolverObservationEvidence, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	attempted := make(map[string]bool, len(attemptedSteps))
	for _, step := range attemptedSteps {
		attempted[step.GetStepId()] = true
	}
	var compensationErr error
	mode := assignment.Plan.GetComponentLifecycleMode()
	if mode == agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_ENABLE {
		for index := len(assignment.Plan.Steps) - 1; index >= 0; index-- {
			step := assignment.Plan.Steps[index]
			switch {
			case step.GetHostResolutionApply() != nil && attempted[step.GetStepId()]:
				apply := step.GetHostResolutionApply()
				compensationErr = errors.Join(
					compensationErr,
					p.executeLifecycleCompensation(ctx, assignment, &agentpb.ExecutionStep{
						StepId:         step.GetStepId(),
						TimeoutSeconds: 30,
						Payload: &agentpb.ExecutionStep_HostResolutionRestore{
							HostResolutionRestore: &agentpb.HostResolutionRestore{
								ComponentId: apply.GetComponentId(),
								Generation:  apply.GetGeneration(),
							},
						},
					}),
				)
			case step.GetComposeApply() != nil && attempted[step.GetStepId()]:
				apply := step.GetComposeApply()
				compensationAssignment, compensationStep, deriveErr := componentLifecycleComposeAssignment(
					assignment,
					&agentpb.ExecutionStep{
						StepId:         ids.New(ids.KindStep),
						TimeoutSeconds: 30,
						Payload: &agentpb.ExecutionStep_ComposeRemove{
							ComposeRemove: &agentpb.ComposeRemove{
								ArtifactId: apply.GetArtifactId(),
								ServiceIds: append([]string(nil), apply.GetServiceIds()...),
							},
						},
					},
				)
				if deriveErr == nil {
					deriveErr = p.executeLifecycleCompensation(ctx, compensationAssignment, compensationStep)
				}
				compensationErr = errors.Join(
					compensationErr,
					deriveErr,
				)
			}
		}
	}
	if managedConfigStep != nil {
		rollbackState, rollbackErr := p.componentActions.FinalizeManagedConfig(
			ctx,
			assignment,
			managedConfigStep,
			false,
		)
		if rollbackErr == nil && !managedConfigRollbackProven(managedConfigStep, rollbackState) {
			rollbackErr = errs.New(errs.KindInternal, "agent: managed-config rollback result is invalid")
		}
		var serviceRollbackErr error
		if rollbackErr == nil && mode == agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UPDATE {
			if apply := attemptedComponentComposeApply(assignment.Plan, attempted); apply != nil {
				rollbackArtifact := componentRollbackComposeArtifact(assignment.Plan, apply.GetArtifactId())
				if rollbackArtifact == nil {
					serviceRollbackErr = errs.New(errs.KindInternal, "agent: Component rollback artifact is missing")
				} else {
					rollbackAssignment, rollbackStep, deriveErr := componentRollbackComposeAssignment(
						assignment,
						apply,
						rollbackArtifact,
					)
					if deriveErr != nil {
						serviceRollbackErr = deriveErr
					} else {
						serviceRollbackErr = p.executeLifecycleCompensation(ctx, rollbackAssignment, rollbackStep)
					}
				}
			}
		}
		compensationErr = errors.Join(compensationErr, rollbackErr, serviceRollbackErr)
		if rollbackErr == nil && serviceRollbackErr == nil &&
			mode != agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_ENABLE &&
			len(managedConfigStep.GetComponentApply().GetExpectedPreviousArtifactDigest()) != 0 {
			rollbackObservation, observationErr := p.observeManagedConfigRollback(
				ctx, assignment, managedConfigStep,
			)
			if observationErr != nil {
				compensationErr = errors.Join(compensationErr, observationErr)
			} else if compensationErr == nil {
				return rollbackObservation, nil
			}
		}
	}
	if mode == agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_DISABLE {
		mutationAttempted := false
		for _, step := range assignment.Plan.Steps {
			if remove := step.GetComposeRemove(); remove != nil && attempted[step.GetStepId()] {
				mutationAttempted = true
				compensationAssignment, compensationStep, deriveErr := componentLifecycleComposeAssignment(
					assignment,
					&agentpb.ExecutionStep{
						StepId:         ids.New(ids.KindStep),
						TimeoutSeconds: 30,
						Payload: &agentpb.ExecutionStep_ComposeApply{
							ComposeApply: &agentpb.ComposeApply{
								ArtifactId:     remove.GetArtifactId(),
								ServiceIds:     append([]string(nil), remove.GetServiceIds()...),
								ForceRecreate:  true,
								NoDependencies: true,
							},
						},
					},
				)
				if deriveErr == nil {
					deriveErr = p.executeLifecycleCompensation(ctx, compensationAssignment, compensationStep)
				}
				if deriveErr != nil {
					return nil, errors.Join(compensationErr, deriveErr)
				}
			}
		}
		for _, step := range assignment.Plan.Steps {
			if step.GetHostResolutionRestore() != nil && attempted[step.GetStepId()] {
				mutationAttempted = true
			}
		}
		if !mutationAttempted {
			return nil, compensationErr
		}
		rollbackObservation, observationErr := p.observeComponentRollback(ctx, assignment)
		if observationErr != nil {
			return nil, errors.Join(compensationErr, observationErr)
		}
		for _, step := range assignment.Plan.Steps {
			if restore := step.GetHostResolutionRestore(); restore != nil && attempted[step.GetStepId()] {
				if err := p.executeLifecycleCompensation(ctx, assignment, &agentpb.ExecutionStep{
					StepId:         step.GetStepId(),
					TimeoutSeconds: 30,
					Payload: &agentpb.ExecutionStep_HostResolutionApply{
						HostResolutionApply: &agentpb.HostResolutionApply{
							ComponentId: restore.GetComponentId(),
							Generation:  restore.GetGeneration(),
						},
					},
				}); err != nil {
					return nil, errors.Join(compensationErr, err)
				}
			}
		}
		return rollbackObservation, compensationErr
	}
	return nil, compensationErr
}

func (p *WorkerPool) executeLifecycleCompensation(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
) error {
	if step.GetHostResolutionApply() != nil || step.GetHostResolutionRestore() != nil {
		if p.hostResolution == nil {
			return errs.New(errs.KindInternal, "agent: host resolution compensation runtime is not configured")
		}
		return p.hostResolution.ExecuteHostResolution(ctx, assignment, step)
	}
	if p.compose == nil {
		return errs.New(errs.KindInternal, "agent: Compose compensation runtime is not configured")
	}
	_, err := p.compose.ExecuteStep(ctx, assignment, step)
	return err
}
