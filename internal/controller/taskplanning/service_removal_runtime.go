package taskplanning

import (
	"github.com/AlanD20/groundplane/internal/controller/servicelifecycle"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Preserve each physical member's acknowledged bytes and ownership. The stable
// proxy and the two workload slots can belong to different historical plans.
func serviceRuntimeArtifacts(
	task etcd.TaskRecord,
	serviceID string,
	runtime *serviceruntimerecord.Record,
) ([]*agentpb.ComposeArtifact, error) {
	if runtime == nil {
		return nil, errs.New(errs.KindInternal, "Service acknowledged runtime is missing")
	}
	if err := serviceruntimerecord.Validate(*runtime); err != nil {
		return nil, err
	}
	return servicelifecycle.SplitRuntimeArtifacts(task.ID, task.CreatedAt, serviceID,
		[][]byte{runtime.Runtime.CurrentArtifact, runtime.Runtime.RetainedPriorArtifact})
}

func (resolver *TaskPlanResolver) buildAcknowledgedServiceRemovalPlan(
	task etcd.TaskRecord,
	intent environmentchanges.ServiceRemovalIntent,
) (*agentpb.ExecutionPlan, error) {
	artifacts, err := serviceRuntimeArtifacts(task, intent.ServiceID, intent.AcknowledgedRuntime)
	if err != nil {
		return nil, err
	}
	forwardCount := len(task.Steps)
	if intent.Authored != nil {
		forwardCount -= 2
	}
	if len(artifacts) != forwardCount {
		return nil, errs.New(errs.KindInternal, "Service removal runtime procedure changed")
	}
	sources := make([]*agentpb.ServiceLifecycleSource, len(artifacts))
	steps := make([]*agentpb.ExecutionStep, len(artifacts))
	for index, artifact := range artifacts {
		source, sourceErr := servicelifecycle.RuntimeSource(artifact, task.Steps[index].ID)
		if sourceErr != nil {
			return nil, sourceErr
		}
		sources[index] = source
		steps[index] = &agentpb.ExecutionStep{
			StepId: source.StepId, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
				ArtifactId: artifact.ArtifactId, ServiceIds: []string{intent.ServiceID},
			}},
		}
		if intent.Authored != nil {
			steps[index].Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD
		}
		if index > 0 {
			steps[index].PrerequisiteStepId = steps[index-1].StepId
		}
	}
	operation := agentpb.PlanOperation_PLAN_OPERATION_REMOVE
	var procedure *agentpb.CandidateReleaseProcedure
	if intent.Authored != nil {
		operation = agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
		recovery, err := environmentchanges.ResolveAuthoredServiceRemovalRecovery(intent)
		if err != nil {
			return nil, err
		}
		probeID, compensateID := task.Steps[forwardCount].ID, task.Steps[forwardCount+1].ID
		forwardIDs := make([]string, len(steps))
		for index := range steps {
			forwardIDs[index] = steps[index].GetStepId()
		}
		procedure, err = executionplan.BuildCandidateReleaseProcedure(executionplan.CandidateReleaseProcedureInput{
			Operation: operation,
			Members: []executionplan.CandidateReleaseMemberInput{{
				ServiceID: intent.ServiceID, CandidateReleaseID: recovery.CandidateReleaseID,
				CandidateArtifactID: recovery.CandidateArtifactID, ForwardStepIDs: forwardIDs,
				ServingPredecessor: &executionplan.ServingPredecessorInput{
					ProbeStepID: probeID, CompensateStepID: compensateID,
					PriorArtifactID: recovery.PriorArtifactID, PriorReleaseID: recovery.PriorReleaseID,
					PriorTarget: recovery.PriorTarget, RetainedPriorArtifactID: recovery.RetainedPriorArtifactID,
				},
				CandidateAbsence: &executionplan.CandidateAbsenceInput{
					ComposeProjectName: recovery.ComposeProjectName,
					Services: []executionplan.CandidateServiceIdentity{{
						ServiceID: intent.ServiceID, ReleaseID: recovery.CandidateReleaseID,
					}},
					ProbeStepID: probeID, CompensateStepID: compensateID,
				},
			}},
		})
		if err != nil {
			return nil, err
		}
		steps = append(steps,
			&agentpb.ExecutionStep{
				StepId: probeID, TimeoutSeconds: uint32(task.TimeoutSeconds),
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
				Payload: &agentpb.ExecutionStep_CandidateRestorationProbe{
					CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
						CandidateArtifactId: recovery.CandidateArtifactID, ServiceId: intent.ServiceID,
						CandidateReleaseId: recovery.CandidateReleaseID,
					},
				},
			},
			&agentpb.ExecutionStep{
				StepId: compensateID, TimeoutSeconds: uint32(task.TimeoutSeconds),
				Policy:             agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
				PrerequisiteStepId: forwardIDs[len(forwardIDs)-1],
				Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
					CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
						CandidateArtifactId: recovery.CandidateArtifactID, ServiceId: intent.ServiceID,
						CandidateReleaseId: recovery.CandidateReleaseID,
					},
				},
			},
		)
		artifacts = append(artifacts, recovery.PriorArtifacts...)
	}
	return taskplan.Build(taskplan.BuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration),
		Operation: operation, TargetID: intent.ServiceID,
		Artifacts: artifacts, Steps: steps, ServiceLifecycleProcedure: &agentpb.ServiceLifecycleProcedure{Sources: sources},
		CandidateReleaseProcedure: procedure,
	})
}
