package controller

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	EnvironmentCreateVolumeDirectoryParam   = taskcontract.EnvironmentCreateVolumeDirectoryParam
	EnvironmentBlueprintArtifactParam       = taskcontract.EnvironmentBlueprintArtifactParam
	EnvironmentBlueprintManagedVolumesParam = "managed_volume_ids"
	EnvironmentRemoveVolumeDirectoryParam   = taskcontract.EnvironmentRemoveVolumeDirectoryParam
)

// ExecutionPlan is the rendered operation and procedure sequence ready
// for dispatch. Both sides of the Agent channel carry plan_id and
// plan_hash; the Agent rejects a bundle whose two projections don't
// match (blueprint.md, "x-gp-execution").
type ExecutionPlan = agentpb.ExecutionPlan

// PlanBuildInput is the complete typed output of rendering and procedure
// sequencing. It deliberately contains no persisted rendered-artifact handle:
// a resolver rebuilds these values from retained desired-state inputs.
type PlanBuildInput struct {
	VolumeRoot                   string
	PlanID                       string
	RenderGeneration             uint64
	Operation                    agentpb.PlanOperation
	TargetID                     string
	Artifacts                    []*agentpb.ComposeArtifact
	ScriptRunnerSnapshots        []*agentpb.ResolvedRunnerSnapshot
	ScriptRunnerProjections      []*agentpb.ScriptRunnerProjection
	ScriptBodyArtifacts          []*agentpb.ScriptBodyArtifactMetadata
	Steps                        []*agentpb.ExecutionStep
	ComponentLifecycleMode       agentpb.ComponentLifecycleMode
	ComponentRollbackObservation *agentpb.ComponentApply
	CandidateReleaseProcedure    *agentpb.CandidateReleaseProcedure
	ManagedComponentProcedure    *agentpb.ManagedComponentProcedure
	ServiceLifecycleProcedure    *agentpb.ServiceLifecycleProcedure
	EntryMutationProcedure       *agentpb.EntryMutationProcedure
}

// BuildPlan owns schema selection, defensive copying, deterministic hashing,
// and closed-shape validation for every Controller-produced execution plan.
func BuildPlan(input PlanBuildInput) (*ExecutionPlan, error) {
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: 1, PlanId: input.PlanID, RenderGeneration: input.RenderGeneration,
		Operation:                    input.Operation,
		TargetId:                     input.TargetID,
		Artifacts:                    input.Artifacts,
		ScriptRunnerSnapshots:        input.ScriptRunnerSnapshots,
		ScriptRunnerProjections:      input.ScriptRunnerProjections,
		ScriptBodyArtifacts:          input.ScriptBodyArtifacts,
		Steps:                        input.Steps,
		ComponentLifecycleMode:       input.ComponentLifecycleMode,
		ComponentRollbackObservation: input.ComponentRollbackObservation,
		CandidateReleaseProcedure:    input.CandidateReleaseProcedure,
		ManagedComponentProcedure:    input.ManagedComponentProcedure,
		ServiceLifecycleProcedure:    input.ServiceLifecycleProcedure,
		EntryMutationProcedure:       input.EntryMutationProcedure,
	})
	if err != nil {
		return nil, err
	}
	if err := executionplan.AuthorizeVolumeDirectories(plan, input.VolumeRoot); err != nil {
		return nil, err
	}
	return plan, nil
}
