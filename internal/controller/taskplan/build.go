package taskplan

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// BuildInput is the complete typed output of rendering and procedure
// sequencing. It deliberately contains no persisted rendered-artifact handle:
// a resolver rebuilds these values from retained desired-state inputs.
type BuildInput struct {
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

// Build owns schema selection, defensive copying, deterministic hashing,
// and closed-shape validation for every Controller-produced execution plan.
func Build(input BuildInput) (*agentpb.ExecutionPlan, error) {
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
