package taskplanning

import (
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"math"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type RollingForwardPlanInput struct {
	ProxyImplementation serviceproxy.Implementation
	Plan                rollingrelease.Plan
	Artifacts           []*agentpb.ComposeArtifact
	StepIDs             []string
	PrerequisiteStepID  string
	ProxyPorts          []domain.ProxyPort
	ProxyGeneration     uint64
}

type RollingForwardPlan struct {
	Procedure *agentpb.RollingReleaseProcedure
	Steps     []*agentpb.ExecutionStep
}

// BuildRollingForwardPlan supplies the same sealed forward procedure to Deploy,
// Blueprint and rollback. Recovery anchors and final publication remain with the
// enclosing Release member; this compiler never publishes or acknowledges it.
func BuildRollingForwardPlan(input RollingForwardPlanInput) (RollingForwardPlan, error) {
	changes, err := input.Plan.Changes()
	if err != nil {
		return RollingForwardPlan{}, err
	}
	count := 1 // Final whole-Release acknowledgement, after all retirements.
	for _, change := range changes {
		count += len(change.Phases())
	}
	if len(input.StepIDs) != count {
		return RollingForwardPlan{}, errs.New(
			errs.KindValidationFailed,
			"rolling Task steps do not cover the captured replica procedure",
		)
	}
	hasProxy := len(input.ProxyPorts) != 0
	if hasProxy != (input.ProxyImplementation != "") {
		return RollingForwardPlan{}, errs.New(
			errs.KindValidationFailed,
			"rolling proxy implementation does not match its captured listeners",
		)
	}
	if hasProxy && (input.ProxyGeneration == 0 || input.ProxyGeneration > math.MaxUint64-uint64(len(changes)-1)) ||
		!hasProxy && input.ProxyGeneration != 0 {
		return RollingForwardPlan{}, errs.New(errs.KindValidationFailed, "rolling proxy generation range is invalid")
	}
	procedure := &agentpb.RollingReleaseProcedure{ReplicaTimeoutSeconds: input.Plan.ReplicaTimeoutSeconds,
		Prior: sealRollingReplicas(input.Plan.Prior), Candidate: sealRollingReplicas(input.Plan.Candidate),
		Retained: sealRollingReplicas(input.Plan.Retained),
		HasProxy: hasProxy, AcknowledgeStepId: input.StepIDs[len(input.StepIDs)-1]}
	if hasProxy {
		procedure.ProxyImplementation, err = executionplan.SealProxyImplementation(input.ProxyImplementation)
		if err != nil {
			return RollingForwardPlan{}, err
		}
	}
	cursor := 0
	for index, change := range changes {
		phaseCount := len(change.Phases())
		captured := &agentpb.RollingReplicaChange{Ordinal: change.Ordinal,
			StepIds: slices.Clone(input.StepIDs[cursor : cursor+phaseCount])}
		cursor += phaseCount
		if hasProxy {
			captured.ProxyGeneration = input.ProxyGeneration + uint64(index)
			config, err := domain.RenderRollingProxyConfig(
				input.ProxyImplementation,
				input.Plan,
				change.Ordinal,
				captured.ProxyGeneration,
				input.ProxyPorts,
			)
			if err != nil {
				return RollingForwardPlan{}, err
			}
			captured.ProxyConfigJson = config.JSON
			captured.ProxyConfigSha256 = slices.Clone(config.SHA256[:])
		}
		procedure.Changes = append(procedure.Changes, captured)
	}
	member := &agentpb.CandidateReleaseMember{ServiceId: input.Plan.ServiceID, CandidateReleaseId: input.Plan.ReleaseID,
		CandidateArtifactId: input.Plan.Candidate[0].ArtifactID, ForwardStepIds: slices.Clone(input.StepIDs), Rolling: procedure}
	steps, err := executionplan.RollingForwardSteps(member, input.Artifacts, input.PrerequisiteStepID)
	if err != nil {
		return RollingForwardPlan{}, err
	}
	return RollingForwardPlan{Procedure: procedure, Steps: steps}, nil
}

func sealRollingReplicas(values []rollingrelease.Replica) []*agentpb.RollingReplica {
	result := make([]*agentpb.RollingReplica, len(values))
	for index, value := range values {
		result[index] = &agentpb.RollingReplica{ReleaseId: value.ReleaseID, ArtifactId: value.ArtifactID,
			ComposeName: value.ComposeName, Ordinal: value.Ordinal,
			ContainerId: value.ContainerID, ContainerName: value.ContainerName, ComposeReplica: value.ComposeReplica}
	}
	return result
}
