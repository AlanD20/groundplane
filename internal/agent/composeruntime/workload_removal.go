package composeruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func evaluatePhysicalWorkloadConvergence(artifact *agentpb.ComposeArtifact, observed *agentpb.ObservedProject,
	workload *agentpb.ComposeService,
) (composeConvergence, error) {
	if err := selectedReplicaCollisions(observed, workload); err != nil {
		return composeConvergence{}, err
	}
	scoped := proto.CloneOf(observed)
	scoped.Collisions = nil
	return evaluateSelectedComposeConvergence(artifact, scoped, []string{workload.GetComposeName()}, true)
}

// Other named containers can belong to an earlier replica in the same rolling
// Service. A collision on this exact replica, or unattributable evidence, cannot
// be ignored when proving health or absence.
func selectedReplicaCollisions(observed *agentpb.ObservedProject, workload *agentpb.ComposeService) error {
	if observed == nil || workload == nil {
		return errs.New(errs.KindStateConflict, "replica runtime observation is unavailable")
	}
	for _, collision := range observed.GetCollisions() {
		if collision.GetKind() != agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER ||
			collision.GetComposeServiceName() == "" || collision.GetComposeServiceName() == workload.GetComposeName() {
			return errs.New(errs.KindStateConflict, "replica runtime ownership is inconsistent")
		}
	}
	return nil
}

func (runtime *Runtime) removeWorkload(ctx context.Context, assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep, remove *agentpb.ComposeWorkloadRemove,
) (StepResult, error) {
	artifact := taskassignment.ComposeArtifact(assignment.Plan, remove.GetArtifactId())
	workload, err := executionplan.ReleaseWorkloadService(
		artifact,
		remove.GetServiceId(),
		remove.GetTarget(),
		remove.GetRollingReplicaOrdinal(),
	)
	if err != nil {
		return StepResult{}, err
	}
	replica, err := executionplan.RollingRetirement(assignment.Plan, step)
	if err != nil {
		return StepResult{}, err
	}
	return runtime.mutate(
		ctx,
		assignment,
		step,
		artifact.GetArtifactId(),
		func(observed *agentpb.ObservedProject) error {
			if observed == nil || observed.GetProjectName() != artifact.GetProjectName() {
				return errs.New(errs.KindStateConflict, "replica removal has no matching Compose observation")
			}
			if err := selectedReplicaCollisions(observed, workload); err != nil {
				return err
			}
			if replica.ContainerID != "" {
				container, err := capturedRollingContainer(observed, workload, replica)
				if err != nil {
					return err
				}
				if container != nil {
					return errs.New(errs.KindStateConflict, "captured predecessor container still exists after removal")
				}
				return nil
			}
			if len(containersForService(observed.GetContainers(), workload)) != 0 {
				return errs.New(errs.KindStateConflict, "the selected workload replica still exists after removal")
			}
			return nil
		},
	)
}
