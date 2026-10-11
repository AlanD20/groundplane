package composeruntime

import (
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func capturedRollingContainer(observed *agentpb.ObservedProject, workload *agentpb.ComposeService,
	replica rollingrelease.Replica,
) (*agentpb.ObservedContainer, error) {
	var selected *agentpb.ObservedContainer
	for _, container := range observed.GetContainers() {
		if container.GetContainerId() != replica.ContainerID && container.GetName() != replica.ContainerName {
			continue
		}
		if selected != nil || container.GetContainerId() != replica.ContainerID ||
			container.GetName() != replica.ContainerName ||
			container.GetServiceId() != workload.GetServiceId() ||
			!labelPairsEqual(container.GetLabels(), workload.GetExpectedLabels()) ||
			container.GetImageId() != workload.GetImageReference() ||
			container.GetImageReference() != workload.GetImageReference() {
			return nil, errs.New(
				errs.KindStateConflict,
				"captured rolling predecessor container identity or image changed",
			)
		}
		selected = container
	}
	return selected, nil
}

func evaluateRollingReplicaConvergence(artifact *agentpb.ComposeArtifact, observed *agentpb.ObservedProject,
	workload *agentpb.ComposeService, replica rollingrelease.Replica,
) (composeConvergence, error) {
	if replica.ContainerID == "" {
		return evaluatePhysicalWorkloadConvergence(artifact, observed, workload)
	}
	if err := selectedReplicaCollisions(observed, workload); err != nil {
		return composeConvergence{}, err
	}
	container, err := capturedRollingContainer(observed, workload, replica)
	if err != nil {
		return composeConvergence{}, err
	}
	if container == nil {
		return composeConvergence{}, errs.New(
			errs.KindStateConflict,
			"captured rolling predecessor container is missing",
		)
	}
	// Scope only the readiness calculation. The executable artifact and sealed
	// replica count are never rewritten or substituted as mutation authority.
	scopedArtifact := proto.CloneOf(artifact)
	scopedWorkload := proto.CloneOf(workload)
	scopedWorkload.ExpectedReplicas = 1
	scopedArtifact.Services = []*agentpb.ComposeService{scopedWorkload}
	scopedObservation := proto.CloneOf(observed)
	scopedObservation.Containers = []*agentpb.ObservedContainer{container}
	return evaluatePhysicalWorkloadConvergence(scopedArtifact, scopedObservation, scopedWorkload)
}
