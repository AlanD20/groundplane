package releaseexecution

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/healthcheckfailure"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Readiness checks captured candidate ownership and health. Both the Agent's
// wait loop and Controller evidence admission use this decision; neither can
// substitute a different Service or an earlier observation from the same host.
func Readiness(input Input, progress Progress, observed *agentpb.ObservedProject) (bool, error) {
	services, err := ReadinessWorkloads(input, progress)
	if err != nil {
		return false, err
	}
	if observed == nil || executionplan.RejectUnknown(observed) != nil || observed.ProjectName != input.Artifacts.Candidate.ProjectName ||
		observed.ObservedAt == nil ||
		observed.ObservedAt.CheckValid() != nil ||
		observed.ObservedAt.AsTime().Before(progress.UpdatedAt) {
		return false, errs.New(
			errs.KindStateConflict,
			"Release readiness requires a fresh observation of its captured project",
		)
	}
	for _, collision := range observed.Collisions {
		if readinessCollision(input, services, progress.Rolling != nil, collision) {
			return false, errs.New(
				errs.KindStateConflict,
				"candidate runtime ownership differs from the captured Release",
			)
		}
	}
	seen := make(map[string]bool, len(observed.Containers))
	for _, container := range observed.Containers {
		if container == nil || container.ContainerId == "" || seen[container.ContainerId] {
			return false, errs.New(
				errs.KindStateConflict,
				"Release observation contains an unidentified or duplicate container",
			)
		}
		seen[container.ContainerId] = true
	}
	for _, service := range services {
		if !service.HasHealthcheck &&
			service.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON {
			return false, errs.Newf(
				errs.KindValidationFailed,
				"service %s requires an explicit healthcheck for this deployment strategy",
				service.ComposeName,
			)
		}
		count, ready := uint32(0), true
		for _, container := range observed.Containers {
			if container.ServiceId != service.ServiceId ||
				!slices.EqualFunc(
					container.Labels,
					service.ExpectedLabels,
					func(a, b *agentpb.LabelPair) bool { return a != nil && b != nil && proto.Equal(a, b) },
				) {
				continue
			}
			if container.ImageReference != service.ImageReference || container.ImageId != service.ImageReference {
				return false, errs.Newf(
					errs.KindStateConflict,
					"service %s is running an image different from the captured Release",
					service.ComposeName,
				)
			}
			count++
			if container.State != agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING {
				ready = false
				continue
			}
			if service.HasHealthcheck {
				if detail := healthcheckfailure.Detail(container.HealthcheckDiagnostic); detail != "" {
					return false, errs.Newf(
						errs.KindValidationFailed,
						"service %s container %s: %s",
						service.ComposeName,
						container.Name,
						detail,
					)
				}
				if container.Health != agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY {
					ready = false
				}
			}
		}
		if !ready || count != service.ExpectedReplicas {
			return false, nil
		}
	}
	return true, nil
}

// ReadinessWorkloads selects exactly one rolling candidate or all nonrolling
// candidate workloads. It never includes the stable proxy or predecessor.
func ReadinessWorkloads(input Input, progress Progress) ([]*agentpb.ComposeService, error) {
	if progress.Validate(input) != nil || progress.Phase != Rollout || !progress.InFlight {
		return nil, invalidProgress()
	}
	if input.Artifacts.Rolling == nil {
		if progress.Action != WaitCandidate {
			return nil, invalidProgress()
		}
		return input.Artifacts.Workloads, nil
	}
	if progress.Rolling == nil || progress.Rolling.Phase != rollingrelease.WaitCandidateHealthy {
		return nil, invalidProgress()
	}
	changes, err := input.Artifacts.Rolling.Changes()
	if err != nil {
		return nil, err
	}
	candidate := changes[progress.Rolling.Change-1].Candidate
	if candidate == nil {
		return nil, invalidProgress()
	}
	for _, service := range input.Artifacts.Workloads {
		if service.ComposeName == candidate.ComposeName {
			return []*agentpb.ComposeService{service}, nil
		}
	}
	return nil, invalidProgress()
}

func readinessCollision(
	input Input,
	services []*agentpb.ComposeService,
	rolling bool,
	collision *agentpb.ObservedCollision,
) bool {
	if collision == nil {
		return true
	}
	switch collision.Kind {
	case agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER:
		if collision.ComposeServiceName == "" {
			return true
		}
		for _, service := range services {
			if collision.ComposeServiceName == service.ComposeName ||
				!rolling && collision.ServiceId == service.ServiceId {
				return true
			}
		}
		return false
	case agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_NETWORK:
		return rolling || collision.Name == "" ||
			slices.ContainsFunc(input.Artifacts.Candidate.Networks, func(network *agentpb.ComposeNetwork) bool {
				return collision.Name == network.ComposeName || collision.Name == network.DockerName
			})
	case agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_VOLUME:
		return rolling || collision.Name == "" ||
			slices.ContainsFunc(input.Artifacts.Candidate.Volumes, func(volume *agentpb.ComposeVolume) bool {
				return collision.Name == volume.ComposeName || collision.Name == volume.DockerName
			})
	default:
		return true
	}
}
