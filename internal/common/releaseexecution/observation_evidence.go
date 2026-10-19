package releaseexecution

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ReadinessProof is the authenticated Agent's selected-container observation.
// Captured labels are hashed, not duplicated for every replica. Unrelated
// project resources and diagnostics are not retained in this completion proof.
type ReadinessProof struct {
	ObservedAt time.Time        `json:"observed_at"`
	Containers []ReadyContainer `json:"containers"`
}

type ReadyContainer struct {
	ComposeName    string                          `json:"compose_name"`
	ContainerID    string                          `json:"container_id"`
	LabelsSHA256   string                          `json:"labels_sha256"`
	ImageReference string                          `json:"image_reference"`
	ImageID        string                          `json:"image_id"`
	State          agentpb.ObservedContainerState  `json:"state"`
	Health         agentpb.ObservedContainerHealth `json:"health"`
}

func ReadinessResult(input Input, progress Progress, observed *agentpb.ObservedProject) (EffectResult, error) {
	ready, err := Readiness(input, progress, observed)
	if err != nil {
		return EffectResult{}, err
	}
	if !ready {
		return EffectResult{}, invalidProgress()
	}
	services, err := ReadinessWorkloads(input, progress)
	if err != nil {
		return EffectResult{}, err
	}
	proof := &ReadinessProof{ObservedAt: observed.ObservedAt.AsTime().UTC()}
	for _, service := range services {
		digest, err := readinessLabelsDigest(service.ExpectedLabels)
		if err != nil {
			return EffectResult{}, err
		}
		for _, container := range observed.Containers {
			if container.ServiceId != service.ServiceId ||
				!slices.EqualFunc(
					container.Labels,
					service.ExpectedLabels,
					func(a, b *agentpb.LabelPair) bool { return proto.Equal(a, b) },
				) {
				continue
			}
			proof.Containers = append(
				proof.Containers,
				ReadyContainer{ComposeName: service.ComposeName, ContainerID: container.ContainerId,
					LabelsSHA256: digest, ImageReference: container.ImageReference, ImageID: container.ImageId, State: container.State, Health: container.Health},
			)
		}
	}
	slices.SortFunc(proof.Containers, func(a, b ReadyContainer) int {
		if a.ContainerID < b.ContainerID {
			return -1
		}
		if a.ContainerID > b.ContainerID {
			return 1
		}
		return 0
	})
	return EffectResult{Readiness: proof}, nil
}

func (result EffectResult) validate(input Input, progress Progress, at time.Time) error {
	if progress.Validate(input) != nil || !progress.InFlight ||
		(progress.Phase != Rollout && progress.Phase != Configuration) {
		return invalidProgress()
	}
	count := 0
	if result.Configuration != nil {
		count++
	}
	if len(result.Helper) != 0 {
		count++
	}
	if result.Readiness != nil {
		count++
	}
	if result.DrainedAt != nil {
		count++
	}
	if count != 1 {
		return invalidProgress()
	}
	switch {
	case result.Configuration != nil:
		return result.Configuration.validate(input, progress)
	case len(result.Helper) != 0:
		response := &agentpb.ReleaseHelperResponse{}
		if proto.Unmarshal(result.Helper, response) != nil {
			return invalidProgress()
		}
		return ValidateHelperResult(input, progress, response)
	case result.Readiness != nil:
		return result.Readiness.validate(input, progress, at)
	default:
		cursor := progress.Rolling
		if progress.Phase != Rollout || cursor == nil || cursor.Phase != rollingrelease.DrainPredecessor ||
			result.DrainedAt.IsZero() ||
			result.DrainedAt.After(at) ||
			result.DrainedAt.Before(progress.UpdatedAt) ||
			result.DrainedAt.Before(cursor.DrainUntil) ||
			!result.DrainedAt.Before(cursor.ReplicaDeadline) ||
			!result.DrainedAt.Before(input.Attempt.Deadline) {
			return invalidProgress()
		}
		return nil
	}
}

func (proof ReadinessProof) validate(input Input, progress Progress, at time.Time) error {
	services, err := ReadinessWorkloads(input, progress)
	if err != nil {
		return err
	}
	if proof.ObservedAt.IsZero() || proof.ObservedAt.After(at) || !proof.ObservedAt.Before(input.Attempt.Deadline) ||
		progress.Rolling != nil &&
			!proof.ObservedAt.Before(progress.Rolling.ReplicaDeadline) || len(proof.Containers) == 0 {
		return invalidProgress()
	}
	observed := &agentpb.ObservedProject{
		ProjectName: input.Artifacts.Candidate.ProjectName,
		ObservedAt:  timestamppb.New(proof.ObservedAt),
	}
	for index, container := range proof.Containers {
		if container.ContainerID == "" || len(container.ContainerID) > 128 || len(container.ImageID) > 256 ||
			index > 0 && proof.Containers[index-1].ContainerID >= container.ContainerID {
			return invalidProgress()
		}
		selected := slices.IndexFunc(
			services,
			func(service *agentpb.ComposeService) bool { return service.ComposeName == container.ComposeName },
		)
		if selected < 0 {
			return invalidProgress()
		}
		service := services[selected]
		digest, err := readinessLabelsDigest(service.ExpectedLabels)
		if err != nil || digest != container.LabelsSHA256 {
			return invalidProgress()
		}
		observed.Containers = append(
			observed.Containers,
			&agentpb.ObservedContainer{ContainerId: container.ContainerID, Name: container.ContainerID,
				ServiceId: service.ServiceId, ImageReference: container.ImageReference, ImageId: container.ImageID,
				Labels: service.ExpectedLabels, State: container.State, Health: container.Health},
		)
	}
	ready, err := Readiness(input, progress, observed)
	if err != nil {
		return err
	}
	if !ready {
		return invalidProgress()
	}
	return nil
}

func readinessLabelsDigest(labels []*agentpb.LabelPair) (string, error) {
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeService{ExpectedLabels: labels})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}
