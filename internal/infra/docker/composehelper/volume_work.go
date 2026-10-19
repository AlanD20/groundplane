package composehelper

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type VolumeDockerObservation struct {
	Ready             bool
	Absent            bool
	ConsumersDetached bool
}

// ApplyVolumeDocker performs only the selected Docker Volume operation. The
// caller must hold the acknowledged checkpoint and host execution lease. It
// never creates/removes directories or selects other Compose resources.
func ApplyVolumeDocker(ctx context.Context, taskRunner runner.Runner, input *agentpb.VolumeWorkInput) error {
	artifact, volume, err := capturedVolume(input)
	if err != nil {
		return err
	}
	if ctx == nil || taskRunner == nil {
		return errs.New(errs.KindInternal, "Volume Docker execution is not configured")
	}
	if _, bounded := ctx.Deadline(); !bounded {
		return errs.New(errs.KindValidationFailed, "Volume Docker execution requires its attempt deadline")
	}
	var response *agentpb.ComposeHelperResponse
	if input.Action == agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE {
		response, err = removeCapturedVolume(ctx, taskRunner, artifact, volume)
	} else {
		response, err = prepareCapturedVolume(ctx, taskRunner, artifact, input.VolumeId,
			input.Action == agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_VERIFY)
	}
	if err != nil {
		return err
	}
	if response == nil || response.ExitCode != 0 {
		return errs.New(errs.KindRequestFailed, "Docker could not complete the captured Volume operation")
	}
	return nil
}

// ObserveVolumeDocker uses a successful explicit inventory to prove absence.
// An inspect error is never interpreted as an absent Volume. Removal also checks
// stopped containers and direct bind mounts before reporting detachment.
func ObserveVolumeDocker(
	ctx context.Context,
	taskRunner runner.Runner,
	input *agentpb.VolumeWorkInput,
) (VolumeDockerObservation, error) {
	artifact, volume, err := capturedVolume(input)
	if err != nil {
		return VolumeDockerObservation{}, err
	}
	if ctx == nil || taskRunner == nil {
		return VolumeDockerObservation{}, errs.New(errs.KindInternal, "Volume Docker observation is not configured")
	}
	var observation VolumeDockerObservation
	if input.Action == agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE {
		detached, err := volumeConsumersDetached(ctx, taskRunner, artifact, volume)
		if err != nil {
			return VolumeDockerObservation{}, err
		}
		observation.ConsumersDetached = detached
	}
	exists, err := managedVolumeExists(ctx, taskRunner, volume.DockerName)
	if err != nil {
		return VolumeDockerObservation{}, err
	}
	if !exists {
		observation.Absent = true
		return observation, nil
	}
	inspection, present, failure, err := inspectManagedComposeVolume(ctx, taskRunner, volume.DockerName)
	if err != nil {
		return VolumeDockerObservation{}, err
	}
	if failure != nil || !present || !managedVolumeMatches(inspection, volume,
		artifact.AuthorizedVolumeDir+"/"+volume.ComposeName, artifact.ProjectName) {
		return VolumeDockerObservation{}, errs.New(
			errs.KindStateConflict,
			"Volume Docker ownership or bind source differs from captured input",
		)
	}
	observation.Ready = true
	return observation, nil
}

func capturedVolume(input *agentpb.VolumeWorkInput) (*agentpb.ComposeArtifact, *agentpb.ComposeVolume, error) {
	if err := executionplan.ValidateVolumeWorkInput(input); err != nil {
		return nil, nil, err
	}
	artifact := input.Candidate
	if input.Action == agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE {
		artifact = input.Baseline
	}
	for _, volume := range artifact.Volumes {
		if volume.VolumeId == input.VolumeId {
			return artifact, volume, nil
		}
	}
	return nil, nil, errs.New(errs.KindValidationFailed, "Volume is absent from captured execution input")
}
