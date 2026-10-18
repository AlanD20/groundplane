package composehelper

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/backingruntimefact"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/environmentdirectoryhelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ExecuteBackingProvision owns directory and captured singleton-runtime stages.
// File delivery and the separately journalled Custom hook stay with the Agent.
func ExecuteBackingProvision(ctx context.Context, taskRunner runner.Runner, volumeRoot string,
	request *agentpb.BackingProvisionHelperRequest,
) (*agentpb.BackingProvisionHelperResponse, error) {
	input, err := backingprovision.OpenHelperRequest(request)
	if err != nil {
		return nil, err
	}
	if ctx == nil || taskRunner == nil || input.Assignment.Procedure.VolumeRoot != volumeRoot || input.Progress == nil {
		return nil, errs.New(errs.KindValidationFailed, "backing creation helper has no matching host root and forward stage")
	}
	bounded, cancel := context.WithDeadline(ctx, input.Assignment.Attempt.Deadline)
	if input.InspectOnly {
		cancel()
		bounded, cancel = context.WithTimeout(ctx, 15*time.Second)
	}
	defer cancel()
	var observation backingprovision.Observation
	var runtime *backingruntimefact.Observation
	if input.Progress.Stage == backingprovision.PrepareDirectories {
		observation, err = provisionBackingDirectories(bounded, input)
	} else {
		observation, runtime, err = provisionBackingRuntime(bounded, taskRunner, input)
	}
	if err != nil {
		return nil, err
	}
	return backingprovision.HelperObservation(input, observation, runtime)
}

func provisionBackingDirectories(ctx context.Context, input backingprovision.HelperInput) (backingprovision.Observation, error) {
	procedure, attempt := input.Assignment.Procedure, input.Assignment.Attempt
	directories := environmentdirectoryhelper.Creator{}
	if !input.InspectOnly {
		if err := directories.Create(ctx, procedure.VolumeRoot, procedure.VolumeDirectory); err != nil {
			return backingprovision.Observation{}, err
		}
		if input.Layout.ManagedVolume {
			digest, err := hex.DecodeString(attempt.Input.SHA256)
			if err != nil {
				return backingprovision.Observation{}, err
			}
			volume := procedure.Runtime.Volumes[0]
			if err := directories.EnsureManagedVolumes(ctx, environmentdirectoryhelper.ManagedVolumeEnsureRequest{
				TaskID: attempt.TaskID, OperationID: procedure.OperationId, IntentSHA256: digest,
				VolumeRoot: procedure.VolumeRoot, VolumeDir: procedure.VolumeDirectory,
				Volumes: []environmentdirectoryhelper.ManagedVolume{{ID: volume.VolumeId, Key: volume.ComposeName}},
			}); err != nil {
				return backingprovision.Observation{}, err
			}
		}
	}
	absent, err := directories.Absent(ctx, procedure.VolumeRoot, procedure.VolumeDirectory)
	if err != nil {
		return backingprovision.Observation{}, err
	}
	ready := !absent
	if ready && input.Layout.ManagedVolume {
		volume := procedure.Runtime.Volumes[0]
		observed, err := directories.InspectManagedVolume(ctx, environmentdirectoryhelper.ManagedVolumeInspectRequest{
			VolumeRoot: procedure.VolumeRoot, VolumeDir: procedure.VolumeDirectory, VolumeID: volume.VolumeId, ComposeKey: volume.ComposeName})
		if err != nil {
			return backingprovision.Observation{}, err
		}
		ready = observed.Ready
	}
	observation := backingprovision.Observation{ServiceID: input.Layout.ServiceID, Input: attempt.Input,
		Stage: backingprovision.PrepareDirectories, At: time.Now().UTC()}
	if ready {
		observation.DirectoryReady, observation.VolumeReady = true, input.Layout.ManagedVolume
	}
	return observation, nil
}

func provisionBackingRuntime(ctx context.Context, taskRunner runner.Runner, input backingprovision.HelperInput) (backingprovision.Observation, *backingruntimefact.Observation, error) {
	stage := input.Progress.Stage
	if stage != backingprovision.ReconcileRuntime && stage != backingprovision.WaitReady && stage != backingprovision.VerifyRuntime {
		return backingprovision.Observation{}, nil, errs.New(errs.KindValidationFailed, "backing creation stage is not a Docker runtime operation")
	}
	observation, runtime, err := observeBackingProvision(ctx, taskRunner, input)
	if err != nil || input.InspectOnly || observation.Satisfies(stage, input.Layout) {
		return observation, runtime, err
	}
	if stage == backingprovision.ReconcileRuntime {
		// observeBackingProvision has checked every extant member against
		// captured labels and exact image before Compose may change anything.
		if err := applySingletonArtifact(ctx, taskRunner, input.Assignment.Procedure.Runtime); err != nil {
			return backingprovision.Observation{}, nil, err
		}
		return observeBackingProvision(ctx, taskRunner, input)
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return backingprovision.Observation{}, nil, ctx.Err()
		case <-ticker.C:
			observation, runtime, err = observeBackingProvision(ctx, taskRunner, input)
			if err != nil || observation.Satisfies(stage, input.Layout) {
				return observation, runtime, err
			}
		}
	}
}
