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
	if ctx == nil || taskRunner == nil || input.Assignment.Procedure.VolumeRoot != volumeRoot {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backing creation helper has no matching host root and forward stage",
		)
	}
	bounded, cancel := context.WithDeadline(ctx, input.Assignment.Attempt.Deadline)
	if input.InspectOnly {
		cancel()
		bounded, cancel = context.WithTimeout(ctx, 15*time.Second)
	}
	defer cancel()
	if input.Assignment.Recovery != nil {
		return cleanupBackingProvision(bounded, taskRunner, input)
	}
	if input.Progress == nil {
		return nil, errs.New(errs.KindValidationFailed, "backing creation helper has no active stage")
	}
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

func cleanupBackingProvision(
	ctx context.Context,
	taskRunner runner.Runner,
	input backingprovision.HelperInput,
) (*agentpb.BackingProvisionHelperResponse, error) {
	procedure := input.Assignment.Procedure
	containers, err := inspectCapturedContainers(ctx, taskRunner, procedure.Runtime, procedure.Runtime.Services[0])
	if err != nil {
		return nil, err
	}
	for _, id := range containers {
		// No compose down, force removal, Volume deletion or database command.
		// Container absence also terminates every captured in-container hook.
		for _, args := range [][]string{{"container", "stop", "--time", "30", "--", id}, {"container", "rm", "--", id}} {
			result, err := taskRunner.Run(ctx, runner.RunCmdOpts{Name: DockerExecutable, Args: args, Dir: WorkDirectory,
				Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true, CaptureLimitBytes: 32 * 1024})
			clear(result.Stdout)
			clear(result.Stderr)
			if err != nil || result.ExitCode != 0 {
				return nil, errs.New(
					errs.KindRequestFailed,
					"backing creation cleanup could not stop and remove its captured container",
				)
			}
		}
	}
	remaining, err := inspectCapturedContainers(ctx, taskRunner, procedure.Runtime, procedure.Runtime.Services[0])
	if err != nil {
		return nil, err
	}
	if len(remaining) != 0 {
		return nil, errs.New(errs.KindStateConflict, "backing creation runtime remains after captured cleanup")
	}
	return backingprovision.HelperRecovery(input, time.Now().UTC())
}

func provisionBackingDirectories(
	ctx context.Context,
	input backingprovision.HelperInput,
) (backingprovision.Observation, error) {
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

func provisionBackingRuntime(
	ctx context.Context,
	taskRunner runner.Runner,
	input backingprovision.HelperInput,
) (backingprovision.Observation, *backingruntimefact.Observation, error) {
	stage := input.Progress.Stage
	if stage != backingprovision.ReconcileRuntime && stage != backingprovision.WaitReady &&
		stage != backingprovision.VerifyRuntime {
		return backingprovision.Observation{}, nil, errs.New(
			errs.KindValidationFailed,
			"backing creation stage is not a Docker runtime operation",
		)
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
