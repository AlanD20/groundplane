package composehelper

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func detachVolumeConsumers(ctx context.Context, taskRunner runner.Runner, input *agentpb.VolumeWorkInput) error {
	if err := executionplan.ValidateVolumeWorkInput(input); err != nil {
		return err
	}
	if input.Action != agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE {
		return errs.New(errs.KindValidationFailed, "Volume consumer detachment requires captured removal input")
	}
	names, err := (volumeexecution.Removal{VolumeID: input.VolumeId, Key: input.ComposeKey,
		Baseline: input.Baseline, Candidate: input.Candidate, ConsumerIDs: input.ConsumerIds}).Consumers()
	if err != nil {
		return err
	}
	selected := make([]string, 0, len(names))
	for _, name := range names {
		containers, err := inspectCapturedWorkloads(ctx, taskRunner, input.Baseline, name)
		if err != nil {
			return err
		}
		if len(containers) != 0 {
			selected = append(selected, name)
		}
	}
	if len(selected) != 0 {
		stdin, err := composeCLIInput(input.Candidate.CanonicalYaml)
		if err != nil {
			return err
		}
		defer clear(stdin)
		prefix := []string{"compose", "--project-name", input.Candidate.ProjectName,
			"--project-directory", composeProjectDirectory(input.Candidate), "--file", "-"}
		commands := [][]string{
			append(slices.Clone(prefix), "config", "--quiet", "--no-interpolate"),
			append(append(slices.Clone(prefix), "up", "--detach", "--no-deps", "--no-build", "--pull", "never"), selected...),
		}
		for _, args := range commands {
			if err := ctx.Err(); err != nil {
				return err
			}
			result, err := taskRunner.Run(ctx, runner.RunCmdOpts{
				Name: DockerExecutable, Args: args, Dir: WorkDirectory, Stdin: stdin,
				Env: slices.Clone(fixedEnvironment), ReplaceEnv: true, CaptureLimitBytes: 32 * 1024,
			})
			clear(result.Stdout)
			clear(result.Stderr)
			if err != nil || result.ExitCode != 0 {
				return errs.New(errs.KindRequestFailed, "Volume consumer configuration could not be applied; inspect the captured workloads before retrying")
			}
		}
	}
	artifact, volume, err := capturedVolume(input)
	if err != nil {
		return err
	}
	return proveVolumeConsumersDetached(ctx, taskRunner, artifact, volume)
}
