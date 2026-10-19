package composehelper

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/docker/composeobserver"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func ExecuteBackupConsumer(
	ctx context.Context,
	taskRunner runner.Runner,
	request *agentpb.BackupConsumerHelperRequest,
) (_ *agentpb.BackupConsumerHelperResponse, resultErr error) {
	input, err := executionplan.OpenBackupConsumerHelper(ctx, request)
	if err != nil {
		return nil, err
	}
	if taskRunner == nil {
		return nil, errs.New(errs.KindInternal, "Backup Volume consumer runner is missing")
	}
	deadline := input.Consumer.Attempt.Deadline
	if source := time.Unix(0, int64(input.Consumer.Source.StepDeadlineUnixNano)); source.Before(deadline) {
		deadline = source
	}
	bounded, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	observer, err := composeobserver.New()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := observer.Close(); err != nil {
			resultErr = errs.WrapJoined(errs.KindRequestFailed, resultErr, err)
		}
	}()
	observed, err := observer.ObserveBackupConsumer(bounded, request)
	if err != nil {
		return nil, err
	}
	if !input.Inspection {
		artifact := input.Consumer.Artifact
		stdin, err := backupConsumerCLIInput(input.Consumer)
		if err != nil {
			return nil, err
		}
		defer clear(stdin)
		prefix := []string{
			"compose",
			"--project-name",
			artifact.ProjectName,
			"--project-directory",
			composeProjectDirectory(artifact),
			"--file",
			"-",
		}
		base := runner.RunCmdOpts{
			Name:              DockerExecutable,
			Dir:               WorkDirectory,
			Env:               slices.Clone(fixedEnvironment),
			ReplaceEnv:        true,
			Stdin:             stdin,
			CaptureLimitBytes: 32 * 1024,
		}
		validation := base
		validation.Args = append(slices.Clone(prefix), "config", "--quiet", "--no-interpolate")
		mutation := base
		if input.Operation == agentpb.BackupConsumerOperation_BACKUP_CONSUMER_OPERATION_STOP {
			mutation.Args = append(slices.Clone(prefix), "stop", "--timeout", "30", input.Consumer.Service.ComposeName)
		} else {
			mutation.Args = append(slices.Clone(prefix), "up", "--detach", "--no-deps", "--no-build", "--pull", "never", "--wait", input.Consumer.Service.ComposeName)
		}
		for _, command := range []runner.RunCmdOpts{validation, mutation} {
			result, err := taskRunner.Run(bounded, command)
			if err != nil {
				return nil, err
			}
			if result.ExitCode != 0 {
				return nil, errs.New(errs.KindRequestFailed, "Backup Volume consumer Compose action failed")
			}
		}
		observed, err = observer.ObserveBackupConsumer(bounded, request)
		if err != nil {
			return nil, err
		}
	}
	digest, err := executionplan.BackupConsumerObservation(input.Consumer, observed)
	if err != nil {
		return nil, err
	}
	wire, err := workexecution.EncodeAgentAttempt(input.Consumer.Attempt)
	if err != nil {
		return nil, err
	}
	response := &agentpb.BackupConsumerHelperResponse{
		Attempt:           wire,
		SourceOrdinal:     input.SourceOrdinal,
		ServiceId:         input.Consumer.Fact.ServiceId,
		Inspection:        input.Inspection,
		Observed:          observed,
		ObservationSha256: digest,
		ObservedAt:        observed.ObservedAt,
	}
	if err := executionplan.ValidateBackupConsumerHelperResponse(input, response); err != nil {
		return nil, err
	}
	return response, nil
}
