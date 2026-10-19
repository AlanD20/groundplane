package materialization

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/infra/docker/materializerrunner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// StopForwardFiles requires a granted quiescence boundary. Its caller holds
// exclusive process ownership before invoking this exact captured selection.
func (runtime *Runtime) StopForwardFiles(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
) error {
	if runtime == nil || runtime.helper == nil || ctx == nil || report.Validate(input) != nil || report.Ack == nil ||
		!report.Progress.InFlight || report.Progress.Phase != releaseexecution.QuiesceForward {
		return errs.New(errs.KindStateConflict, "file helper cleanup requires an acknowledged recovery boundary")
	}
	stage, err := releaseexecution.ConfigurationStageID(input.Forward.Input)
	if err != nil {
		return err
	}
	configuration := input.Input.Release.Configuration
	for _, destination := range input.Input.Release.ConfigurationDestinations {
		if err := runtime.helper.StopCaptured(ctx, materializerrunner.Ownership{
			TaskID: input.Forward.TaskID, StageID: stage, EnvironmentID: input.Input.Release.EnvironmentId,
			Destination: destination, Generation: configuration.Generation,
		}); err != nil {
			return err
		}
	}
	return nil
}

// StopInterruptedRestorationFiles closes the prior restore/verification helper
// scopes at quiescence, before any new inspection or mutation. Stable stage ids
// bind the same captured input across retries; no file contents are resolved.
func (runtime *Runtime) StopInterruptedRestorationFiles(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
) error {
	if runtime == nil || runtime.helper == nil || ctx == nil || report.Validate(input) != nil || report.Ack == nil ||
		!report.Progress.InFlight || report.Progress.Phase != releaseexecution.QuiesceForward {
		return errs.New(
			errs.KindStateConflict,
			"interrupted restoration files require an acknowledged quiescence boundary",
		)
	}
	owner := input.Input.Attempt
	if input.Interruption != nil {
		owner = input.Interruption.Attempt
	}
	generation := input.Input.Release.Configuration.Generation
	if selected := input.RecoveryConfigurationReference(); selected != nil {
		generation = selected.Generation
	}
	for _, phase := range []releaseexecution.RecoveryPhase{releaseexecution.RestoreConfiguration, releaseexecution.VerifyRestoration} {
		stage, err := releaseexecution.RecoveryStageID(owner.Input, phase)
		if err != nil {
			return err
		}
		for _, destination := range input.Input.Release.ConfigurationDestinations {
			if err := runtime.helper.StopCaptured(ctx, materializerrunner.Ownership{
				TaskID: owner.TaskID, StageID: stage, EnvironmentID: input.Input.Release.EnvironmentId,
				Destination: destination, Generation: generation,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// StopRecoveryFiles removes abandoned restore/verification helpers before a
// new file operation. Stage identities and generations come from the immutable
// selection; no resource lookup or latest desired configuration is consulted.
func (runtime *Runtime) StopRecoveryFiles(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
	files []entrymaterialization.Header,
) error {
	if runtime == nil || runtime.helper == nil || ctx == nil {
		return errs.New(errs.KindStateConflict, "recovery file helper cleanup is unavailable")
	}
	if _, err := releaseexecution.RecoveryConfigurationResult(input, report, files); err != nil {
		return err
	}
	for _, phase := range []releaseexecution.RecoveryPhase{releaseexecution.RestoreConfiguration, releaseexecution.VerifyRestoration} {
		stage, err := releaseexecution.RecoveryStageID(input.Input.Attempt.Input, phase)
		if err != nil {
			return err
		}
		for _, file := range files {
			owner := materializerrunner.OwnershipFromHeader(file)
			owner.StageID = stage
			if err := runtime.helper.StopCaptured(ctx, owner); err != nil {
				return err
			}
		}
	}
	return nil
}
