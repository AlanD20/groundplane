package releaseruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RecoveryDriver struct {
	quiescence    *RecoveryQuiescence
	configuration *RecoveryConfiguration
	helper        RecoveryHelper
	evidence      *checkpointdelivery.RecoveryEffect
	hooks         *Hooks
}

func NewRecoveryDriver(
	quiescence *RecoveryQuiescence,
	configuration *RecoveryConfiguration,
	helper RecoveryHelper,
	evidence *checkpointdelivery.RecoveryEffect,
	hooks *Hooks,
) (*RecoveryDriver, error) {
	if quiescence == nil || configuration == nil || helper == nil || evidence == nil || hooks == nil {
		return nil, errs.New(errs.KindInternal, "Release recovery driver dependencies are incomplete")
	}
	return &RecoveryDriver{
		quiescence:    quiescence,
		configuration: configuration,
		helper:        helper,
		evidence:      evidence,
		hooks:         hooks,
	}, nil
}

func (driver *RecoveryDriver) ExecuteRecovery(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
) (workexecution.Input, error) {
	return driver.run(ctx, input, report, false)
}

func (driver *RecoveryDriver) InspectRecovery(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
) (workexecution.Input, error) {
	return driver.run(ctx, input, report, true)
}

func (driver *RecoveryDriver) run(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
	inspectOnly bool,
) (workexecution.Input, error) {
	if driver == nil || ctx == nil || report.Validate(input) != nil || report.Ack == nil || !report.Progress.InFlight {
		return workexecution.Input{}, errs.New(
			errs.KindStateConflict,
			"Release recovery driver has no acknowledged boundary",
		)
	}
	if reference, found, err := driver.evidence.Resume(ctx, report); err != nil || found {
		return reference, err
	}
	result := releaseexecution.RecoveryEffectResult{}
	switch report.Progress.Phase {
	case releaseexecution.QuiesceForward:
		proof, err := driver.quiescence.Observe(ctx, input, report)
		if err != nil {
			return workexecution.Input{}, err
		}
		result.Quiescence = &proof
	case releaseexecution.RestoreConfiguration:
		proof, err := driver.configuration.Run(ctx, input, report, inspectOnly)
		if err != nil {
			return workexecution.Input{}, err
		}
		result.Configuration = &proof
	case releaseexecution.InspectFailedRuntime,
		releaseexecution.RestoreServingRuntime,
		releaseexecution.RemoveFailedCandidate:
		proof, err := RunRecoveryHelper(ctx, driver.helper, input, report, inspectOnly)
		if err != nil {
			return workexecution.Input{}, err
		}
		result.Runtime = &proof
	case releaseexecution.VerifyRestoration:
		files, err := driver.configuration.Run(ctx, input, report, true)
		if err != nil {
			return workexecution.Input{}, err
		}
		runtime, err := RunRecoveryHelper(ctx, driver.helper, input, report, true)
		if err != nil {
			return workexecution.Input{}, err
		}
		result.Configuration, result.Runtime = &files, &runtime
	case releaseexecution.FailureHooks:
		proof, err := driver.hooks.RunRecovery(ctx, input, report)
		if err != nil {
			return workexecution.Input{}, err
		}
		result.Hook = &proof
	default:
		return workexecution.Input{}, errs.New(errs.KindStateConflict, "Release recovery phase has no host action")
	}
	return driver.evidence.Record(ctx, report, result)
}
