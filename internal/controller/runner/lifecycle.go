package runner

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Operation = runnerallocation.RuntimeOperation
type Status = runnerallocation.RuntimeStatus
type Attempt = runnerallocation.RunnerRuntimeAttempt
type Progress = runnerallocation.RunnerRuntimeProgress

const (
	OperationCreate = runnerallocation.RuntimeOperationCreate
	OperationRemove = runnerallocation.RuntimeOperationRemove
	StatusRunning   = runnerallocation.RuntimeStatusRunning
	StatusFailed    = runnerallocation.RuntimeStatusFailed
	StatusReady     = runnerallocation.RuntimeStatusReady
	StatusRemoved   = runnerallocation.RuntimeStatusRemoved
)

type RuntimeEvidence = runnerallocation.RunnerRuntimeEvidence

type Journal interface {
	Begin(context.Context, Attempt, Operation) (Progress, error)
	Issue(context.Context, Progress, runnerallocation.RunnerRuntimeStep) (Progress, error)
	Checkpoint(context.Context, Progress, runnerallocation.RunnerRuntimeStepEvidence) (Progress, error)
	Fail(context.Context, Progress) error
	Ready(context.Context, Progress, RuntimeEvidence) error
	Removed(context.Context, Progress) error
}

type Runtime interface {
	EnsureIdentity(context.Context, runnerallocation.RuntimePlan) (runnerallocation.RunnerRuntimeStepEvidence, error)
	ObserveIdentity(context.Context, runnerallocation.RuntimePlan) (runnerallocation.RunnerRuntimeStepEvidence, error)
	EnsureNetwork(context.Context, runnerallocation.RuntimePlan) (runnerallocation.RunnerRuntimeStepEvidence, error)
	ObserveNetwork(context.Context, runnerallocation.RuntimePlan) (runnerallocation.RunnerRuntimeStepEvidence, error)
	EnsureEgress(context.Context, runnerallocation.RuntimePlan) (runnerallocation.RunnerRuntimeStepEvidence, error)
	ObserveEgress(context.Context, runnerallocation.RuntimePlan) (runnerallocation.RunnerRuntimeStepEvidence, error)
	StartDaemon(context.Context, runnerallocation.RuntimePlan) (runnerallocation.RunnerRuntimeStepEvidence, error)
	ObserveDaemon(context.Context, runnerallocation.RuntimePlan) (runnerallocation.RunnerRuntimeStepEvidence, error)
	StartRunner(
		context.Context,
		runnerallocation.RuntimePlan,
		[]byte,
	) (runnerallocation.RunnerRuntimeStepEvidence, error)
	ObserveRunner(context.Context, runnerallocation.RuntimePlan) (runnerallocation.RunnerRuntimeStepEvidence, error)
	StopRunner(context.Context, runnerallocation.RuntimePlan) (runnerallocation.RunnerRuntimeStepEvidence, error)
	ObserveRunnerAbsent(
		context.Context,
		runnerallocation.RuntimePlan,
	) (runnerallocation.RunnerRuntimeStepEvidence, error)
	StopDaemon(context.Context, runnerallocation.RuntimePlan) (runnerallocation.RunnerRuntimeStepEvidence, error)
	ObserveDaemonAbsent(
		context.Context,
		runnerallocation.RuntimePlan,
	) (runnerallocation.RunnerRuntimeStepEvidence, error)
	RemoveNetwork(context.Context, runnerallocation.RuntimePlan) (runnerallocation.RunnerRuntimeStepEvidence, error)
	ObserveNetworkAbsent(
		context.Context,
		runnerallocation.RuntimePlan,
	) (runnerallocation.RunnerRuntimeStepEvidence, error)
	RemoveIdentity(context.Context, runnerallocation.RuntimePlan) (runnerallocation.RunnerRuntimeStepEvidence, error)
	ObserveIdentityAbsent(
		context.Context,
		runnerallocation.RuntimePlan,
	) (runnerallocation.RunnerRuntimeStepEvidence, error)
	RemoveEgress(context.Context, runnerallocation.RuntimePlan) (runnerallocation.RunnerRuntimeStepEvidence, error)
	ObserveEgressAbsent(
		context.Context,
		runnerallocation.RuntimePlan,
	) (runnerallocation.RunnerRuntimeStepEvidence, error)
}

type Lifecycle struct {
	diagnostics *executiondiagnostics.Repository
	journal     Journal
	runtime     Runtime
}

// EffectAuthority is bound to one native attempt. A local journal entry alone
// cannot authorize an effect after Abort or replacement of the Controller.
type EffectAuthority interface {
	Grant(context.Context, runnerallocation.RunnerRuntimeStep, time.Time) error
}

func NewLifecycle(journal Journal, runtime Runtime, diagnostics *executiondiagnostics.Repository) (*Lifecycle, error) {
	if journal == nil || runtime == nil || diagnostics == nil {
		return nil, errs.New(errs.KindInternal, "runner lifecycle dependencies are required")
	}
	return &Lifecycle{journal: journal, runtime: runtime, diagnostics: diagnostics}, nil
}

type RegistrationToken struct{ value []byte }

func NewRegistrationToken(source []byte) (*RegistrationToken, error) {
	defer clear(source)
	if !validRegistrationToken(source) {
		return nil, errs.New(errs.KindValidationFailed, "runner registration token is invalid")
	}
	return &RegistrationToken{value: append([]byte(nil), source...)}, nil
}

func validRegistrationToken(value []byte) bool {
	if len(value) == 0 || len(value) > 4096 {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}

func (token *RegistrationToken) take() []byte {
	if token == nil {
		return nil
	}
	value := append([]byte(nil), token.value...)
	clear(token.value)
	token.value = nil
	return value
}

func (token *RegistrationToken) clear() {
	if token != nil {
		clear(token.value)
		token.value = nil
	}
}

func (lifecycle *Lifecycle) CreateWithEvidence(
	ctx context.Context,
	attempt Attempt,
	token *RegistrationToken,
	authority EffectAuthority,
) (RuntimeEvidence, error) {
	defer token.clear()
	var evidence RuntimeEvidence
	err := lifecycle.execute(
		ctx, attempt, OperationCreate, runnerallocation.CreationRuntimeSteps(), token, &evidence, authority,
	)
	return evidence, err
}

func (lifecycle *Lifecycle) Remove(ctx context.Context, attempt Attempt, authority EffectAuthority) error {
	return lifecycle.execute(ctx, attempt, OperationRemove, runnerallocation.RemovalRuntimeSteps(), nil, nil, authority)
}

func (lifecycle *Lifecycle) execute(
	ctx context.Context,
	attempt Attempt,
	operation Operation,
	steps []runnerallocation.RunnerRuntimeStep,
	token *RegistrationToken,
	result *RuntimeEvidence,
	authority EffectAuthority,
) error {
	if ctx == nil || ctx.Err() != nil || attempt.TaskID == "" || attempt.Plan.Digest() == "" || authority == nil {
		return errs.New(errs.KindValidationFailed, "runner lifecycle attempt is invalid")
	}
	progress, err := lifecycle.journal.Begin(ctx, attempt, operation)
	if err != nil {
		return err
	}
	if operation == OperationCreate && progress.Status == StatusReady ||
		operation == OperationRemove && progress.Status == StatusRemoved {
		if operation == OperationCreate {
			if progress.Evidence == nil || !progress.Evidence.Valid() || result == nil {
				return errs.New(errs.KindInternal, "runner ready journal is missing ownership evidence")
			}
			*result = *progress.Evidence
		}
		return nil
	}
	if progress.Status == StatusFailed {
		return errs.New(errs.KindStateConflict, "runner lifecycle attempt is already failed")
	}
	// A completed registration or an issued registration being observed does
	// not consume another token. Reject missing input before any new host setup.
	if operation == OperationCreate && (token == nil || !validRegistrationToken(token.value)) {
		for index := progress.NextStep; index < len(steps); index++ {
			if steps[index] == runnerallocation.StepStartRunner &&
				(progress.ActiveStep == nil || *progress.ActiveStep != runnerallocation.StepStartRunner) {
				return errs.New(errs.KindValidationFailed, "registration_token_required")
			}
		}
	}
	for index := progress.NextStep; index < len(steps); index++ {
		step := steps[index]
		var transient []byte
		if step == runnerallocation.StepStartRunner && progress.ActiveStep == nil {
			transient = token.take()
			if len(transient) == 0 {
				return lifecycle.fail(ctx, progress, errs.New(errs.KindValidationFailed, "registration_token_required"))
			}
		}
		var stepEvidence runnerallocation.RunnerRuntimeStepEvidence
		report := lifecycle.beginStage(ctx, attempt, step)
		if progress.ActiveStep != nil {
			if *progress.ActiveStep != step {
				clear(transient)
				report.finish(ctx, false)
				return lifecycle.fail(
					ctx,
					progress,
					errs.New(errs.KindInternal, "runner lifecycle active step is corrupt"),
				)
			}
			stepEvidence, err = lifecycle.observe(ctx, progress.Plan, step)
		} else {
			progress, err = lifecycle.journal.Issue(ctx, progress, step)
			if err == nil {
				err = authority.Grant(ctx, step, time.Now().UTC())
			}
			if err == nil {
				stepEvidence, err = lifecycle.apply(ctx, progress.Plan, step, transient)
				if err != nil {
					resolved, resolveErr := lifecycle.observe(ctx, progress.Plan, step)
					if resolveErr == nil {
						stepEvidence, err = resolved, nil
					} else {
						err = errors.Join(err, resolveErr)
					}
				}
			}
		}
		clear(transient)
		if err != nil {
			report.finish(ctx, false)
			return lifecycle.fail(ctx, progress, err)
		}
		if !stepEvidence.Valid(operation, step) {
			report.finish(ctx, false)
			return lifecycle.fail(ctx, progress, errs.New(errs.KindInternal, "runner runtime evidence is invalid"))
		}
		progress, err = lifecycle.journal.Checkpoint(ctx, progress, stepEvidence)
		if err != nil {
			report.finish(ctx, false)
			return err
		}
		report.finish(ctx, true)
	}
	if operation == OperationRemove {
		return lifecycle.journal.Removed(ctx, progress)
	}
	if progress.Evidence == nil || !progress.Evidence.Valid() {
		return lifecycle.fail(
			ctx,
			progress,
			errs.New(errs.KindInternal, "runner runtime returned invalid ownership evidence"),
		)
	}
	if result == nil {
		return lifecycle.fail(
			ctx,
			progress,
			errs.New(errs.KindInternal, "runner creation evidence destination is missing"),
		)
	}
	*result = *progress.Evidence
	return lifecycle.journal.Ready(ctx, progress, *progress.Evidence)
}

func (lifecycle *Lifecycle) apply(
	ctx context.Context,
	plan runnerallocation.RuntimePlan,
	step runnerallocation.RunnerRuntimeStep,
	token []byte,
) (runnerallocation.RunnerRuntimeStepEvidence, error) {
	if step != runnerallocation.StepStartRunner && len(token) != 0 {
		clear(token)
		return runnerallocation.RunnerRuntimeStepEvidence{}, errs.New(
			errs.KindInternal,
			"runner token reached a non-registration capability",
		)
	}
	switch step {
	case runnerallocation.StepEnsureIdentity:
		return lifecycle.runtime.EnsureIdentity(ctx, plan)
	case runnerallocation.StepEnsureNetwork:
		return lifecycle.runtime.EnsureNetwork(ctx, plan)
	case runnerallocation.StepEnsureEgress:
		return lifecycle.runtime.EnsureEgress(ctx, plan)
	case runnerallocation.StepStartDaemon:
		return lifecycle.runtime.StartDaemon(ctx, plan)
	case runnerallocation.StepStartRunner:
		return lifecycle.runtime.StartRunner(ctx, plan, token)
	case runnerallocation.StepStopRunner:
		return lifecycle.runtime.StopRunner(ctx, plan)
	case runnerallocation.StepStopDaemon:
		return lifecycle.runtime.StopDaemon(ctx, plan)
	case runnerallocation.StepRemoveNetwork:
		return lifecycle.runtime.RemoveNetwork(ctx, plan)
	case runnerallocation.StepRemoveIdentity:
		return lifecycle.runtime.RemoveIdentity(ctx, plan)
	case runnerallocation.StepRemoveEgress:
		return lifecycle.runtime.RemoveEgress(ctx, plan)
	default:
		return runnerallocation.RunnerRuntimeStepEvidence{}, errs.New(
			errs.KindInternal,
			"runner plan contains an unknown capability",
		)
	}
}

func (lifecycle *Lifecycle) observe(
	ctx context.Context,
	plan runnerallocation.RuntimePlan,
	step runnerallocation.RunnerRuntimeStep,
) (runnerallocation.RunnerRuntimeStepEvidence, error) {
	switch step {
	case runnerallocation.StepEnsureIdentity:
		return lifecycle.runtime.ObserveIdentity(ctx, plan)
	case runnerallocation.StepEnsureNetwork:
		return lifecycle.runtime.ObserveNetwork(ctx, plan)
	case runnerallocation.StepEnsureEgress:
		return lifecycle.runtime.ObserveEgress(ctx, plan)
	case runnerallocation.StepStartDaemon:
		return lifecycle.runtime.ObserveDaemon(ctx, plan)
	case runnerallocation.StepStartRunner:
		return lifecycle.runtime.ObserveRunner(ctx, plan)
	case runnerallocation.StepStopRunner:
		return lifecycle.runtime.ObserveRunnerAbsent(ctx, plan)
	case runnerallocation.StepStopDaemon:
		return lifecycle.runtime.ObserveDaemonAbsent(ctx, plan)
	case runnerallocation.StepRemoveNetwork:
		return lifecycle.runtime.ObserveNetworkAbsent(ctx, plan)
	case runnerallocation.StepRemoveIdentity:
		return lifecycle.runtime.ObserveIdentityAbsent(ctx, plan)
	case runnerallocation.StepRemoveEgress:
		return lifecycle.runtime.ObserveEgressAbsent(ctx, plan)
	default:
		return runnerallocation.RunnerRuntimeStepEvidence{}, errs.New(
			errs.KindInternal,
			"runner plan contains an unknown observation capability",
		)
	}
}

func (lifecycle *Lifecycle) fail(ctx context.Context, progress Progress, cause error) error {
	if journalErr := lifecycle.journal.Fail(ctx, progress); journalErr != nil {
		return errors.Join(cause, journalErr)
	}
	return cause
}
