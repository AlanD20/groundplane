package runner

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type hostOperations interface {
	EnsureIdentity(context.Context, corerunner.Plan) error
	ObserveIdentity(context.Context, corerunner.Plan) (corerunner.StepEvidence, error)
	EnsureNetwork(context.Context, corerunner.Plan) error
	ObserveNetwork(context.Context, corerunner.Plan) (corerunner.StepEvidence, error)
	EnsureEgress(context.Context, corerunner.Plan) error
	ObserveEgress(context.Context, corerunner.Plan) (corerunner.StepEvidence, error)
	StartProxy(context.Context, corerunner.Plan) error
	ObserveProxy(context.Context, corerunner.Plan) (corerunner.StepEvidence, error)
	StartDaemon(context.Context, corerunner.Plan) error
	ObserveDaemon(context.Context, corerunner.Plan) (corerunner.StepEvidence, error)
	StartRunner(context.Context, corerunner.Plan, []byte) (runnerallocation.RunnerRuntimeEvidence, error)
	ObserveRunner(context.Context, corerunner.Plan) (corerunner.StepEvidence, error)
	StopRunner(context.Context, corerunner.Plan) (string, error)
	ObserveRunnerAbsent(context.Context, corerunner.Plan) (corerunner.StepEvidence, error)
	StopDaemon(context.Context, corerunner.Plan) (string, error)
	ObserveDaemonAbsent(context.Context, corerunner.Plan) (corerunner.StepEvidence, error)
	StopProxy(context.Context, corerunner.Plan) (string, error)
	ObserveProxyAbsent(context.Context, corerunner.Plan) (corerunner.StepEvidence, error)
	RemoveNetwork(context.Context, corerunner.Plan) (string, error)
	ObserveNetworkAbsent(context.Context, corerunner.Plan) (corerunner.StepEvidence, error)
	RemoveIdentity(context.Context, corerunner.Plan) (string, error)
	ObserveIdentityAbsent(context.Context, corerunner.Plan) (corerunner.StepEvidence, error)
	RemoveEgress(context.Context, corerunner.Plan) (string, error)
	ObserveEgressAbsent(context.Context, corerunner.Plan) (corerunner.StepEvidence, error)
}

// HostControl is the sole validated host-effect facade. Every method fixes
// one capability and evidence shape before crossing the transport seam.
type HostControl struct{ operations hostOperations }

func New(operations hostOperations) (*HostControl, error) {
	if operations == nil {
		return nil, errs.New(errs.KindInternal, "runner host control operations are required")
	}
	return &HostControl{operations: operations}, nil
}

func (control *HostControl) EnsureIdentity(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.ensure(ctx, plan, corerunner.StepEnsureIdentity, control.operations.EnsureIdentity)
}
func (control *HostControl) ObserveIdentity(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.observe(ctx, plan, corerunner.StepEnsureIdentity, false, control.operations.ObserveIdentity)
}
func (control *HostControl) EnsureNetwork(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.ensure(ctx, plan, corerunner.StepEnsureNetwork, control.operations.EnsureNetwork)
}
func (control *HostControl) ObserveNetwork(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.observe(ctx, plan, corerunner.StepEnsureNetwork, false, control.operations.ObserveNetwork)
}
func (control *HostControl) EnsureEgress(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.ensure(ctx, plan, corerunner.StepEnsureEgress, control.operations.EnsureEgress)
}
func (control *HostControl) ObserveEgress(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.observe(ctx, plan, corerunner.StepEnsureEgress, false, control.operations.ObserveEgress)
}
func (control *HostControl) StartProxy(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.ensure(ctx, plan, corerunner.StepStartProxy, control.operations.StartProxy)
}
func (control *HostControl) ObserveProxy(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.observe(ctx, plan, corerunner.StepStartProxy, false, control.operations.ObserveProxy)
}
func (control *HostControl) StartDaemon(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.ensure(ctx, plan, corerunner.StepStartDaemon, control.operations.StartDaemon)
}
func (control *HostControl) ObserveDaemon(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.observe(ctx, plan, corerunner.StepStartDaemon, false, control.operations.ObserveDaemon)
}

func (control *HostControl) StartRunner(
	ctx context.Context,
	plan corerunner.Plan,
	token []byte,
) (corerunner.StepEvidence, error) {
	if err := validateCall(ctx, plan); err != nil {
		clear(token)
		return corerunner.StepEvidence{}, err
	}
	if len(token) == 0 {
		return corerunner.StepEvidence{}, errs.New(errs.KindValidationFailed, "registration_token_required")
	}
	evidence, err := control.operations.StartRunner(ctx, plan, token)
	clear(token)
	if err != nil {
		return corerunner.StepEvidence{}, err
	}
	result := corerunner.StepEvidence{
		Step: corerunner.StepStartRunner, State: corerunner.EffectApplied, Ownership: &evidence,
	}
	if !result.Valid(runnerallocation.RuntimeOperationCreate, corerunner.StepStartRunner) {
		return corerunner.StepEvidence{}, errs.New(errs.KindInternal, "runner start evidence is invalid")
	}
	return result, nil
}
func (control *HostControl) ObserveRunner(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.observe(ctx, plan, corerunner.StepStartRunner, false, control.operations.ObserveRunner)
}
func (control *HostControl) StopRunner(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.remove(ctx, plan, corerunner.StepStopRunner, control.operations.StopRunner)
}
func (control *HostControl) ObserveRunnerAbsent(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.observe(ctx, plan, corerunner.StepStopRunner, true, control.operations.ObserveRunnerAbsent)
}
func (control *HostControl) StopDaemon(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.remove(ctx, plan, corerunner.StepStopDaemon, control.operations.StopDaemon)
}
func (control *HostControl) ObserveDaemonAbsent(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.observe(ctx, plan, corerunner.StepStopDaemon, true, control.operations.ObserveDaemonAbsent)
}
func (control *HostControl) StopProxy(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.remove(ctx, plan, corerunner.StepStopProxy, control.operations.StopProxy)
}
func (control *HostControl) ObserveProxyAbsent(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.observe(ctx, plan, corerunner.StepStopProxy, true, control.operations.ObserveProxyAbsent)
}
func (control *HostControl) RemoveNetwork(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.remove(ctx, plan, corerunner.StepRemoveNetwork, control.operations.RemoveNetwork)
}
func (control *HostControl) ObserveNetworkAbsent(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.observe(ctx, plan, corerunner.StepRemoveNetwork, true, control.operations.ObserveNetworkAbsent)
}
func (control *HostControl) RemoveIdentity(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.remove(ctx, plan, corerunner.StepRemoveIdentity, control.operations.RemoveIdentity)
}
func (control *HostControl) ObserveIdentityAbsent(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.observe(ctx, plan, corerunner.StepRemoveIdentity, true, control.operations.ObserveIdentityAbsent)
}
func (control *HostControl) RemoveEgress(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.remove(ctx, plan, corerunner.StepRemoveEgress, control.operations.RemoveEgress)
}
func (control *HostControl) ObserveEgressAbsent(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return control.observe(ctx, plan, corerunner.StepRemoveEgress, true, control.operations.ObserveEgressAbsent)
}

func (control *HostControl) ensure(
	ctx context.Context,
	plan corerunner.Plan,
	step corerunner.Step,
	operation func(context.Context, corerunner.Plan) error,
) (corerunner.StepEvidence, error) {
	if err := validateCall(ctx, plan); err != nil {
		return corerunner.StepEvidence{}, err
	}
	if err := operation(ctx, plan); err != nil {
		return corerunner.StepEvidence{}, err
	}
	return corerunner.StepEvidence{Step: step, State: corerunner.EffectApplied}, nil
}

func (control *HostControl) remove(
	ctx context.Context,
	plan corerunner.Plan,
	step corerunner.Step,
	operation func(context.Context, corerunner.Plan) (string, error),
) (corerunner.StepEvidence, error) {
	if err := validateCall(ctx, plan); err != nil {
		return corerunner.StepEvidence{}, err
	}
	receipt, err := operation(ctx, plan)
	if err != nil {
		return corerunner.StepEvidence{}, err
	}
	result := corerunner.StepEvidence{Step: step, State: corerunner.EffectAbsent, ReceiptSHA256: receipt}
	if !result.Valid(runnerallocation.RuntimeOperationRemove, step) {
		return corerunner.StepEvidence{}, errs.New(errs.KindInternal, "runner removal evidence is invalid")
	}
	return result, nil
}

func (control *HostControl) observe(
	ctx context.Context,
	plan corerunner.Plan,
	step corerunner.Step,
	removal bool,
	operation func(context.Context, corerunner.Plan) (corerunner.StepEvidence, error),
) (corerunner.StepEvidence, error) {
	if err := validateCall(ctx, plan); err != nil {
		return corerunner.StepEvidence{}, err
	}
	evidence, err := operation(ctx, plan)
	if err != nil {
		return corerunner.StepEvidence{}, err
	}
	want := runnerallocation.RuntimeOperationCreate
	if removal {
		want = runnerallocation.RuntimeOperationRemove
	}
	if !evidence.Valid(want, step) {
		return corerunner.StepEvidence{}, errs.New(errs.KindInternal, "runner observation evidence is invalid")
	}
	return evidence, nil
}

func validateCall(ctx context.Context, plan corerunner.Plan) error {
	if ctx == nil || ctx.Err() != nil {
		return errs.New(errs.KindValidationFailed, "runner host control context is invalid")
	}
	return corerunner.ValidatePlan(plan)
}
