package runner

import (
	"context"
	"errors"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"os"
)

func (operations *localOperations) StartProxy(ctx context.Context, plan corerunner.Plan) error {
	unit := proxyUnit(plan)
	args := []string{
		"--quiet", "--unit=" + unit,
		"--property=User=" + plan.Identity.User,
		"--property=Group=" + plan.Identity.Group,
		"--property=Restart=always", "--property=RestartSec=1",
		"--", "/usr/bin/socat",
		"UNIX-LISTEN:" + plan.Paths.ProxySocket + ",fork,mode=0600,unlink-early",
		"UNIX-CONNECT:" + plan.Paths.RawSocket,
	}
	if err := operations.ensureUnit(ctx, unit, args); err != nil {
		return err
	}
	return waitForSocket(ctx, plan.Paths.ProxySocket)
}

func (operations *localOperations) ObserveProxy(
	ctx context.Context,
	plan corerunner.Plan,
) (corerunner.StepEvidence, error) {
	if err := operations.observeUnit(ctx, proxyUnit(plan)); err != nil {
		return corerunner.StepEvidence{}, err
	}
	if _, _, err := socketIdentity(plan.Paths.ProxySocket); err != nil {
		return corerunner.StepEvidence{}, err
	}
	return applied(corerunner.StepStartProxy), nil
}

func (operations *localOperations) StopProxy(ctx context.Context, plan corerunner.Plan) (string, error) {
	if err := operations.stopUnit(ctx, proxyUnit(plan)); err != nil {
		return "", err
	}
	if err := os.Remove(plan.Paths.ProxySocket); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	return receipt(plan, corerunner.StepStopProxy), nil
}

func (operations *localOperations) ObserveProxyAbsent(
	ctx context.Context,
	plan corerunner.Plan,
) (corerunner.StepEvidence, error) {
	if err := operations.observeUnitAbsent(ctx, proxyUnit(plan)); err != nil {
		return corerunner.StepEvidence{}, err
	}
	if _, err := os.Lstat(plan.Paths.ProxySocket); err == nil || !errors.Is(err, os.ErrNotExist) {
		return corerunner.StepEvidence{}, errs.New(errs.KindStateConflict, "Runner proxy socket still exists")
	}
	return absent(plan, corerunner.StepStopProxy), nil
}
