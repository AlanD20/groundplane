package runner

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
)

func (operations *localOperations) ensureUnit(ctx context.Context, unit string, runArgs []string) error {
	load, _ := operations.run(ctx, "systemctl", "show", "--property=LoadState", "--value", unit)
	if strings.TrimSpace(string(load.Stdout)) == "loaded" {
		if _, err := operations.run(ctx, "systemctl", "start", unit); err != nil {
			return err
		}
		return operations.observeUnit(ctx, unit)
	}
	if _, err := operations.run(ctx, "systemd-run", runArgs...); err != nil {
		return err
	}
	return operations.observeUnit(ctx, unit)
}

func (operations *localOperations) observeUnit(ctx context.Context, unit string) error {
	result, err := operations.run(ctx, "systemctl", "is-active", unit)
	if err != nil || strings.TrimSpace(string(result.Stdout)) != "active" {
		return errs.New(errs.KindStateConflict, "Runner systemd unit is not active")
	}
	return nil
}

func (operations *localOperations) stopUnit(ctx context.Context, unit string) error {
	result, err := operations.run(ctx, "systemctl", "stop", unit)
	if err != nil && result.ExitCode != 5 {
		return err
	}
	_, _ = operations.run(ctx, "systemctl", "reset-failed", unit)
	return nil
}

func (operations *localOperations) observeUnitAbsent(ctx context.Context, unit string) error {
	result, err := operations.run(ctx, "systemctl", "is-active", unit)
	if err == nil || strings.TrimSpace(string(result.Stdout)) == "active" {
		return errs.New(errs.KindStateConflict, "Runner systemd unit is still active")
	}
	return nil
}
