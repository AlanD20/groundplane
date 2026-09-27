package runner

import (
	"context"
	"strconv"
	"strings"

	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func daemonDescription(plan corerunner.Plan) string {
	return "Groundplane Runner " + plan.RunnerID + " " + plan.Digest()
}

// unitState refuses to start or stop a same-name unit belonging to other work.
// A failed systemctl request is never interpreted as proof of absence.
func (operations *localOperations) unitState(ctx context.Context, plan corerunner.Plan) (string, error) {
	result, err := operations.run(ctx, "systemctl", "show",
		"--property=LoadState", "--property=ActiveState", "--property=Description",
		"--property=User", "--property=Group", daemonUnit(plan))
	if err != nil {
		return "", err
	}
	values := make(map[string]string)
	for _, line := range strings.Split(strings.TrimSpace(string(result.Stdout)), "\n") {
		key, value, found := strings.Cut(line, "=")
		if !found {
			return "", errs.New(errs.KindStateConflict, "Runner systemd observation is invalid")
		}
		values[key] = value
	}
	if values["LoadState"] == "not-found" {
		return "absent", nil
	}
	if values["LoadState"] != "loaded" || values["Description"] != daemonDescription(plan) ||
		values["User"] != plan.Identity.User || values["Group"] != plan.Identity.Group {
		return "", errs.New(errs.KindStateConflict, "Runner systemd unit ownership changed")
	}
	switch values["ActiveState"] {
	case "active", "inactive", "failed", "activating", "deactivating":
		return values["ActiveState"], nil
	default:
		return "", errs.New(errs.KindStateConflict, "Runner systemd state is unavailable")
	}
}

func (operations *localOperations) ensureUnit(ctx context.Context, plan corerunner.Plan, runArgs []string) error {
	state, err := operations.unitState(ctx, plan)
	if err != nil {
		return err
	}
	if state == "absent" {
		if _, err := operations.run(ctx, "systemd-run", runArgs...); err != nil {
			return err
		}
	} else if state != "active" {
		if _, err := operations.run(ctx, "systemctl", "start", daemonUnit(plan)); err != nil {
			return err
		}
	}
	return operations.observeUnit(ctx, plan)
}

func (operations *localOperations) observeUnit(ctx context.Context, plan corerunner.Plan) error {
	state, err := operations.unitState(ctx, plan)
	if err != nil {
		return err
	}
	if state != "active" {
		return errs.New(errs.KindStateConflict, "Runner systemd unit is not active")
	}
	return nil
}

func (operations *localOperations) stopUnit(ctx context.Context, plan corerunner.Plan) error {
	state, err := operations.unitState(ctx, plan)
	if err != nil {
		return err
	}
	if state == "absent" {
		return nil
	}
	if _, err := operations.run(ctx, "systemctl", "stop", daemonUnit(plan)); err != nil {
		return err
	}
	state, err = operations.unitState(ctx, plan)
	if err != nil {
		return err
	}
	if state == "failed" {
		if _, err := operations.run(ctx, "systemctl", "reset-failed", daemonUnit(plan)); err != nil {
			return err
		}
	}
	return operations.observeUnitAbsent(ctx, plan)
}

func (operations *localOperations) observeUnitAbsent(ctx context.Context, plan corerunner.Plan) error {
	state, err := operations.unitState(ctx, plan)
	if err != nil {
		return err
	}
	if state != "absent" && state != "inactive" {
		return errs.New(errs.KindStateConflict, "Runner systemd unit is not stopped")
	}
	// Even after the main daemon exits, user-namespace descendants must be gone
	// before the account or data directory can be removed.
	result, probeErr := operations.run(ctx, "pgrep", "-u", strconv.FormatUint(uint64(plan.Identity.UID), 10))
	if probeErr == nil || result.ExitCode != 1 {
		return errs.New(errs.KindStateConflict, "Runner processes remain after daemon shutdown")
	}
	return nil
}
