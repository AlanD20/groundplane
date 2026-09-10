package systemd

import (
	"context"
	"strings"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type upgradeCommands interface {
	Run(context.Context, runner.RunCmdOpts) (runner.Result, error)
}

// ControllerUpgrade can control only the native Controller and the Task-named
// transient predecessor watchdog. There is no operator-controlled executable,
// environment, unit, argument vector or shell string.
type ControllerUpgrade struct{ commands upgradeCommands }

func NewControllerUpgrade(commands upgradeCommands) (*ControllerUpgrade, error) {
	if commands == nil {
		return nil, errs.New(errs.KindInternal, "native upgrade command runner is required")
	}
	return &ControllerUpgrade{commands: commands}, nil
}

func (control *ControllerUpgrade) Launch(ctx context.Context, taskID string) error {
	if ids.Validate(ids.KindTask, taskID) != nil {
		return errs.New(errs.KindValidationFailed, "controller upgrade Task identity is invalid")
	}
	unit := "groundplane-controller-upgrade-" + taskID + ".service"
	_, err := control.run(ctx, "/usr/bin/systemd-run", []string{
		"--quiet", "--collect", "--unit=" + unit, "--expand-environment=no",
		"--property=Type=exec", "--property=Restart=on-failure", "--property=RestartSec=1s",
		"--property=StartLimitIntervalSec=0", "--property=UMask=0077", "--property=NoNewPrivileges=yes",
		upgrade.PreviousExecutable, upgrade.RecoveryFlag, taskID,
	})
	if err == nil {
		return nil
	}
	// Creation may have succeeded despite a lost response. The exact Task unit
	// with the fixed command is the only acceptable replay witness.
	result, readErr := control.run(
		ctx,
		"/usr/bin/systemctl",
		[]string{"show", "--property=ExecStart", "--value", unit},
	)
	if readErr == nil && strings.Contains(string(result.Stdout),
		"argv[]="+upgrade.PreviousExecutable+" "+upgrade.RecoveryFlag+" "+taskID+" ;") {
		return nil
	}
	return err
}

func (control *ControllerUpgrade) Stop(ctx context.Context) error {
	_, err := control.run(ctx, "/usr/bin/systemctl", []string{"stop", upgrade.ControllerUnit})
	return err
}

func (control *ControllerUpgrade) Start(ctx context.Context) error {
	_, err := control.run(ctx, "/usr/bin/systemctl", []string{"start", upgrade.ControllerUnit})
	return err
}

// VerifyBootstrap rejects a unit that would bypass the predecessor startup
// guard. The bootstrap installer owns the unit and executable paths.
func (control *ControllerUpgrade) VerifyBootstrap(ctx context.Context) error {
	for property, command := range map[string]string{
		"ExecStartPre": upgrade.RecoveryExecutable + " " + upgrade.GuardFlag,
		"ExecStart":    upgrade.ControllerExecutable,
	} {
		result, err := control.run(
			ctx,
			"/usr/bin/systemctl",
			[]string{"show", "--property=" + property, "--value", upgrade.ControllerUnit},
		)
		if err != nil {
			return err
		}
		if !strings.Contains(string(result.Stdout), "argv[]="+command+" ;") {
			return errs.New(
				errs.KindStateConflict,
				"native Controller recovery bootstrap is not installed",
			)
		}
	}
	return nil
}

func (control *ControllerUpgrade) run(
	ctx context.Context,
	name string,
	args []string,
) (runner.Result, error) {
	if err := ctx.Err(); err != nil {
		return runner.Result{}, err
	}
	result, err := control.commands.Run(ctx, runner.RunCmdOpts{Name: name, Args: args,
		Env: []string{"PATH=/usr/sbin:/usr/bin:/sbin:/bin", "LANG=C", "LC_ALL=C"}, ReplaceEnv: true,
		Timeout: 30 * time.Second, CaptureLimitBytes: 8192})
	if err != nil {
		return runner.Result{}, err
	}
	if result.ExitCode != 0 {
		return runner.Result{}, errs.New(errs.KindInternal, "native Controller unit command failed")
	}
	return result, nil
}
