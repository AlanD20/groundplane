package app

import (
	"context"
	"log/slog"
	"os"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/controller/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/infra/controllerrelease"
	"github.com/AlanD20/groundplane/internal/infra/systemd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RunControllerProcess dispatches only the dedicated binary's closed bootstrap
// modes. Neither mode constructs Controller/etcd/Docker or loads candidate config.
// The public CLI's controller serve remains the ordinary foreground daemon.
func RunControllerProcess(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return RunController(ctx)
	}
	guard := len(args) == 1 && args[0] == upgrade.GuardFlag
	recovering := len(args) == 2 && args[0] == upgrade.RecoveryFlag
	if !guard && !recovering {
		return errs.New(errs.KindValidationFailed, "unsupported Controller process arguments")
	}
	if os.Geteuid() != 0 {
		return errs.New(errs.KindStateConflict, "native Controller recovery requires root")
	}
	store, err := controllerrelease.Open(ctx)
	if err != nil {
		return err
	}
	defer store.Close()
	if guard {
		return store.Guard(ctx, time.Now().UTC())
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	control, err := systemd.NewControllerUpgrade(runner.New(logger))
	if err != nil {
		return err
	}
	watchdog, err := controllerupgrade.NewWatchdog(store, control)
	if err != nil {
		return err
	}
	return watchdog.Run(ctx, args[1])
}
