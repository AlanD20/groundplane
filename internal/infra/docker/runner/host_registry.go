package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"

	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/internal/infra/registryconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func runnerDockerConfig(plan corerunner.Plan) string {
	return filepath.Join(plan.Paths.RunnerHome, ".groundplane-docker")
}

// Seed ordinary Docker credentials once. Trusted workflows may add other
// logins; maintenance must not overwrite their Docker configuration.
func prepareRunnerRegistry(ctx context.Context, plan corerunner.Plan) error {
	directory := runnerDockerConfig(plan)
	if err := ensureOwnedDirectory(directory, plan.Identity.UID, plan.Identity.GID, 0o700); err != nil {
		return err
	}
	path := filepath.Join(directory, "config.json")
	if info, err := os.Lstat(path); err == nil {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || stat.Uid != plan.Identity.UID {
			return errs.New(errs.KindStateConflict, "Runner Docker credentials ownership changed")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return errs.Wrap(errs.KindInternal, err)
	}
	value, err := registryconfiguration.ReadFile(ctx, "client/config.json")
	if err != nil {
		return err
	}
	defer clear(value)
	file, err := os.CreateTemp(directory, ".credentials-")
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer os.Remove(file.Name()) // The stage is absent after successful publication.
	_, writeErr := file.Write(value)
	ownerErr := file.Chown(int(plan.Identity.UID), int(plan.Identity.GID))
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, ownerErr, syncErr, closeErr); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	// Link publishes without replacing a file a job created concurrently.
	if err := os.Link(file.Name(), path); err != nil && !errors.Is(err, os.ErrExist) {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}
