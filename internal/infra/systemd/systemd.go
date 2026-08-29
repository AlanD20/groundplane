// Package systemd handles the sole Groundplane systemd unit, the Controller's
// self-update via staged-binary swap, and backup-schedule calendar validation.
//
// DOCUMENTED os/exec EXCEPTION (docs/standards.md, section 5):
// this package is one of the three listed exceptions ("systemd unit
// control inside internal/infra") allowed to shell out directly rather
// than going through internal/common/runner.Runner — systemctl/
// systemd-analyze invocations are host-lifecycle operations tightly
// coupled to this package's own reasoning about unit state, not general
// adapter/task-step execution.
package systemd

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const systemUnitDirectory = "/etc/systemd/system"

var managedUnits = map[string]struct{}{
	"groundplane-controller.service": {},
}

// ValidateCalendar shells out to `systemd-analyze calendar <expr>` to
// validate a backup policy's Frequency before it's saved.
func ValidateCalendar(ctx context.Context, expr string) error {
	cmd := exec.CommandContext(ctx, "systemd-analyze", "calendar", expr)
	output, err := cmd.CombinedOutput()
	if err == nil {
		return nil
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("systemd: execute calendar validator: %w", err))
	}
	detail := strings.TrimSpace(string(output))
	if detail == "" {
		detail = exitErr.Error()
	}
	return errs.Newf(errs.KindValidationFailed, "systemd: invalid calendar expression %q: %s", expr, detail)
}

// InstallUnit validates and atomically replaces one Groundplane-owned service
// unit, then reloads systemd. A failed reload restores the previous file.
func InstallUnit(ctx context.Context, name, contents string) error {
	if _, ok := managedUnits[name]; !ok {
		return errs.Newf(errs.KindValidationFailed, "systemd: unit %q is not Groundplane-managed", name)
	}
	if strings.TrimSpace(contents) == "" {
		return errs.Newf(errs.KindValidationFailed, "systemd: unit %q is empty", name)
	}
	if err := verifyUnit(ctx, name, contents); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := os.MkdirAll(systemUnitDirectory, 0o755); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("systemd: create unit directory: %w", err))
	}
	target := filepath.Join(systemUnitDirectory, name)
	previous, previousMode, existed, err := readExistingUnit(target)
	if err != nil {
		return err
	}
	if err := writeAtomicFile(target, []byte(contents), 0o644); err != nil {
		return err
	}
	if err := daemonReload(ctx); err == nil {
		return nil
	} else {
		reloadErr := err
		rollbackErr := restoreUnit(target, previous, previousMode, existed)
		if rollbackErr == nil && ctx.Err() == nil {
			rollbackErr = daemonReload(ctx)
		}
		if rollbackErr != nil {
			return errs.Wrap(errs.KindInternal, errors.Join(reloadErr,
				fmt.Errorf("systemd: rollback failed: %w", rollbackErr)))
		}
		return reloadErr
	}
}

func verifyUnit(ctx context.Context, name, contents string) error {
	directory, err := os.MkdirTemp("", "groundplane-unit-")
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("systemd: create verification directory: %w", err))
	}
	defer os.RemoveAll(directory)
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("systemd: write unit for verification: %w", err))
	}

	cmd := exec.CommandContext(ctx, "systemd-analyze", "verify", path)
	output, err := cmd.CombinedOutput()
	if err == nil {
		return nil
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("systemd: execute unit validator: %w", err))
	}
	return errs.Newf(errs.KindValidationFailed, "systemd: invalid unit %q: %s", name, commandDetail(output, exitErr))
}

func daemonReload(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "systemctl", "daemon-reload")
	output, err := cmd.CombinedOutput()
	if err == nil {
		return nil
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	return errs.Wrap(errs.KindInternal, fmt.Errorf("systemd: daemon-reload: %s", commandDetail(output, err)))
}

func readExistingUnit(path string) ([]byte, fs.FileMode, bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, 0, false, nil
	}
	if err != nil {
		return nil, 0, false, errs.Wrap(errs.KindInternal, fmt.Errorf("systemd: inspect existing unit: %w", err))
	}
	if !info.Mode().IsRegular() {
		return nil, 0, false, errs.New(errs.KindInternal, "systemd: existing managed unit is not a regular file")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, false, errs.Wrap(errs.KindInternal, fmt.Errorf("systemd: read existing unit: %w", err))
	}
	return contents, info.Mode().Perm(), true, nil
}

func restoreUnit(path string, contents []byte, mode fs.FileMode, existed bool) error {
	if existed {
		return writeAtomicFile(path, contents, mode)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("systemd: remove failed unit: %w", err))
	}
	return syncDirectory(filepath.Dir(path))
}

func writeAtomicFile(path string, contents []byte, mode fs.FileMode) error {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".groundplane-unit-")
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("systemd: create temporary unit: %w", err))
	}
	temporaryPath := temporary.Name()
	keepTemporary := true
	defer func() {
		_ = temporary.Close()
		if keepTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(mode); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("systemd: set unit mode: %w", err))
	}
	if _, err := temporary.Write(contents); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("systemd: write temporary unit: %w", err))
	}
	if err := temporary.Sync(); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("systemd: sync temporary unit: %w", err))
	}
	if err := temporary.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("systemd: close temporary unit: %w", err))
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("systemd: replace unit: %w", err))
	}
	keepTemporary = false
	return syncDirectory(directory)
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("systemd: open unit directory: %w", err))
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("systemd: sync unit directory: %w", err))
	}
	return nil
}

func commandDetail(output []byte, fallback error) string {
	const limit = 4096
	detail := strings.TrimSpace(string(output))
	if detail == "" {
		detail = fallback.Error()
	}
	if len(detail) > limit {
		detail = detail[:limit] + "..."
	}
	return detail
}

// SelfUpdate performs the Controller's staged-binary swap: write the new
// binary alongside the running one, validate it (e.g. `--version`),
// exec into it, falling back to the old binary on any failure. This is
// the ONE systemd unit that never becomes a container (see mvp.md) — its
// own recovery is systemd's job (docker.service restarts itself; the
// Controller surviving docker death is why it's a systemd unit at all).
//
// This handoff is ALSO a documented os/exec exception (section 5, "the
// staged-binary swap handoff") — the exec() call that replaces the
// running process can't itself go through a Runner it's in the process
// of superseding.
func SelfUpdate(ctx context.Context, newBinaryPath string) error {
	return errs.New(errs.KindNotImplemented, "systemd: not implemented")
}
