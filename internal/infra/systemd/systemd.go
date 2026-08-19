// Package systemd handles the host-level systemd surfaces: installing
// unit files (groundplane-controller.service, groundplane-agent.service,
// groundplane-etcd.service), the Controller's self-update via staged-
// binary swap, and validating backup-schedule calendar expressions with
// `systemd-analyze calendar`. See mvp.md, "Core (locked)" and
// "groundplane-etcd.service".
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
	"os/exec"

	"github.com/sample-tenant/groundplane/pkg/errs"
)

// ValidateCalendar shells out to `systemd-analyze calendar <expr>` to
// validate a backup policy's Frequency before it's saved.
func ValidateCalendar(ctx context.Context, expr string) error {
	cmd := exec.CommandContext(ctx, "systemd-analyze", "calendar", expr)
	if err := cmd.Run(); err != nil {
		return errs.Newf(errs.CodeValidationFailed, "systemd: invalid calendar expression: %s", expr)
	}
	return nil
}

// InstallUnit writes a unit file and runs `systemctl daemon-reload`.
// TODO: implement — used once, by the bootstrap install path, and again
// by `core component controller update`'s staged-binary swap.
func InstallUnit(ctx context.Context, name, contents string) error {
	return errs.New(errs.CodeNotImplemented, "systemd: not implemented")
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
	return errs.New(errs.CodeNotImplemented, "systemd: not implemented")
}
