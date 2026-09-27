package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const slotOwnerFile = ".groundplane-owner"

// claimSlot records allocation ownership before creating accounts or starting
// processes. An existing account without that claim is not ours to adopt.
func (operations *localOperations) claimSlot(ctx context.Context, plan corerunner.Plan) error {
	ownerPath := filepath.Join(plan.Paths.SlotRoot, slotOwnerFile)
	if _, err := os.Lstat(ownerPath); err == nil {
		return observeSlotClaim(plan)
	} else if !errors.Is(err, os.ErrNotExist) {
		return errs.Wrap(errs.KindInternal, err)
	}
	for _, database := range []string{"passwd", "group"} {
		result, err := operations.run(ctx, "getent", database, plan.Identity.User)
		if err == nil || result.ExitCode != 2 {
			return errs.New(errs.KindStateConflict, "Runner account exists without allocation ownership")
		}
	}
	if err := ensureOwnedDirectory(filepath.Dir(plan.Paths.SlotRoot), 0, 0, 0o755); err != nil {
		return err
	}
	if err := ensureOwnedDirectory(plan.Paths.SlotRoot, 0, plan.Identity.GID, 0o710); err != nil {
		return err
	}
	entries, err := os.ReadDir(plan.Paths.SlotRoot)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if len(entries) != 0 {
		return errs.New(errs.KindStateConflict, "Runner slot contains files without allocation ownership")
	}
	file, err := os.OpenFile(ownerPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return observeSlotClaim(plan)
	}
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	_, writeErr := file.WriteString(plan.IdentityDigest() + "\n")
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	directory, err := os.Open(plan.Paths.SlotRoot)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	syncErr = directory.Sync()
	closeErr = directory.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return observeSlotClaim(plan)
}

func observeSlotClaim(plan corerunner.Plan) error {
	if err := observeOwnedDirectory(plan.Paths.SlotRoot, 0, plan.Identity.GID, 0o710); err != nil {
		return err
	}
	ownerPath := filepath.Join(plan.Paths.SlotRoot, slotOwnerFile)
	info, err := os.Lstat(ownerPath)
	if err != nil {
		return errs.Wrap(errs.KindStateConflict, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() != int64(len(plan.IdentityDigest())+1) {
		return errs.New(errs.KindStateConflict, "Runner slot ownership changed")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 {
		return errs.New(errs.KindStateConflict, "Runner slot claim is not Controller-owned")
	}
	value, err := os.ReadFile(ownerPath)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if strings.TrimSuffix(string(value), "\n") != plan.IdentityDigest() {
		return errs.New(errs.KindStateConflict, "Runner slot belongs to another allocation")
	}
	return nil
}
