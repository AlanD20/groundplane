package controllerrelease

import (
	"context"
	"io"
	"os"
	"strings"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ExecutingDigest identifies this process's running inode even after an
// installation-path rename. The fixed kernel self reference is deliberate;
// untrusted release inputs still go through the no-symlink staging boundary.
func ExecutingDigest(ctx context.Context) (upgrade.Digest, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	file, err := os.Open("/proc/self/exe")
	if err != nil {
		return "", fileError(err)
	}
	defer file.Close()
	return copyDigest(ctx, io.Discard, file)
}

// Installed returns the current executable's actual digest, never a version label.
func (store *Store) Installed(ctx context.Context) (upgrade.Digest, error) {
	if store.binaries == nil {
		return "", errs.New(
			errs.KindStateConflict,
			"native controller upgrade bootstrap is not installed",
		)
	}
	file, err := store.openRegular(ctx, store.binaries, "controller", MaximumBinaryBytes)
	if err != nil {
		return "", err
	}
	defer file.Close()
	return copyDigest(ctx, io.Discard, file)
}

// Prepare retains the predecessor and installs its recovery guard before the
// journal can authorize any executable change. Failure leaves Controller intact.
func (store *Store) Prepare(ctx context.Context, journal upgrade.Journal) error {
	if err := journal.Validate(); err != nil {
		return err
	}
	if journal.Phase != upgrade.PhasePrepared || store.binaries == nil {
		return phaseConflict()
	}
	unlock, err := store.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	current, found, err := store.Current(ctx)
	if err != nil {
		return err
	}
	if found && current.TaskID == journal.TaskID {
		if !current.SameOperation(journal) {
			return phaseConflict()
		}
		return nil
	}
	if found && !current.Phase.Settled() {
		return errs.New(errs.KindResourceInUse, "a controller activation is already active")
	}
	manifest, err := store.Inspect(ctx, journal.Release)
	if err != nil {
		return err
	}
	if manifest != journal.Manifest {
		return phaseConflict()
	}
	if err := store.copyExecutable(ctx, store.binaries, "controller", store.root, "previous", journal.PreviousController); err != nil {
		return err
	}
	if err := store.copyExecutable(ctx, store.binaries, "controller", store.binaries, "controller-recovery", journal.PreviousController); err != nil {
		return err
	}
	return store.writeJournal(ctx, journal)
}

// Activate is replay-safe after either journal publication or executable rename.
// The watchdog stops the service first; this method never calls systemctl while
// holding a lock needed by the startup guard.
func (store *Store) Activate(ctx context.Context, taskID string) error {
	unlock, err := store.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	journal, found, err := store.Current(ctx)
	if err != nil {
		return err
	}
	if !found || journal.TaskID != taskID ||
		(journal.Phase != upgrade.PhaseActivating && journal.Phase != upgrade.PhaseTrial) {
		return phaseConflict()
	}
	installed, err := store.Installed(ctx)
	if err != nil {
		return err
	}
	if journal.Phase == upgrade.PhaseTrial && installed == journal.Manifest.ControllerSHA256 {
		return nil
	}
	if installed != journal.PreviousController {
		return phaseConflict()
	}
	manifest, err := store.Inspect(ctx, journal.Release)
	if err != nil {
		return err
	}
	if manifest != journal.Manifest {
		return phaseConflict()
	}
	root, err := store.releaseDirectory(ctx, journal.Release)
	if err != nil {
		return err
	}
	defer root.Close()
	journal.Phase = upgrade.PhaseTrial
	journal.TrialBootID = store.bootID
	if err := store.writeJournal(ctx, journal); err != nil {
		return err
	}
	return store.copyExecutable(
		ctx,
		root,
		"controller",
		store.binaries,
		"controller",
		manifest.ControllerSHA256,
	)
}

// Guard executes synchronously before the Controller binary. It authorizes one
// candidate attempt; any subsequent unfinished startup restores the predecessor.
func (store *Store) Guard(ctx context.Context, now time.Time) error {
	unlock, err := store.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	journal, found, err := store.Current(ctx)
	if err != nil || !found {
		return err
	}
	installed, err := store.Installed(ctx)
	if err != nil {
		return err
	}
	switch journal.Phase {
	case upgrade.PhaseStopping:
		// The watchdog has acquired a pending stop. Restore executable bytes
		// if needed, but do not let the starting predecessor qualify until
		// that stop has completed and the watchdog publishes RolledBack.
		return store.copyExecutable(
			ctx,
			store.root,
			"previous",
			store.binaries,
			"controller",
			journal.PreviousController,
		)
	case upgrade.PhaseHealthy:
		if installed != journal.Manifest.ControllerSHA256 {
			return phaseConflict()
		}
		return nil
	case upgrade.PhasePrepared,
		upgrade.PhaseCancelled,
		upgrade.PhaseRolledBack,
		upgrade.PhaseRecovered:
		if installed != journal.PreviousController {
			return phaseConflict()
		}
		return nil
	case upgrade.PhaseTrial:
		if installed == journal.Manifest.ControllerSHA256 &&
			now.Before(journal.CandidateDeadline()) &&
			journal.TrialBootID == store.bootID {
			journal.Phase = upgrade.PhaseStarting
			return store.writeJournal(ctx, journal)
		}
	}
	return store.rollbackLocked(ctx, journal)
}

func readBootID(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	file, err := os.Open("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", fileError(err)
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 38))
	if err != nil {
		return "", fileError(err)
	}
	value := strings.TrimSuffix(string(raw), "\n")
	if !upgrade.ValidBootID(value) {
		return "", errs.New(errs.KindInternal, "kernel boot identity is invalid")
	}
	return value, nil
}

// Rollback restores only the current operation's predecessor, never a release
// which has already qualified or been replaced by a newer operation.
func (store *Store) Rollback(ctx context.Context, taskID string) error {
	unlock, err := store.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	journal, found, err := store.Current(ctx)
	if err != nil {
		return err
	}
	if !found || journal.TaskID != taskID {
		return phaseConflict()
	}
	return store.rollbackLocked(ctx, journal)
}

func (store *Store) rollbackLocked(ctx context.Context, journal upgrade.Journal) error {
	if journal.Phase.Settled() {
		return phaseConflict()
	}
	if journal.Phase != upgrade.PhaseRollingBack && journal.Phase != upgrade.PhaseStopping &&
		journal.Phase != upgrade.PhaseRolledBack {
		if !journal.Phase.CanAdvance(upgrade.PhaseRollingBack) {
			return phaseConflict()
		}
		journal.Phase = upgrade.PhaseRollingBack
		if err := store.writeJournal(ctx, journal); err != nil {
			return err
		}
	}
	if err := store.copyExecutable(ctx, store.root, "previous", store.binaries, "controller", journal.PreviousController); err != nil {
		return err
	}
	journal.Phase = upgrade.PhaseRolledBack
	return store.writeJournal(ctx, journal)
}
