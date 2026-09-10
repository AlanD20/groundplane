package controllerrelease

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumJournalBytes = 16384

// Current reads one atomically published snapshot; it never creates state.
func (store *Store) Current(ctx context.Context) (upgrade.Journal, bool, error) {
	raw, err := store.read(ctx, store.root, "journal.json", maximumJournalBytes)
	if errors.Is(err, fs.ErrNotExist) {
		return upgrade.Journal{}, false, nil
	}
	if err != nil {
		return upgrade.Journal{}, false, err
	}
	journal, err := jcs.Decode[upgrade.Journal](raw)
	if err != nil {
		return upgrade.Journal{}, false, err
	}
	if err := journal.Validate(); err != nil {
		return upgrade.Journal{}, false, err
	}
	return journal, true, nil
}

// Advance is a compare-and-swap over the closed recovery state graph. An exact
// replay is accepted; a stale task, incompatible transition or race is rejected.
func (store *Store) Advance(
	ctx context.Context,
	taskID string,
	from, to upgrade.Phase,
) (upgrade.Journal, error) {
	unlock, err := store.lock(ctx)
	if err != nil {
		return upgrade.Journal{}, err
	}
	defer unlock()
	current, found, err := store.Current(ctx)
	if err != nil {
		return upgrade.Journal{}, err
	}
	if !found || current.TaskID != taskID || !from.CanAdvance(to) {
		return upgrade.Journal{}, phaseConflict()
	}
	if current.Phase == to {
		return current, nil
	}
	if current.Phase != from {
		return upgrade.Journal{}, phaseConflict()
	}
	current.Phase = to
	if err := store.writeJournal(ctx, current); err != nil {
		return upgrade.Journal{}, err
	}
	return current, nil
}

func (store *Store) writeJournal(ctx context.Context, journal upgrade.Journal) error {
	if err := journal.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(journal)
	if err != nil {
		return fileError(err)
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return err
	}
	if len(canonical) > maximumJournalBytes {
		return unsafeFile()
	}
	return store.atomicWrite(ctx, store.root, "journal.json", 0o400, func(output io.Writer) error {
		_, err := output.Write(canonical)
		return fileError(err)
	})
}

// lock serializes the short journal/filesystem transactions across Controller,
// startup guard and watchdog processes. It is never held across systemctl or
// readiness waits, which may need another process to read/advance the journal.
func (store *Store) lock(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := openLeaf(ctx, store.root, "journal.lock", unix.O_RDWR|unix.O_CREAT, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close() // Cleanup only after failed stat.
		return nil, fileError(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || stat.Uid != store.uid ||
		stat.Nlink != 1 {
		_ = file.Close() // Cleanup only after rejecting an unsafe lock file.
		return nil, unsafeFile()
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return func() {
				_ = file.Close() // Closing the private descriptor releases its flock, including process exit.
			}, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EINTR) {
			_ = file.Close() // Cleanup after lock failure.
			return nil, fileError(err)
		}
		select {
		case <-ctx.Done():
			_ = file.Close() // No lock was acquired; release only this descriptor.
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func phaseConflict() error {
	return errs.New(errs.KindStateConflict, "controller activation authority or phase changed")
}

func openLeaf(
	ctx context.Context,
	root *os.Root,
	name string,
	flags int,
	mode uint32,
) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	directory, err := root.Open(".")
	if err != nil {
		return nil, fileError(err)
	}
	defer directory.Close()
	fd, err := unix.Openat(
		int(directory.Fd()),
		name,
		flags|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC,
		mode,
	)
	if err != nil {
		return nil, fileError(err)
	}
	return os.NewFile(uintptr(fd), name), nil
}
