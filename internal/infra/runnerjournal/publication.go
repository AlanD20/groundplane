package runnerjournal

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

func currentBootID(ctx context.Context) (string, error) {
	if ctx == nil {
		return "", corruptJournal()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	value, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	bootID := strings.TrimSpace(string(value))
	if !validBootID(bootID) {
		return "", corruptJournal()
	}
	return bootID, nil
}

func validBootID(value string) bool {
	return len(value) == 36 && value[8] == '-' && value[13] == '-' && value[18] == '-' && value[23] == '-' &&
		validLowerHex(strings.ReplaceAll(value, "-", ""), 32)
}

func (journal *Journal) append(ctx context.Context, directory string,
	progress runnerallocation.RunnerRuntimeProgress,
) (runnerallocation.RunnerRuntimeProgress, error) {
	bootID, err := journal.bootID(ctx)
	if err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, err
	}
	if bootID != progress.BootID {
		return runnerallocation.RunnerRuntimeProgress{}, errs.New(errs.KindStateConflict, "runner journal boot changed")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, errs.Wrap(errs.KindInternal, err)
	}
	defer root.Close()
	lock, err := root.Open(".")
	if err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, errs.Wrap(errs.KindInternal, err)
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, errs.Wrap(errs.KindStateConflict, err)
	}
	// Exclusively publishing a filename alone does not reject an altered caller
	// snapshot. Compare the transition with the actual committed predecessor.
	previous, exists, err := readLongestChain(directory)
	if err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, err
	}
	if (!exists && progress.Revision != -1) || (exists && progress.Revision != previous.Revision) {
		return runnerallocation.RunnerRuntimeProgress{}, errs.New(
			errs.KindStateConflict,
			"runner journal revision changed",
		)
	}
	progress.Revision++
	if progress.Revision >= maximumJournalRevisions {
		return runnerallocation.RunnerRuntimeProgress{}, errs.New(errs.KindResourceInUse, "runner journal is exhausted")
	}
	if !exists {
		progress.PredecessorSHA256 = ""
	} else {
		value, err := readBounded(filepath.Join(directory, revisionName(previous.Revision)))
		if err != nil {
			return runnerallocation.RunnerRuntimeProgress{}, err
		}
		progress.PredecessorSHA256 = digestBytes(value)
		clear(value)
	}
	if !validTransition(previous, progress, exists) {
		return runnerallocation.RunnerRuntimeProgress{}, corruptJournal()
	}
	value, err := encodeRevision(progress)
	if err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, err
	}
	defer clear(value)
	if err := publishRevision(ctx, root, lock, revisionName(progress.Revision), value); err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, err
	}
	head, err := json.Marshal(headRecord{
		Revision: strconv.FormatInt(progress.Revision, 10), Digest: digestBytes(value),
	})
	if err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(head)
	stage := ".HEAD." + ids.NewULID()
	if err := writeStage(root, stage, head); err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, err
	}
	defer root.Remove(stage) // Advisory staging never authorizes a host effect.
	if err := root.Rename(stage, "HEAD"); err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, errs.Wrap(errs.KindInternal, err)
	}
	if err := lock.Sync(); err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, errs.Wrap(errs.KindInternal, err)
	}
	return progress, nil
}

func publishRevision(ctx context.Context, root *os.Root, directory *os.File, name string, value []byte) error {
	stage := ".revision." + ids.NewULID()
	if err := writeStage(root, stage, value); err != nil {
		return err
	}
	defer root.Remove(stage) // Partial unpublished data cannot become a revision.
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := root.Link(stage, name); err != nil {
		return errs.Wrap(errs.KindStateConflict, err)
	}
	if err := directory.Sync(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func writeStage(root *os.Root, name string, value []byte) error {
	file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	_, writeErr := file.Write(value)
	if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}
