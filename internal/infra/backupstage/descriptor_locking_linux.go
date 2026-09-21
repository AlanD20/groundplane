package backupstage

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
	"math"
	"time"
)

func lockWithContext(ctx context.Context, fd int, ops linuxOperations) error {
	backoff := time.Millisecond
	for {
		if err := contextError(ctx); err != nil {
			return err
		}
		if err := ops.flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if !flockWouldBlock(err) {
				return systemError("lock capacity reservation root", err)
			}
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			if backoff < 100*time.Millisecond {
				backoff *= 2
			}
			continue
		}
		return nil
	}
}

func unlockWithPrimary(ctx context.Context, primary error, fd int, ops linuxOperations) error {
	unlockErr := rawOperationError("unlock capacity reservation root", ops.flock(fd, unix.LOCK_UN))
	if unlockErr == nil {
		return primary
	}
	if primary == nil {
		return joinPrivate(unlockErr)
	}
	return errs.WrapJoined(errs.KindInternal, primary, unlockErr)
}

func closeAndUnlock(ctx context.Context, primary error, rootFD int, ops linuxOperations, fds ...int) error {
	closeErr := closeFDs(ctx, fds...)
	unlockErr := rawOperationError("unlock capacity reservation root", ops.flock(rootFD, unix.LOCK_UN))
	if unlockErr != nil {
		if primary == nil {
			return joinPrivate(unlockErr, closeErr)
		}
		return errs.WrapJoined(errs.KindInternal, primary, closeErr, unlockErr)
	}
	return joinPrivate(primary, closeErr)
}

func addUint64(left, right uint64) (uint64, bool) {
	if right > math.MaxUint64-left {
		return 0, true
	}
	return left + right, false
}

func flockWouldBlock(err error) bool {
	return errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN)
}
