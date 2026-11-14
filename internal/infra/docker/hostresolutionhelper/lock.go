package hostresolutionhelper

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

// The dhcpcd guard takes this same lock before deciding whether its DNS hook
// may run. Holding it through every resolver write closes the check/write race.
func withResolverLock(ctx context.Context, operation func() error) (result error) {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := ensureStateDirectory(); err != nil {
		return err
	}
	fd, err := unix.Open(
		StateDirectory+"/resolver.lock",
		unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_CLOEXEC,
		0o600,
	)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	file := os.NewFile(uintptr(fd), "host resolver lock")
	defer func() { result = errors.Join(result, file.Close()) }()
	var info unix.Stat_t
	if err := unix.Fstat(fd, &info); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if info.Mode&unix.S_IFMT != unix.S_IFREG || info.Uid != 0 || info.Nlink != 1 || info.Mode&0o022 != 0 {
		return errs.New(errs.KindStateConflict, "host resolver lock has unsafe ownership")
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return operation()
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			return errs.Wrap(errs.KindInternal, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
