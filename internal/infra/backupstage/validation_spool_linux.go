//go:build linux

package backupstage

import (
	"context"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// ValidationSpool is a stage-owned O_TMPFILE, never a pathname or a recovery
// mutation. Writes consume the same lease's bounded capacity as its source and
// ciphertext. Sync seals it; Close and stage suspension remove its plaintext.
type ValidationSpool struct {
	stage    *Stage
	ctx      context.Context
	file     *os.File
	maximum  uint64
	written  uint64
	sealed   bool
	closed   bool
	closeErr error
}

func (stage *Stage) CreateValidationSpool(ctx context.Context, maximum uint64) (*ValidationSpool, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if stage == nil || maximum == 0 {
		return nil, validationError("validation spool bound is required")
	}
	stage.mu.Lock()
	defer stage.mu.Unlock()
	if stage.closed || stage.cleaned || stage.poisoned || stage.capacity.kind != capacityBounded ||
		maximum > stage.reservationRemaining || len(stage.validationSpools) != 0 {
		return nil, stateConflictError("validation spool lacks an active bounded staging lease")
	}
	fd, err := stage.owner.ops.openat2(stage.pointFD, ".", &unix.OpenHow{
		Flags: uint64(unix.O_TMPFILE | unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW),
		Mode:  uint64(fileMode), Resolve: descendantResolvePolicy,
	})
	if err != nil {
		return nil, storageSystemError("create private validation spool", err)
	}
	fail := func(primary error) (*ValidationSpool, error) {
		return nil, joinPrivate(primary, rawOperationError("close failed validation spool", unix.Close(fd)))
	}
	if err := unix.Fchown(fd, 0, 0); err != nil {
		return fail(systemError("set validation spool ownership", err))
	}
	if err := unix.Fchmod(fd, fileMode); err != nil {
		return fail(systemError("set validation spool permissions", err))
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return fail(systemError("inspect validation spool", err))
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 0 || stat.Size != 0 ||
		stat.Uid != 0 || stat.Gid != 0 || stat.Mode&0o7777 != fileMode {
		return fail(validationError("validation spool metadata is invalid"))
	}
	file := os.NewFile(uintptr(fd), "backup-validation-spool")
	if file == nil {
		return fail(internalError("wrap validation spool descriptor"))
	}
	spool := &ValidationSpool{stage: stage, ctx: ctx, file: file, maximum: maximum}
	if stage.validationSpools == nil {
		stage.validationSpools = make(map[*ValidationSpool]struct{})
	}
	stage.validationSpools[spool] = struct{}{}
	return spool, nil
}

func (spool *ValidationSpool) Write(content []byte) (int, error) {
	spool.stage.mu.Lock()
	defer spool.stage.mu.Unlock()
	if err := spool.available(); err != nil {
		return 0, err
	}
	if spool.sealed || uint64(len(content)) > spool.maximum-spool.written ||
		uint64(len(content)) > spool.stage.reservationRemaining {
		return 0, stateConflictError("validation spool write exceeds its sealed staging bound")
	}
	count, err := spool.stage.owner.ops.write(int(spool.file.Fd()), content)
	if count < 0 || count > len(content) {
		spool.stage.poisoned = true
		return 0, internalError("validation spool write returned an invalid count")
	}
	spool.written += uint64(count)
	reservationErr := spool.stage.consumeReservation(spool.ctx, uint64(count))
	if err != nil || count != len(content) || reservationErr != nil {
		spool.stage.poisoned = true
		if err == nil && count != len(content) {
			err = io.ErrShortWrite
		}
		return count, joinPrivate(rawOperationError("write validation spool", err), reservationErr)
	}
	return count, nil
}

func (spool *ValidationSpool) ReadAt(content []byte, offset int64) (int, error) {
	spool.stage.mu.Lock()
	defer spool.stage.mu.Unlock()
	if err := spool.available(); err != nil {
		return 0, err
	}
	return spool.file.ReadAt(content, offset)
}

// Validation spools are append-only before Sync and immutable after it.
func (spool *ValidationSpool) WriteAt([]byte, int64) (int, error) {
	return 0, stateConflictError("validation spool cannot be rewritten")
}

func (spool *ValidationSpool) Stat() (os.FileInfo, error) {
	spool.stage.mu.Lock()
	defer spool.stage.mu.Unlock()
	if err := spool.available(); err != nil {
		return nil, err
	}
	return spool.file.Stat()
}

func (spool *ValidationSpool) Sync() error {
	spool.stage.mu.Lock()
	defer spool.stage.mu.Unlock()
	if spool.closed || spool.file == nil {
		return systemError("sync closed validation spool", os.ErrClosed)
	}
	if err := spool.stage.owner.ops.fsync(int(spool.file.Fd())); err != nil {
		return storageSystemError("sync validation spool", err)
	}
	spool.sealed = true
	return nil
}

// Truncate is cleanup-only and works after cancellation.
func (spool *ValidationSpool) Truncate(size int64) error {
	spool.stage.mu.Lock()
	defer spool.stage.mu.Unlock()
	if size != 0 {
		return stateConflictError("validation spool may only be truncated for cleanup")
	}
	if spool.closed || spool.file == nil {
		return systemError("truncate closed validation spool", os.ErrClosed)
	}
	return rawOperationError("truncate validation spool", spool.file.Truncate(0))
}

func (spool *ValidationSpool) Close() error {
	spool.stage.mu.Lock()
	defer spool.stage.mu.Unlock()
	return spool.closeLocked()
}

func (spool *ValidationSpool) closeLocked() error {
	if spool.closed {
		return spool.closeErr
	}
	spool.closed = true
	spool.closeErr = joinPrivate(
		rawOperationError("clear validation spool", spool.file.Truncate(0)),
		storageOperationError("sync cleared validation spool", spool.stage.owner.ops.fsync(int(spool.file.Fd()))),
		rawOperationError("close validation spool", spool.file.Close()),
	)
	spool.file = nil
	delete(spool.stage.validationSpools, spool)
	return spool.closeErr
}

func (spool *ValidationSpool) available() error {
	if err := contextError(spool.ctx); err != nil {
		return err
	}
	if spool.closed || spool.file == nil || spool.stage.closed || spool.stage.cleaned || spool.stage.poisoned {
		return stateConflictError("validation spool is not available")
	}
	return nil
}
