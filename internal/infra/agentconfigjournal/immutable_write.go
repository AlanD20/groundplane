package agentconfigjournal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"

	"golang.org/x/sys/unix"
)

// writeImmutable completes only the exact expected bytes. A crash-created
// .next prefix is compared before appending, never truncated or discarded.
// The caller derives raw from the same sealed authority and validated replay.
func (journal *Journal) writeImmutable(ctx context.Context, final, next string, raw []byte, limit int) error {
	if current, err := journal.readRawLimit(ctx, final, limit); err == nil {
		if !bytes.Equal(current, raw) {
			return conflict("Agent Config journal committed bytes conflict")
		}
		return fileError(journal.directory.Sync())
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	file, err := journal.openLeaf(ctx, next, unix.O_RDWR, 0, limit)
	if errors.Is(err, fs.ErrNotExist) {
		file, err = journal.openLeaf(ctx, next, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL, 0o600, limit)
	}
	if err != nil {
		return err
	}
	current, readErr := io.ReadAll(io.LimitReader(file, int64(limit)+1))
	if readErr != nil || len(current) > len(raw) || !bytes.Equal(current, raw[:len(current)]) {
		closeErr := file.Close()
		if readErr != nil || closeErr != nil {
			return fileError(errors.Join(readErr, closeErr))
		}
		return conflict("Agent Config journal uncommitted prefix conflicts")
	}
	if err := ctx.Err(); err != nil {
		_ = file.Close()
		return err
	} // Close after cancellation preserves the prefix.
	tail := raw[len(current):]
	written, writeErr := file.WriteAt(tail, int64(len(current)))
	if writeErr == nil && written != len(tail) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		return fileError(errors.Join(writeErr, closeErr))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := unix.Renameat2(int(journal.directory.Fd()), next, int(journal.directory.Fd()), final, unix.RENAME_NOREPLACE); err != nil {
		return fileError(err)
	}
	return fileError(journal.directory.Sync())
}
