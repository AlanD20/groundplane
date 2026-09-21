package entrymaterializer

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"golang.org/x/sys/unix"
	"os"
	"strings"
)

func (materializer *materializer) reconcileOrphans(ctx context.Context, parentFD int) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	readFD, err := openDirectoryAt(ctx, materializer.ops, parentFD, ".")
	if err != nil {
		return wrapSystemError("open destination directory for reconciliation", err)
	}
	directory := os.NewFile(uintptr(readFD), "entry-materializer-directory")
	entries, readErr := directory.ReadDir(-1)
	closeErr := directory.Close()
	var operationErr error
	if readErr != nil {
		operationErr = wrapSystemError("read destination directory", readErr)
	}
	var cleanupErr error
	if closeErr != nil {
		cleanupErr = wrapSystemError("close reconciliation directory", closeErr)
	}
	if result := preferCleanupError(operationErr, cleanupErr); result != nil {
		return result
	}

	removed := false
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), entrymaterialization.TemporaryPrefix) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return finishDirectoryMutation(
				context.WithoutCancel(ctx),
				materializer.ops,
				parentFD,
				removed,
				err,
			)
		}
		if err := inspectOrphan(ctx, materializer.ops, parentFD, entry.Name()); err != nil {
			return finishDirectoryMutation(
				context.WithoutCancel(ctx),
				materializer.ops,
				parentFD,
				removed,
				err,
			)
		}
		if err := materializer.ops.unlinkat(parentFD, entry.Name(), 0); err != nil {
			return finishDirectoryMutation(
				context.WithoutCancel(ctx),
				materializer.ops,
				parentFD,
				removed,
				wrapSystemError("remove orphaned temporary", err),
			)
		}
		removed = true
	}
	return finishDirectoryMutation(
		context.WithoutCancel(ctx),
		materializer.ops,
		parentFD,
		removed,
		ctx.Err(),
	)
}

func inspectOrphan(ctx context.Context, ops linuxOps, parentFD int, name string) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	fd, err := openPathAt(ctx, ops, parentFD, name)
	if err != nil {
		return wrapSystemError("open orphaned temporary", err)
	}
	var stat unix.Stat_t
	statErr := unix.Fstat(fd, &stat)
	closeErr := ops.close(fd)
	var operationErr error
	if statErr != nil {
		operationErr = wrapSystemError("inspect orphaned temporary", statErr)
	}
	var cleanupErr error
	if closeErr != nil {
		cleanupErr = wrapSystemError("close orphaned temporary", closeErr)
	}
	if result := preferCleanupError(operationErr, cleanupErr); result != nil {
		return result
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 {
		return internalError("orphaned temporary is unsafe")
	}
	return nil
}
