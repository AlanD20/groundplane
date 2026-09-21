package entrymaterializer

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
)

func (materializer *materializer) openExistingParents(
	ctx context.Context,
	rootFD int,
	components []string,
) (int, bool, error) {
	if err := contextError(ctx); err != nil {
		return -1, false, err
	}
	currentFD, err := unix.Dup(rootFD)
	if err != nil {
		return -1, false, wrapSystemError("duplicate removal parent root", err)
	}
	unix.CloseOnExec(currentFD)
	for _, component := range components {
		childFD, openErr := openDirectoryAt(ctx, materializer.ops, currentFD, component)
		if errors.Is(openErr, unix.ENOENT) {
			return -1, true, closeBeforeReturn(
				context.WithoutCancel(ctx), materializer.ops, currentFD, nil, "close missing removal parent",
			)
		}
		if openErr != nil {
			return -1, false, closeBeforeReturn(
				context.WithoutCancel(ctx), materializer.ops, currentFD,
				wrapSystemError("open removal parent", openErr), "close rejected removal parent",
			)
		}
		if err := verifyDirectory(ctx, childFD, materializer.helperUID, materializer.helperGID); err != nil {
			return -1, false, closePairBeforeReturn(
				context.WithoutCancel(ctx), materializer.ops, currentFD, childFD, err,
			)
		}
		if err := materializer.ops.close(currentFD); err != nil {
			return -1, false, closeBeforeReturn(
				context.WithoutCancel(ctx), materializer.ops, childFD,
				wrapSystemError("close traversed removal parent", err), "close removal child",
			)
		}
		currentFD = childFD
	}
	return currentFD, false, nil
}

func (materializer *materializer) duplicateRoot(ctx context.Context) (int, error) {
	if err := contextError(ctx); err != nil {
		return -1, err
	}
	materializer.lifecycleMu.Lock()
	defer materializer.lifecycleMu.Unlock()
	if materializer.rootFD < 0 {
		return -1, internalError("root descriptor is closed")
	}
	rootFD, err := unix.Dup(materializer.rootFD)
	if err != nil {
		return -1, wrapSystemError("duplicate operation root", err)
	}
	unix.CloseOnExec(rootFD)
	return rootFD, nil
}

func (materializer *materializer) ensureParents(
	ctx context.Context,
	rootFD int,
	components []string,
) (int, error) {
	if err := contextError(ctx); err != nil {
		return -1, err
	}
	currentFD, err := unix.Dup(rootFD)
	if err != nil {
		return -1, wrapSystemError("duplicate parent root", err)
	}
	unix.CloseOnExec(currentFD)
	cleanupCtx := context.WithoutCancel(ctx)
	for _, component := range components {
		if err := ctx.Err(); err != nil {
			return -1, closeBeforeReturn(
				cleanupCtx,
				materializer.ops,
				currentFD,
				err,
				"close cancelled parent",
			)
		}
		childFD, openErr := openDirectoryAt(ctx, materializer.ops, currentFD, component)
		created := false
		if errors.Is(openErr, unix.ENOENT) {
			if mkdirErr := unix.Mkdirat(currentFD, component, directoryMode); mkdirErr != nil {
				if !errors.Is(mkdirErr, unix.EEXIST) {
					return -1, closeBeforeReturn(
						cleanupCtx,
						materializer.ops,
						currentFD,
						wrapSystemError("create destination parent", mkdirErr),
						"close parent after create failure",
					)
				}
			} else {
				created = true
			}
			childFD, openErr = openDirectoryAt(ctx, materializer.ops, currentFD, component)
		}
		if openErr != nil {
			return -1, closeBeforeReturn(
				cleanupCtx,
				materializer.ops,
				currentFD,
				wrapSystemError("open destination parent", openErr),
				"close rejected parent",
			)
		}
		if created {
			if err := setCreatedDirectory(
				ctx,
				childFD,
				materializer.helperUID,
				materializer.helperGID,
			); err != nil {
				return -1, closePairBeforeReturn(
					cleanupCtx,
					materializer.ops,
					currentFD,
					childFD,
					err,
				)
			}
		}
		if err := verifyDirectory(
			ctx,
			childFD,
			materializer.helperUID,
			materializer.helperGID,
		); err != nil {
			return -1, closePairBeforeReturn(
				cleanupCtx,
				materializer.ops,
				currentFD,
				childFD,
				err,
			)
		}
		if created {
			if err := materializer.ops.fsync(currentFD); err != nil {
				return -1, closePairBeforeReturn(
					cleanupCtx,
					materializer.ops,
					currentFD,
					childFD,
					wrapSystemError("sync created parent", err),
				)
			}
		}
		if err := materializer.ops.close(currentFD); err != nil {
			return -1, closeBeforeReturn(
				cleanupCtx,
				materializer.ops,
				childFD,
				wrapSystemError("close traversed parent", err),
				"close child after parent close failure",
			)
		}
		currentFD = childFD
	}
	return currentFD, nil
}
