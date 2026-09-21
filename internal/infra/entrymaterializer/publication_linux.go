package entrymaterializer

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
)

type publication struct {
	rootFD      int
	parentFD    int
	temporary   *temporary
	destination string
	remove      bool
	missing     bool
	ops         linuxOps
}

func (publication *publication) publish(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return publication.abort(context.WithoutCancel(ctx), err)
	}
	if publication.remove {
		return publication.publishRemoval(ctx)
	}
	if err := publication.ops.renameat(
		publication.parentFD,
		publication.temporary.name,
		publication.parentFD,
		publication.destination,
	); err != nil {
		return publication.abort(
			context.WithoutCancel(ctx),
			wrapSystemError("publish temporary", err),
		)
	}
	publication.temporary.present = false
	result := error(nil)
	if err := publication.ops.fsync(publication.parentFD); err != nil {
		result = wrapSystemError("sync published destination", err)
	} else {
		result = ctx.Err()
	}
	return closeOperationDescriptors(
		context.WithoutCancel(ctx),
		publication.ops,
		publication.rootFD,
		publication.parentFD,
		result,
	)
}

func (publication *publication) publishRemoval(ctx context.Context) error {
	if publication.missing {
		return closeBeforeReturn(
			context.WithoutCancel(ctx), publication.ops, publication.rootFD, ctx.Err(), "close removal root",
		)
	}
	removed := false
	err := publication.ops.unlinkat(publication.parentFD, publication.destination, 0)
	if errors.Is(err, unix.ENOENT) {
		err = nil
	} else if err != nil {
		err = wrapSystemError("remove destination", err)
	} else {
		removed = true
	}
	result := finishDirectoryMutation(
		context.WithoutCancel(ctx), publication.ops, publication.parentFD, removed, err,
	)
	result = preferCleanupError(result, ctx.Err())
	return closeOperationDescriptors(
		context.WithoutCancel(ctx), publication.ops, publication.rootFD, publication.parentFD, result,
	)
}

func (publication *publication) abort(ctx context.Context, operationErr error) error {
	result := operationErr
	if publication.temporary != nil {
		result = preferCleanupError(
			operationErr,
			cleanupTemporary(ctx, publication.ops, publication.parentFD, publication.temporary),
		)
	}
	if publication.parentFD < 0 {
		return closeBeforeReturn(ctx, publication.ops, publication.rootFD, result, "close removal root")
	}
	return closeOperationDescriptors(
		ctx,
		publication.ops,
		publication.rootFD,
		publication.parentFD,
		result,
	)
}
