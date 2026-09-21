package entrymaterializer

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"io"
	"strings"
)

func (materializer *materializer) prepare(
	ctx context.Context,
	header entrymaterialization.Header,
	content io.Reader,
) (*publication, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	if header.TaskID() == "" {
		return nil, internalError("validated header is required")
	}
	if content == nil {
		return nil, internalError("content stream is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rootFD, err := materializer.duplicateRoot(ctx)
	if err != nil {
		return nil, err
	}
	if err := verifyDirectory(
		ctx,
		rootFD,
		materializer.helperUID,
		materializer.helperGID,
	); err != nil {
		return nil, closeBeforeReturn(
			context.WithoutCancel(ctx),
			materializer.ops,
			rootFD,
			err,
			"close rejected operation root",
		)
	}

	components := strings.Split(header.Destination(), "/")
	if header.OutputKind().Removes() {
		if err := verifyRemovalContent(ctx, materializer.ops, header, content); err != nil {
			return nil, closeBeforeReturn(
				context.WithoutCancel(ctx), materializer.ops, rootFD, err, "close rejected removal root",
			)
		}
		parentFD, missing, err := materializer.openExistingParents(ctx, rootFD, components[:len(components)-1])
		if err != nil {
			return nil, closeBeforeReturn(
				context.WithoutCancel(ctx), materializer.ops, rootFD, err, "close removal root after traversal failure",
			)
		}
		if missing {
			return &publication{
				rootFD: rootFD, parentFD: -1, destination: components[len(components)-1],
				remove: true, missing: true, ops: materializer.ops,
			}, nil
		}
		if err := materializer.reconcileOrphans(ctx, parentFD); err != nil {
			return nil, closeOperationDescriptors(
				context.WithoutCancel(ctx), materializer.ops, rootFD, parentFD, err,
			)
		}
		if err := inspectDestination(
			ctx, materializer.ops, parentFD, components[len(components)-1], header,
		); err != nil {
			return nil, closeOperationDescriptors(
				context.WithoutCancel(ctx), materializer.ops, rootFD, parentFD, err,
			)
		}
		return &publication{
			rootFD: rootFD, parentFD: parentFD, destination: components[len(components)-1],
			remove: true, ops: materializer.ops,
		}, nil
	}
	parentFD, err := materializer.ensureParents(ctx, rootFD, components[:len(components)-1])
	if err != nil {
		return nil, closeBeforeReturn(
			context.WithoutCancel(ctx),
			materializer.ops,
			rootFD,
			err,
			"close operation root after traversal failure",
		)
	}
	if err := materializer.reconcileOrphans(ctx, parentFD); err != nil {
		return nil, closeOperationDescriptors(
			context.WithoutCancel(ctx),
			materializer.ops,
			rootFD,
			parentFD,
			err,
		)
	}
	if err := inspectDestination(
		ctx,
		materializer.ops,
		parentFD,
		components[len(components)-1],
		header,
	); err != nil {
		return nil, closeOperationDescriptors(
			context.WithoutCancel(ctx),
			materializer.ops,
			rootFD,
			parentFD,
			err,
		)
	}
	temporaryFile, err := materializer.prepareTemporary(ctx, parentFD, header, content)
	if err != nil {
		return nil, closeOperationDescriptors(
			context.WithoutCancel(ctx),
			materializer.ops,
			rootFD,
			parentFD,
			err,
		)
	}
	return &publication{
		rootFD:      rootFD,
		parentFD:    parentFD,
		temporary:   temporaryFile,
		destination: components[len(components)-1],
		ops:         materializer.ops,
	}, nil
}
