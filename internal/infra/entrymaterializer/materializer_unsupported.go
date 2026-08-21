//go:build !linux

package entrymaterializer

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
)

type materializer struct{}

func Run(ctx context.Context, source io.ReadCloser, limits entrymaterialization.Limits) error {
	_, err := entrymaterialization.Decode(
		ctx,
		source,
		limits,
		func(context.Context, entrymaterialization.Header, io.Reader) error {
			return internalError("linux openat2 support is required")
		},
	)
	return err
}

func openMaterializer(ctx context.Context) (*materializer, error) {
	if err := requireContext(ctx); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, internalError("linux openat2 support is required")
}

func (*materializer) close(ctx context.Context) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return internalError("linux openat2 support is required")
}

func (*materializer) materialize(
	ctx context.Context,
	_ entrymaterialization.Header,
	_ io.Reader,
) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return internalError("linux openat2 support is required")
}
