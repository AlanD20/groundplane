//go:build !linux

package entrymaterializer

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
)

func Verify(ctx context.Context, source io.ReadCloser, limits entrymaterialization.Limits) error {
	return Run(ctx, source, limits)
}
