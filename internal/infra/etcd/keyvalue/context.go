package keyvalue

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ValidateContext(ctx context.Context) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "hierarchy context is required")
	}
	return ctx.Err()
}
