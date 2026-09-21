package localagent

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func safePortError(ctx context.Context, err error, message string) error {
	if err == nil {
		return nil
	}
	if ctx != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
	}
	if kind, ok := errs.KindOf(err); ok {
		return errs.New(kind, message)
	}
	return errs.New(errs.KindInternal, message)
}
