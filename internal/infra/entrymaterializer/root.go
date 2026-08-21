// Package entrymaterializer publishes one Controller-authorized environment
// file below the task-scoped helper bind.
package entrymaterializer

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const rootPath = "/run/groundplane/materialize"

func requireContext(ctx context.Context) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "entry materializer: context is required")
	}
	return nil
}

func contextError(ctx context.Context) error {
	if err := requireContext(ctx); err != nil {
		return err
	}
	return ctx.Err()
}

func internalError(message string) error {
	return errs.New(errs.KindInternal, "entry materializer: "+message)
}
