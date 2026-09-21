package s3compatible

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func reconciliationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
}

func isAmbiguousMutationFailure(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		classifyProviderFailure(err) == providerFailureUnavailable
}

func joinOperationCleanup(operationErr error, cleanupErr error) error {
	if operationErr == nil {
		return cleanupErr
	}
	if cleanupErr == nil {
		return operationErr
	}
	kind, ok := errs.KindOf(operationErr)
	if !ok {
		kind = errs.KindInternal
	}
	return errs.WrapJoined(kind, operationErr, cleanupErr)
}
