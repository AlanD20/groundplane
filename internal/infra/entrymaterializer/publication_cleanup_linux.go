package entrymaterializer

import (
	"context"
	"errors"
	"fmt"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func cleanupTemporary(
	ctx context.Context,
	ops linuxOps,
	parentFD int,
	temporaryFile *temporary,
) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	var cleanupErrors []error
	if temporaryFile.open {
		if err := ops.close(temporaryFile.fd); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("close temporary: %w", err))
		}
		temporaryFile.open = false
	}
	if temporaryFile.present {
		if err := ops.unlinkat(parentFD, temporaryFile.name, 0); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("remove temporary: %w", err))
		} else {
			temporaryFile.present = false
			if err := ops.fsync(parentFD); err != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("sync temporary removal: %w", err))
			}
		}
	}
	if len(cleanupErrors) != 0 {
		return wrapSystemError("plaintext cleanup failed", errors.Join(cleanupErrors...))
	}
	return nil
}

func finishDirectoryMutation(
	ctx context.Context,
	ops linuxOps,
	parentFD int,
	mutated bool,
	operationErr error,
) error {
	if err := contextError(ctx); err != nil {
		return preferCleanupError(operationErr, err)
	}
	if mutated {
		if err := ops.fsync(parentFD); err != nil {
			return preferCleanupError(
				operationErr,
				wrapSystemError("sync orphan reconciliation", err),
			)
		}
	}
	return operationErr
}

func closeBeforeReturn(
	ctx context.Context,
	ops linuxOps,
	fd int,
	operationErr error,
	closeOperation string,
) error {
	if err := contextError(ctx); err != nil {
		return preferCleanupError(operationErr, err)
	}
	if err := ops.close(fd); err != nil {
		return preferCleanupError(operationErr, wrapSystemError(closeOperation, err))
	}
	return operationErr
}

func closePairBeforeReturn(
	ctx context.Context,
	ops linuxOps,
	firstFD int,
	secondFD int,
	operationErr error,
) error {
	result := closeBeforeReturn(
		ctx,
		ops,
		firstFD,
		operationErr,
		"close parent after traversal failure",
	)
	return closeBeforeReturn(ctx, ops, secondFD, result, "close child after traversal failure")
}

func closeOperationDescriptors(
	ctx context.Context,
	ops linuxOps,
	rootFD int,
	parentFD int,
	operationErr error,
) error {
	result := closeBeforeReturn(ctx, ops, parentFD, operationErr, "close destination directory")
	return closeBeforeReturn(ctx, ops, rootFD, result, "close operation root")
}

func preferCleanupError(operationErr, cleanupErr error) error {
	if cleanupErr == nil {
		return operationErr
	}
	if errors.Is(cleanupErr, context.Canceled) ||
		errors.Is(cleanupErr, context.DeadlineExceeded) {
		return internalError("mandatory cleanup failed")
	}
	if operationErr == nil {
		return cleanupErr
	}
	if errors.Is(operationErr, context.Canceled) ||
		errors.Is(operationErr, context.DeadlineExceeded) {
		return cleanupErr
	}
	return errs.Wrap(errs.KindInternal, errors.Join(cleanupErr, operationErr))
}

func wrapSystemError(operation string, err error) error {
	return errs.Wrap(errs.KindInternal, fmt.Errorf("entry materializer: %s: %w", operation, err))
}
