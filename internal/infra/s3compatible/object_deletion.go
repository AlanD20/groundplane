package s3compatible

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (adapter *Adapter) DeleteExact(ctx context.Context, object backupobject.Object) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := adapter.validateObject(object); err != nil {
		return err
	}
	head, err := adapter.HeadExact(ctx, object.Artifact, &object.Discriminator)
	if err != nil || !head.Present {
		return err
	}

	deleteOnce := func() error {
		input := &s3.DeleteObjectInput{
			Bucket: aws.String(adapter.config.Bucket),
			Key:    aws.String(object.Artifact.Key),
		}
		applyDiscriminatorToDelete(input, object.Discriminator)
		_, deleteErr := adapter.client.DeleteObject(ctx, input, oneAttempt)
		return deleteErr
	}
	deleteErr := deleteOnce()
	if deleteErr == nil {
		return adapter.requireAbsentAfterDelete(ctx, object, providerFailureRejected)
	}
	failure := classifyProviderFailure(deleteErr)
	if failure == providerFailureConflict {
		return conflictError()
	}
	if failure != providerFailureAbsent && failure != providerFailureUnavailable {
		return providerError(deleteErr)
	}

	reconciled, reconcileErr := adapter.HeadExact(ctx, object.Artifact, &object.Discriminator)
	if reconcileErr != nil {
		return reconcileErr
	}
	if !reconciled.Present {
		return nil
	}
	if failure == providerFailureAbsent {
		return rejectedError()
	}

	deleteErr = deleteOnce()
	if deleteErr == nil || classifyProviderFailure(deleteErr) == providerFailureAbsent {
		return adapter.requireAbsentAfterDelete(ctx, object, providerFailureRejected)
	}
	if classifyProviderFailure(deleteErr) == providerFailureConflict {
		return conflictError()
	}
	if classifyProviderFailure(deleteErr) == providerFailureUnavailable {
		return adapter.requireAbsentAfterDelete(ctx, object, providerFailureUnavailable)
	}
	return providerError(deleteErr)
}

func (adapter *Adapter) requireAbsentAfterDelete(
	ctx context.Context,
	object backupobject.Object,
	stillPresent providerFailure,
) error {
	head, err := adapter.HeadExact(ctx, object.Artifact, &object.Discriminator)
	if err != nil {
		return err
	}
	if !head.Present {
		return nil
	}
	return failureError(stillPresent)
}
