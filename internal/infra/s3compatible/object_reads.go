package s3compatible

import (
	"bytes"
	"context"
	"crypto/sha256"
	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
)

func completedObject(
	artifact backupobject.Artifact,
	output *s3.CompleteMultipartUploadOutput,
) (backupobject.Object, error) {
	if output == nil {
		return backupobject.Object{}, rejectedError()
	}
	discriminator, err := outputDiscriminator(output.VersionId, output.ETag)
	if err != nil {
		return backupobject.Object{}, err
	}
	return backupobject.Object{Artifact: artifact, Discriminator: discriminator}, nil
}

func (adapter *Adapter) HeadExact(
	ctx context.Context,
	artifact backupobject.Artifact,
	expected *backupobject.Discriminator,
) (backupobject.HeadResult, error) {
	if err := ctx.Err(); err != nil {
		return backupobject.HeadResult{}, err
	}
	if err := adapter.validateArtifact(artifact); err != nil {
		return backupobject.HeadResult{}, err
	}
	input := &s3.HeadObjectInput{Bucket: aws.String(adapter.config.Bucket), Key: aws.String(artifact.Key)}
	if expected != nil {
		if err := expected.Validate(); err != nil {
			return backupobject.HeadResult{}, err
		}
		applyDiscriminatorToHead(input, *expected)
	}
	output, err := adapter.client.HeadObject(ctx, input)
	if err != nil {
		if classifyProviderFailure(err) == providerFailureAbsent {
			return backupobject.HeadResult{Present: false}, nil
		}
		return backupobject.HeadResult{}, providerError(err)
	}
	if output == nil {
		return backupobject.HeadResult{}, rejectedError()
	}
	discriminator, err := validateRemoteObject(
		artifact,
		expected,
		output.ContentLength,
		output.Metadata,
		output.VersionId,
		output.ETag,
	)
	if err != nil {
		return backupobject.HeadResult{}, err
	}
	return backupobject.HeadResult{
		Present: true,
		Object:  backupobject.Object{Artifact: artifact, Discriminator: discriminator},
	}, nil
}

func (adapter *Adapter) GetExact(
	ctx context.Context,
	object backupobject.Object,
	destination io.Writer,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := adapter.validateObject(object); err != nil {
		return err
	}
	if destination == nil {
		return errs.New(errs.KindValidationFailed, "s3 backup object destination is required")
	}
	input := &s3.GetObjectInput{Bucket: aws.String(adapter.config.Bucket), Key: aws.String(object.Artifact.Key)}
	applyDiscriminatorToGet(input, object.Discriminator)
	output, err := adapter.client.GetObject(ctx, input)
	if err != nil {
		if classifyProviderFailure(err) == providerFailureAbsent {
			return conflictError()
		}
		return providerError(err)
	}
	if output == nil || output.Body == nil {
		return rejectedError()
	}
	if _, err := validateRemoteObject(
		object.Artifact,
		&object.Discriminator,
		output.ContentLength,
		output.Metadata,
		output.VersionId,
		output.ETag,
	); err != nil {
		return closeObjectBody(ctx, output.Body, err)
	}
	hasher := sha256.New()
	limit := int64(object.Artifact.Evidence.StoredSizeBytes) + 1
	written, copyErr := io.Copy(io.MultiWriter(destination, hasher), io.LimitReader(output.Body, limit))
	if copyErr != nil {
		return closeObjectBody(ctx, output.Body, providerError(copyErr))
	}
	if uint64(written) != object.Artifact.Evidence.StoredSizeBytes ||
		!bytes.Equal(hasher.Sum(nil), object.Artifact.Evidence.StoredSHA256[:]) {
		return closeObjectBody(ctx, output.Body, conflictError())
	}
	return closeObjectBody(ctx, output.Body, nil)
}

func closeObjectBody(ctx context.Context, body io.Closer, operationErr error) error {
	closeErr := body.Close()
	if operationErr == nil && ctx.Err() != nil {
		operationErr = ctx.Err()
	}
	return joinOperationCleanup(operationErr, providerError(closeErr))
}
