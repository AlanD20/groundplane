package s3compatible

import (
	"context"
	"crypto/sha256"
	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"io"
)

func (adapter *Adapter) putMultipart(
	ctx context.Context,
	artifact backupobject.Artifact,
	source io.ReaderAt,
) (backupobject.Object, error) {
	for generation := range 2 {
		object, restart, err := adapter.putMultipartGeneration(ctx, artifact, source)
		if err != nil {
			return backupobject.Object{}, err
		}
		if !restart {
			return object, nil
		}
		if generation == 1 {
			return backupobject.Object{}, conflictError()
		}
	}
	return backupobject.Object{}, internalError()
}

func (adapter *Adapter) putMultipartGeneration(
	ctx context.Context,
	artifact backupobject.Artifact,
	source io.ReaderAt,
) (backupobject.Object, bool, error) {
	uploadID, err := adapter.createMultipart(ctx, artifact)
	if err != nil {
		return backupobject.Object{}, false, err
	}
	parts, err := adapter.uploadParts(ctx, artifact, source, uploadID)
	if err != nil {
		cleanupCtx, cancel := reconciliationContext(ctx)
		cleanupErr := adapter.cleanupExactUploads(cleanupCtx, artifact.Key)
		cancel()
		return backupobject.Object{}, false, joinOperationCleanup(err, cleanupErr)
	}

	return adapter.completeMultipart(ctx, artifact, source, uploadID, parts)
}

func (adapter *Adapter) createMultipart(ctx context.Context, artifact backupobject.Artifact) (string, error) {
	create := func() (*s3.CreateMultipartUploadOutput, error) {
		return adapter.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket:   aws.String(adapter.config.Bucket),
			Key:      aws.String(artifact.Key),
			Metadata: artifact.Metadata(),
		}, oneAttempt)
	}
	output, err := create()
	if err != nil && isAmbiguousMutationFailure(err) {
		cleanupCtx, cancel := reconciliationContext(ctx)
		cleanupErr := adapter.cleanupExactUploads(cleanupCtx, artifact.Key)
		cancel()
		if cleanupErr != nil {
			return "", joinOperationCleanup(providerError(err), cleanupErr)
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		output, err = create()
		if err != nil && isAmbiguousMutationFailure(err) {
			cleanupCtx, cancel = reconciliationContext(ctx)
			cleanupErr = adapter.cleanupExactUploads(cleanupCtx, artifact.Key)
			cancel()
			return "", joinOperationCleanup(providerError(err), cleanupErr)
		}
	}
	if err != nil {
		if classifyProviderFailure(err) == providerFailureAbsent {
			return "", rejectedError()
		}
		return "", providerError(err)
	}
	if output == nil || output.UploadId == nil || *output.UploadId == "" {
		return "", rejectedError()
	}
	return *output.UploadId, nil
}

func (adapter *Adapter) uploadParts(
	ctx context.Context,
	artifact backupobject.Artifact,
	source io.ReaderAt,
	uploadID string,
) ([]types.CompletedPart, error) {
	partSize := multipartPartSize(artifact.Evidence.StoredSizeBytes)
	partCount := (artifact.Evidence.StoredSizeBytes + partSize - 1) / partSize
	if partCount == 0 || partCount > maximumParts {
		return nil, errs.New(errs.KindValidationFailed, "s3 backup object multipart part count is invalid")
	}
	completed := make([]types.CompletedPart, 0, partCount)
	storedHash := sha256.New()
	for partIndex := uint64(0); partIndex < partCount; partIndex++ {
		offset := partIndex * partSize
		length := min(partSize, artifact.Evidence.StoredSizeBytes-offset)
		contentMD5, _, err := digestRange(ctx, source, offset, length, storedHash)
		if err != nil {
			return nil, err
		}
		partNumber := int32(partIndex + 1)
		output, err := adapter.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:        aws.String(adapter.config.Bucket),
			Key:           aws.String(artifact.Key),
			UploadId:      aws.String(uploadID),
			PartNumber:    aws.Int32(partNumber),
			Body:          io.NewSectionReader(source, int64(offset), int64(length)),
			ContentLength: aws.Int64(int64(length)),
			ContentMD5:    aws.String(contentMD5),
		})
		if err != nil {
			if classifyProviderFailure(err) == providerFailureAbsent {
				return nil, conflictError()
			}
			return nil, providerError(err)
		}
		if output == nil || output.ETag == nil || *output.ETag == "" {
			return nil, rejectedError()
		}
		completed = append(completed, types.CompletedPart{ETag: output.ETag, PartNumber: aws.Int32(partNumber)})
	}
	var actual [sha256.Size]byte
	copy(actual[:], storedHash.Sum(nil))
	if actual != artifact.Evidence.StoredSHA256 {
		return nil, errs.New(errs.KindValidationFailed, "s3 backup object source digest is invalid")
	}
	return completed, nil
}

func (adapter *Adapter) completeMultipart(
	ctx context.Context,
	artifact backupobject.Artifact,
	source io.ReaderAt,
	uploadID string,
	parts []types.CompletedPart,
) (backupobject.Object, bool, error) {
	complete := func() (*s3.CompleteMultipartUploadOutput, error) {
		size := int64(artifact.Evidence.StoredSizeBytes)
		return adapter.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:          aws.String(adapter.config.Bucket),
			Key:             aws.String(artifact.Key),
			UploadId:        aws.String(uploadID),
			IfNoneMatch:     aws.String("*"),
			MpuObjectSize:   aws.Int64(size),
			MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
		}, oneAttempt)
	}
	output, err := complete()
	if err == nil {
		object, outputErr := completedObject(artifact, output)
		if outputErr == nil {
			return object, false, nil
		}
		return adapter.resolveFinalComplete(ctx, artifact, outputErr)
	}

	failure := classifyProviderFailure(err)
	if failure == providerFailureConflict || failure == providerFailureAbsent {
		return adapter.resolveFinalComplete(ctx, artifact, err)
	}
	if failure != providerFailureUnavailable {
		cleanupCtx, cancel := reconciliationContext(ctx)
		cleanupErr := adapter.cleanupExactUploads(cleanupCtx, artifact.Key)
		cancel()
		return backupobject.Object{}, false, joinOperationCleanup(providerError(err), cleanupErr)
	}
	if ctx.Err() != nil {
		return adapter.resolveFinalComplete(ctx, artifact, err)
	}
	head, headErr := adapter.HeadExact(ctx, artifact, nil)
	if headErr != nil || head.Present {
		return adapter.resolveFinalCompleteAfterHead(ctx, artifact, err, head, headErr)
	}

	exists, existsErr := adapter.uploadExists(ctx, artifact.Key, uploadID)
	if existsErr != nil {
		return adapter.resolveFinalCompleteAfterHead(ctx, artifact, err, head, nil)
	}
	if !exists {
		return adapter.resolveFinalCompleteAfterHead(ctx, artifact, err, head, nil)
	}
	output, err = complete()
	if err == nil {
		object, outputErr := completedObject(artifact, output)
		if outputErr == nil {
			return object, false, nil
		}
		return adapter.resolveFinalComplete(ctx, artifact, outputErr)
	}
	if isAmbiguousMutationFailure(err) || classifyProviderFailure(err) == providerFailureConflict ||
		classifyProviderFailure(err) == providerFailureAbsent {
		return adapter.resolveFinalComplete(ctx, artifact, err)
	}
	cleanupCtx, cancel := reconciliationContext(ctx)
	cleanupErr := adapter.cleanupExactUploads(cleanupCtx, artifact.Key)
	cancel()
	return backupobject.Object{}, false, joinOperationCleanup(providerError(err), cleanupErr)
}

func (adapter *Adapter) resolveFinalComplete(
	ctx context.Context,
	artifact backupobject.Artifact,
	completionErr error,
) (backupobject.Object, bool, error) {
	reconcileCtx, cancel := reconciliationContext(ctx)
	head, headErr := adapter.HeadExact(reconcileCtx, artifact, nil)
	cancel()
	return adapter.resolveFinalCompleteAfterHead(ctx, artifact, completionErr, head, headErr)
}

func (adapter *Adapter) resolveFinalCompleteAfterHead(
	ctx context.Context,
	artifact backupobject.Artifact,
	completionErr error,
	head backupobject.HeadResult,
	headErr error,
) (backupobject.Object, bool, error) {
	cleanupCtx, cleanupCancel := reconciliationContext(ctx)
	cleanupErr := adapter.cleanupExactUploads(cleanupCtx, artifact.Key)
	cleanupCancel()
	if headErr != nil {
		return backupobject.Object{}, false, joinOperationCleanup(headErr, cleanupErr)
	}
	if cleanupErr != nil {
		return backupobject.Object{}, false, joinOperationCleanup(providerError(completionErr), cleanupErr)
	}
	if head.Present {
		return head.Object, false, nil
	}
	failure := classifyProviderFailure(completionErr)
	if failure == providerFailureConflict || failure == providerFailureAbsent {
		return backupobject.Object{}, true, nil
	}
	return backupobject.Object{}, false, providerError(completionErr)
}
