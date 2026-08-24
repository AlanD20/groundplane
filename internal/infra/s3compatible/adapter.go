package s3compatible

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"hash"
	"io"
	"maps"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	singlePutMaximum uint64 = 100 * 1024 * 1024
	partQuantum      uint64 = 1024 * 1024
	maximumParts     uint64 = 10_000
)

type s3API interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	CreateMultipartUpload(
		context.Context,
		*s3.CreateMultipartUploadInput,
		...func(*s3.Options),
	) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(
		context.Context,
		*s3.CompleteMultipartUploadInput,
		...func(*s3.Options),
	) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(
		context.Context,
		*s3.AbortMultipartUploadInput,
		...func(*s3.Options),
	) (*s3.AbortMultipartUploadOutput, error)
	ListMultipartUploads(
		context.Context,
		*s3.ListMultipartUploadsInput,
		...func(*s3.Options),
	) (*s3.ListMultipartUploadsOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

type Adapter struct {
	config Config
	client s3API
}

var _ backupobject.Store = (*Adapter)(nil)

func (adapter *Adapter) PutExact(
	ctx context.Context,
	artifact backupobject.Artifact,
	source io.ReaderAt,
) (backupobject.Object, error) {
	if err := ctx.Err(); err != nil {
		return backupobject.Object{}, err
	}
	if err := adapter.validateArtifact(artifact); err != nil {
		return backupobject.Object{}, err
	}
	if source == nil {
		return backupobject.Object{}, errs.New(errs.KindValidationFailed, "s3 backup object source is required")
	}
	if artifact.Evidence.StoredSizeBytes <= singlePutMaximum {
		return adapter.putSingle(ctx, artifact, source)
	}
	return adapter.putMultipart(ctx, artifact, source)
}

func (adapter *Adapter) putSingle(
	ctx context.Context,
	artifact backupobject.Artifact,
	source io.ReaderAt,
) (backupobject.Object, error) {
	contentMD5, storedSHA256, err := digestRange(ctx, source, 0, artifact.Evidence.StoredSizeBytes, nil)
	if err != nil {
		return backupobject.Object{}, err
	}
	if storedSHA256 != artifact.Evidence.StoredSHA256 {
		return backupobject.Object{}, errs.New(errs.KindValidationFailed, "s3 backup object source digest is invalid")
	}
	size := int64(artifact.Evidence.StoredSizeBytes)
	output, err := adapter.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(adapter.config.Bucket),
		Key:           aws.String(artifact.Key),
		Body:          io.NewSectionReader(source, 0, size),
		ContentLength: aws.Int64(size),
		ContentMD5:    aws.String(contentMD5),
		IfNoneMatch:   aws.String("*"),
		Metadata:      artifact.Metadata(),
	})
	if err != nil {
		return backupobject.Object{}, providerError(err)
	}
	if output == nil {
		return backupobject.Object{}, rejectedError()
	}
	discriminator, err := outputDiscriminator(output.VersionId, output.ETag)
	if err != nil {
		return backupobject.Object{}, err
	}
	return backupobject.Object{Artifact: artifact, Discriminator: discriminator}, nil
}

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

func (adapter *Adapter) validateArtifact(artifact backupobject.Artifact) error {
	if err := artifact.Validate(); err != nil {
		return err
	}
	if artifact.Key != artifact.ExpectedKey(adapter.config.Prefix) {
		return errs.New(errs.KindValidationFailed, "s3 backup object key is outside its reserved identity")
	}
	return nil
}

func (adapter *Adapter) validateObject(object backupobject.Object) error {
	if err := object.Validate(); err != nil {
		return err
	}
	return adapter.validateArtifact(object.Artifact)
}

func validateRemoteObject(
	artifact backupobject.Artifact,
	expected *backupobject.Discriminator,
	contentLength *int64,
	metadata map[string]string,
	versionID *string,
	etag *string,
) (backupobject.Discriminator, error) {
	if contentLength == nil || *contentLength < 0 || uint64(*contentLength) != artifact.Evidence.StoredSizeBytes ||
		!maps.Equal(metadata, artifact.Metadata()) {
		return backupobject.Discriminator{}, conflictError()
	}
	discriminator, err := outputDiscriminator(versionID, etag)
	if err != nil {
		return backupobject.Discriminator{}, err
	}
	if expected != nil && discriminator != *expected {
		return backupobject.Discriminator{}, conflictError()
	}
	return discriminator, nil
}

func outputDiscriminator(versionID, etag *string) (backupobject.Discriminator, error) {
	if versionID != nil {
		discriminator := backupobject.Discriminator{
			Kind: backupobject.DiscriminatorVersionID, Value: *versionID,
		}
		if err := discriminator.Validate(); err != nil {
			return backupobject.Discriminator{}, rejectedError()
		}
		return discriminator, nil
	}
	if etag != nil {
		discriminator := backupobject.Discriminator{Kind: backupobject.DiscriminatorETag, Value: *etag}
		if err := discriminator.Validate(); err != nil {
			return backupobject.Discriminator{}, rejectedError()
		}
		return discriminator, nil
	}
	return backupobject.Discriminator{}, rejectedError()
}

func applyDiscriminatorToHead(input *s3.HeadObjectInput, discriminator backupobject.Discriminator) {
	if discriminator.Kind == backupobject.DiscriminatorVersionID {
		input.VersionId = aws.String(discriminator.Value)
	} else {
		input.IfMatch = aws.String(discriminator.Value)
	}
}

func applyDiscriminatorToGet(input *s3.GetObjectInput, discriminator backupobject.Discriminator) {
	if discriminator.Kind == backupobject.DiscriminatorVersionID {
		input.VersionId = aws.String(discriminator.Value)
	} else {
		input.IfMatch = aws.String(discriminator.Value)
	}
}

func applyDiscriminatorToDelete(input *s3.DeleteObjectInput, discriminator backupobject.Discriminator) {
	if discriminator.Kind == backupobject.DiscriminatorVersionID {
		input.VersionId = aws.String(discriminator.Value)
	} else {
		input.IfMatch = aws.String(discriminator.Value)
	}
}

func digestRange(
	ctx context.Context,
	source io.ReaderAt,
	offset uint64,
	length uint64,
	whole hash.Hash,
) (string, [sha256.Size]byte, error) {
	md5Hash := md5.New()
	writers := []io.Writer{md5Hash}
	localSHA256 := sha256.New()
	if whole == nil {
		writers = append(writers, localSHA256)
	} else {
		writers = append(writers, whole)
	}
	writer := io.MultiWriter(writers...)
	const maximumRead = 64 * 1024
	buffer := make([]byte, maximumRead)
	for copied := uint64(0); copied < length; {
		if err := ctx.Err(); err != nil {
			return "", [sha256.Size]byte{}, err
		}
		readLength := min(uint64(len(buffer)), length-copied)
		n, readErr := source.ReadAt(buffer[:int(readLength)], int64(offset+copied))
		if n > 0 {
			written, writeErr := writer.Write(buffer[:n])
			if writeErr != nil || written != n {
				return "", [sha256.Size]byte{}, internalError()
			}
			copied += uint64(n)
		}
		if n != int(readLength) || readErr != nil && !errors.Is(readErr, io.EOF) {
			return "", [sha256.Size]byte{}, internalError()
		}
	}
	var digest [sha256.Size]byte
	if whole == nil {
		copy(digest[:], localSHA256.Sum(nil))
	}
	return base64.StdEncoding.EncodeToString(md5Hash.Sum(nil)), digest, nil
}

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

func multipartPartSize(size uint64) uint64 {
	minimumMultiple := (size + maximumParts*partQuantum - 1) / (maximumParts * partQuantum)
	return max(singlePutMaximum, minimumMultiple*partQuantum)
}

func oneAttempt(options *s3.Options) {
	options.Retryer = aws.NopRetryer{}
}

func (adapter *Adapter) listExactUploads(ctx context.Context, key string) ([]string, error) {
	var uploadIDs []string
	var keyMarker, uploadIDMarker *string
	for {
		output, err := adapter.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
			Bucket:         aws.String(adapter.config.Bucket),
			Prefix:         aws.String(key),
			KeyMarker:      keyMarker,
			UploadIdMarker: uploadIDMarker,
		})
		if err != nil {
			if classifyProviderFailure(err) == providerFailureAbsent {
				return nil, rejectedError()
			}
			return nil, providerError(err)
		}
		if output == nil {
			return nil, rejectedError()
		}
		for _, upload := range output.Uploads {
			if upload.Key != nil && *upload.Key == key {
				if upload.UploadId == nil || *upload.UploadId == "" {
					return nil, rejectedError()
				}
				uploadIDs = append(uploadIDs, *upload.UploadId)
			}
		}
		if output.IsTruncated == nil || !*output.IsTruncated {
			return uploadIDs, nil
		}
		if output.NextKeyMarker == nil || output.NextUploadIdMarker == nil ||
			*output.NextKeyMarker == "" || *output.NextUploadIdMarker == "" ||
			keyMarker != nil && *keyMarker == *output.NextKeyMarker &&
				uploadIDMarker != nil && *uploadIDMarker == *output.NextUploadIdMarker {
			return nil, rejectedError()
		}
		keyMarker = output.NextKeyMarker
		uploadIDMarker = output.NextUploadIdMarker
	}
}

func (adapter *Adapter) cleanupExactUploads(ctx context.Context, key string) error {
	uploadIDs, err := adapter.listExactUploads(ctx, key)
	if err != nil {
		return err
	}
	for _, uploadID := range uploadIDs {
		if err := adapter.abortOne(ctx, key, uploadID); err != nil {
			return err
		}
	}
	remaining, err := adapter.listExactUploads(ctx, key)
	if err != nil {
		return err
	}
	if len(remaining) != 0 {
		return conflictError()
	}
	return nil
}

func (adapter *Adapter) abortOne(ctx context.Context, key, uploadID string) error {
	_, err := adapter.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(adapter.config.Bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	if err != nil && classifyProviderFailure(err) != providerFailureAbsent {
		return providerError(err)
	}
	return nil
}

func (adapter *Adapter) uploadExists(ctx context.Context, key, uploadID string) (bool, error) {
	uploadIDs, err := adapter.listExactUploads(ctx, key)
	if err != nil {
		return false, err
	}
	for _, candidate := range uploadIDs {
		if candidate == uploadID {
			return true, nil
		}
	}
	return false, nil
}
