package s3compatible

import (
	"bytes"
	"context"
	"errors"
	"maps"

	"github.com/AlanD20/groundplane/internal/common/s3connector"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (adapter *Adapter) checkMultipart(
	ctx context.Context,
	state *s3connector.ConnectionCheckCleanup,
	journal ConnectionCheckJournal,
	body []byte,
	metadata map[string]string,
) error {
	state.MultipartPending = true
	if err := journal.WriteConnectionCheckCleanup(ctx, *state); err != nil {
		return err
	}
	output, err := adapter.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(adapter.config.Bucket), Key: aws.String(state.MultipartKey), Metadata: metadata,
	}, oneAttempt)
	if err != nil {
		return providerError(err)
	}
	if output == nil || output.UploadId == nil || *output.UploadId == "" || len(*output.UploadId) > 4096 {
		return rejectedError()
	}
	state.UploadIDs = []string{*output.UploadId}
	if err := journal.WriteConnectionCheckCleanup(ctx, *state); err != nil {
		return err
	}
	part, err := adapter.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String(adapter.config.Bucket), Key: aws.String(state.MultipartKey),
		UploadId: output.UploadId, PartNumber: aws.Int32(1),
		Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))),
	}, oneAttempt)
	if err != nil {
		return providerError(err)
	}
	if part == nil || part.ETag == nil || *part.ETag == "" {
		return rejectedError()
	}
	return nil
}

// CleanupConnectionCheck resumes only exact pending probe cleanup after a lost
// response or Controller restart. The caller supplies the retained Connector and
// credentials, not the current desired configuration, and fences exclusive owner.
func (adapter *Adapter) CleanupConnectionCheck(
	ctx context.Context,
	state s3connector.ConnectionCheckCleanup,
	journal ConnectionCheckJournal,
) (s3connector.ConnectionCheckCleanup, error) {
	if ctx == nil || journal == nil {
		return state, rejectedError()
	}
	if err := state.Validate(adapter.config.Prefix); err != nil {
		return state, err
	}
	var cleanupErr error
	if state.ObjectPending {
		if err := adapter.cleanupCheckObject(ctx, state); err != nil {
			cleanupErr = checkCleanupError("object cleanup", err)
		} else {
			state.ObjectPending = false
		}
	}
	if state.MultipartPending {
		if err := adapter.cleanupCheckUploads(ctx, &state, journal); err != nil {
			cleanupErr = errors.Join(cleanupErr, checkCleanupError("multipart cleanup", err))
		} else {
			state.MultipartPending = false
			state.UploadIDs = nil
		}
	}
	return state, errors.Join(cleanupErr, journal.WriteConnectionCheckCleanup(ctx, state))
}

func (adapter *Adapter) cleanupCheckObject(ctx context.Context, state s3connector.ConnectionCheckCleanup) error {
	head, err := adapter.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(adapter.config.Bucket), Key: aws.String(state.ObjectKey),
	}, oneAttempt)
	if classifyProviderFailure(err) == providerFailureAbsent {
		return nil
	}
	if err != nil {
		return providerError(err)
	}
	if head == nil || !maps.Equal(head.Metadata, checkObjectMetadata(state)) ||
		head.ContentLength == nil || *head.ContentLength != state.PayloadSizeBytes ||
		head.ETag == nil || *head.ETag == "" {
		return checkOwnershipConflict()
	}
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(adapter.config.Bucket), Key: aws.String(state.ObjectKey), IfMatch: head.ETag,
	}
	if head.VersionId != nil && *head.VersionId != "" && *head.VersionId != "null" {
		input.VersionId = head.VersionId
	}
	_, deleteErr := adapter.client.DeleteObject(ctx, input, oneAttempt)
	_, err = adapter.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(adapter.config.Bucket), Key: aws.String(state.ObjectKey),
	}, oneAttempt)
	if classifyProviderFailure(err) == providerFailureAbsent {
		return nil
	}
	if deleteErr != nil {
		return providerError(deleteErr)
	}
	if err != nil {
		return providerError(err)
	}
	return checkOwnershipConflict()
}

func (adapter *Adapter) cleanupCheckUploads(
	ctx context.Context,
	state *s3connector.ConnectionCheckCleanup,
	journal ConnectionCheckJournal,
) error {
	uploadIDs, err := adapter.listCheckUploads(ctx, state.MultipartKey)
	if err != nil {
		return err
	}
	if len(uploadIDs) > 16 {
		return checkOwnershipConflict()
	}
	state.UploadIDs = uploadIDs
	// Persist discovered identities before Abort, including uploads whose Create
	// response was lost. The reserved operation key is never reused by a new check.
	if err := journal.WriteConnectionCheckCleanup(ctx, *state); err != nil {
		return err
	}
	for _, uploadID := range uploadIDs {
		_, err := adapter.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket: aws.String(adapter.config.Bucket), Key: aws.String(state.MultipartKey),
			UploadId: aws.String(uploadID),
		}, oneAttempt)
		if err != nil && classifyProviderFailure(err) != providerFailureAbsent {
			return providerError(err)
		}
	}
	remaining, err := adapter.listCheckUploads(ctx, state.MultipartKey)
	if err != nil {
		return err
	}
	if len(remaining) != 0 {
		state.UploadIDs = remaining
		return checkOwnershipConflict()
	}
	return nil
}

// The check has one upload at most. Bound both pages and matching identities;
// pagination cannot turn a provider response into an unbounded diagnostic job.
func (adapter *Adapter) listCheckUploads(ctx context.Context, key string) ([]string, error) {
	var uploadIDs []string
	var keyMarker, uploadMarker *string
	for range 16 {
		output, err := adapter.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
			Bucket: aws.String(adapter.config.Bucket), Prefix: aws.String(key), MaxUploads: aws.Int32(17),
			KeyMarker: keyMarker, UploadIdMarker: uploadMarker,
		}, oneAttempt)
		if err != nil {
			return nil, providerError(err)
		}
		if output == nil || len(output.Uploads) > 17 {
			return nil, rejectedError()
		}
		for _, upload := range output.Uploads {
			if upload.Key == nil || *upload.Key != key {
				continue
			}
			if upload.UploadId == nil || *upload.UploadId == "" || len(*upload.UploadId) > 4096 {
				return nil, rejectedError()
			}
			uploadIDs = append(uploadIDs, *upload.UploadId)
			if len(uploadIDs) > 16 {
				return nil, checkOwnershipConflict()
			}
		}
		if output.IsTruncated == nil || !*output.IsTruncated {
			return uploadIDs, nil
		}
		if output.NextKeyMarker == nil || output.NextUploadIdMarker == nil ||
			*output.NextKeyMarker == "" || *output.NextUploadIdMarker == "" ||
			keyMarker != nil && *keyMarker == *output.NextKeyMarker &&
				uploadMarker != nil && *uploadMarker == *output.NextUploadIdMarker {
			return nil, rejectedError()
		}
		keyMarker, uploadMarker = output.NextKeyMarker, output.NextUploadIdMarker
	}
	return nil, checkOwnershipConflict()
}
