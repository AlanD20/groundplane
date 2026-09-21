package s3compatible

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

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
