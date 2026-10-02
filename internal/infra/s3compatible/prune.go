package s3compatible

import (
	"context"
	"encoding/hex"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const pruneReconcileInterval = 250 * time.Millisecond

// PruneExact removes only the Controller-sealed object. It never lists a
// bucket or selects a new discriminator. Exact HEAD proves the sealed object
// and the complete metadata set before deletion and during absence checks.
func (adapter *Adapter) PruneExact(ctx context.Context, authority backupobject.PruneAuthority) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := authority.Validate(adapter.config.Prefix); err != nil {
		return err
	}

	present, err := adapter.headPruneAuthority(ctx, authority)
	if err != nil || !present {
		return err
	}
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(adapter.config.Bucket),
		Key:    aws.String(authority.Key),
	}
	applyDiscriminatorToDelete(input, authority.Discriminator)
	if _, err := adapter.client.DeleteObject(ctx, input, oneAttempt); err != nil {
		switch classifyProviderFailure(err) {
		case providerFailureAbsent:
			return nil
		case providerFailureConflict:
			return conflictError()
		default:
			return providerError(err)
		}
	}

	ticker := time.NewTicker(pruneReconcileInterval)
	defer ticker.Stop()
	for {
		present, err := adapter.headPruneAuthority(ctx, authority)
		if err == nil && !present {
			return nil
		}
		if err != nil && classifyProviderFailure(err) != providerFailureUnavailable {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (adapter *Adapter) headPruneAuthority(
	ctx context.Context,
	authority backupobject.PruneAuthority,
) (bool, error) {
	input := &s3.HeadObjectInput{Bucket: aws.String(adapter.config.Bucket), Key: aws.String(authority.Key)}
	applyDiscriminatorToHead(input, authority.Discriminator)
	output, err := adapter.client.HeadObject(ctx, input)
	if err != nil {
		if classifyProviderFailure(err) == providerFailureAbsent {
			return false, nil
		}
		return false, providerError(err)
	}
	if output == nil || output.ContentLength == nil || *output.ContentLength < 0 ||
		uint64(
			*output.ContentLength,
		) != authority.Evidence.StoredSizeBytes {
		return false, conflictError()
	}
	discriminator, err := outputDiscriminator(output.VersionId, output.ETag)
	if err != nil {
		return false, err
	}
	count, digest := backupobject.MetadataEvidence(output.Metadata)
	if discriminator != authority.Discriminator || count != authority.MetadataCount ||
		digest != authority.MetadataSHA256 ||
		output.Metadata["groundplane-stored-size-bytes"] != strconv.FormatUint(
			authority.Evidence.StoredSizeBytes,
			10,
		) ||
		output.Metadata["groundplane-source-size-bytes"] != strconv.FormatUint(
			authority.Evidence.SourceSizeBytes,
			10,
		) ||
		output.Metadata["groundplane-source-sha256"] != hex.EncodeToString(authority.Evidence.SourceSHA256[:]) ||
		output.Metadata["groundplane-stored-sha256"] != hex.EncodeToString(authority.Evidence.StoredSHA256[:]) {
		return false, conflictError()
	}
	return true, nil
}
