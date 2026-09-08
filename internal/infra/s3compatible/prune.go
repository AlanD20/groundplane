package s3compatible

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const pruneReconcileInterval = 250 * time.Millisecond

// PruneExact removes only the Controller-sealed object. It never lists a
// bucket: exact HEAD establishes the current provider discriminator and the
// sealed size/hash establish that the object is the authorized artifact.
func (adapter *Adapter) PruneExact(ctx context.Context, authority backupobject.PruneAuthority) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := authority.Validate(adapter.config.Prefix); err != nil {
		return err
	}

	discriminator, present, err := adapter.headPruneAuthority(ctx, authority, nil)
	if err != nil || !present {
		return err
	}
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(adapter.config.Bucket),
		Key:    aws.String(authority.Key),
	}
	applyDiscriminatorToDelete(input, discriminator)
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
		_, present, err := adapter.headPruneAuthority(ctx, authority, &discriminator)
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
	expected *backupobject.Discriminator,
) (backupobject.Discriminator, bool, error) {
	input := &s3.HeadObjectInput{Bucket: aws.String(adapter.config.Bucket), Key: aws.String(authority.Key)}
	if expected != nil {
		applyDiscriminatorToHead(input, *expected)
	}
	output, err := adapter.client.HeadObject(ctx, input)
	if err != nil {
		if classifyProviderFailure(err) == providerFailureAbsent {
			return backupobject.Discriminator{}, false, nil
		}
		return backupobject.Discriminator{}, false, providerError(err)
	}
	if output == nil || output.ContentLength == nil || *output.ContentLength < 0 ||
		uint64(
			*output.ContentLength,
		) != authority.StoredSizeBytes || !matchesStoredSHA256(output.ChecksumSHA256, output.Metadata, authority.StoredSHA256) {
		return backupobject.Discriminator{}, false, conflictError()
	}
	discriminator, err := outputDiscriminator(output.VersionId, output.ETag)
	if err != nil {
		return backupobject.Discriminator{}, false, err
	}
	if expected != nil && discriminator != *expected {
		return backupobject.Discriminator{}, false, conflictError()
	}
	return discriminator, true, nil
}

func matchesStoredSHA256(checksum *string, metadata map[string]string, expected [sha256.Size]byte) bool {
	hexDigest := hex.EncodeToString(expected[:])
	base64Digest := base64.StdEncoding.EncodeToString(expected[:])
	if checksum != nil && strings.TrimSpace(*checksum) == base64Digest {
		return true
	}
	for _, value := range metadata {
		value = strings.TrimSpace(value)
		if strings.EqualFold(value, hexDigest) || value == base64Digest {
			return true
		}
	}
	return false
}
