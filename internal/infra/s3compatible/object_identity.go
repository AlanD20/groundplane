package s3compatible

import (
	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"maps"
)

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
	discriminator, err := observedDiscriminator(versionID, etag, expected)
	if err != nil {
		return backupobject.Discriminator{}, err
	}
	if expected != nil && discriminator != *expected {
		return backupobject.Discriminator{}, conflictError()
	}
	return discriminator, nil
}

func outputDiscriminator(versionID, etag *string) (backupobject.Discriminator, error) {
	// A returned VersionId does not prove version-targeted operations exist.
	// Prefer the conditional ETag contract before sealing an upload identity.
	if etag != nil {
		return checkedDiscriminator(backupobject.DiscriminatorETag, etag)
	}
	return checkedDiscriminator(backupobject.DiscriminatorVersionID, versionID)
}

func observedDiscriminator(
	versionID, etag *string, expected *backupobject.Discriminator,
) (backupobject.Discriminator, error) {
	if expected == nil {
		return outputDiscriminator(versionID, etag)
	}
	// An acknowledged identity is immutable, including its selected kind.
	// Never substitute a different response field after a failed operation.
	switch expected.Kind {
	case backupobject.DiscriminatorETag:
		return checkedDiscriminator(expected.Kind, etag)
	case backupobject.DiscriminatorVersionID:
		return checkedDiscriminator(expected.Kind, versionID)
	default:
		return backupobject.Discriminator{}, rejectedError()
	}
}

func checkedDiscriminator(kind backupobject.DiscriminatorKind, value *string) (backupobject.Discriminator, error) {
	if value != nil {
		discriminator := backupobject.Discriminator{Kind: kind, Value: *value}
		if discriminator.Validate() == nil {
			return discriminator, nil
		}
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
