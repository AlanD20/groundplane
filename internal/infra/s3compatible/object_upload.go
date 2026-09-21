package s3compatible

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
)

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
