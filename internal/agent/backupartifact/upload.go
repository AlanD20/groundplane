package backupartifact

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// PublishCheckpoint must add the task, assignment, step, sequence, predecessor,
// and authority-digest envelope before publishing. It returns only after the
// Controller has acknowledged durable acceptance of the exact request.
type PublishCheckpoint func(context.Context, *agentpb.BackupCheckpointRequest) error

// UploadInput is the complete executor-owned upload state. Stored remains
// owned by the caller and is retained after Upload returns.
type UploadInput struct {
	Authority       *Authority
	Prepared        *agentpb.BackupArtifactPrepared
	Phase           agentpb.BackupCapturePhase
	UploadCompleted *agentpb.BackupUploadCompleted
	UploadVerified  *agentpb.BackupUploadVerified
	Stored          *backupstage.Artifact
	Store           backupobject.Store
	Publish         PublishCheckpoint
}

// Result is the exact provider object selected by verification and the
// checkpoint evidence that was durably accepted for it.
type Result struct {
	Object   backupobject.Object
	Verified *agentpb.BackupUploadVerified
}

// Upload persists the write intent before Put, the Put outcome before Head,
// and exact Head evidence before returning. It never publishes a Recovery
// Point and never cleans the retained staging artifact.
func Upload(ctx context.Context, input UploadInput) (Result, error) {
	if ctx == nil || input.Authority == nil || input.Prepared == nil || input.Stored == nil ||
		input.Store == nil || input.Publish == nil {
		return Result{}, invalidUpload()
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	artifact, err := input.Authority.artifact(input.Prepared)
	if err != nil {
		return Result{}, err
	}
	metadataCount, metadataSHA := artifact.MetadataEvidence()

	switch input.Phase {
	case agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING:
		if input.UploadCompleted != nil || input.UploadVerified != nil {
			return Result{}, invalidUpload()
		}
		if err := input.Publish(ctx, &agentpb.BackupCheckpointRequest{
			Checkpoint: &agentpb.BackupCheckpointRequest_ArtifactPrepared{
				ArtifactPrepared: proto.Clone(input.Prepared).(*agentpb.BackupArtifactPrepared),
			},
		}); err != nil {
			return Result{}, err
		}
		return putAndVerify(ctx, input, artifact, metadataCount, metadataSHA)
	case agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_ARTIFACT_PREPARED,
		agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_UPLOADING:
		if input.UploadCompleted != nil || input.UploadVerified != nil {
			return Result{}, invalidUpload()
		}
		completed := uploadCompleted(input.Authority, input.Prepared, metadataCount, metadataSHA, nil)
		if err := publishCompleted(ctx, input, completed); err != nil {
			return Result{}, err
		}
		return headAndVerify(ctx, input, artifact, completed, metadataCount, metadataSHA)
	case agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_HEAD_VERIFICATION:
		if input.UploadCompleted == nil || input.UploadVerified != nil {
			return Result{}, invalidUpload()
		}
		completed, err := validateCompleted(input, metadataCount, metadataSHA)
		if err != nil {
			return Result{}, err
		}
		return headAndVerify(ctx, input, artifact, completed, metadataCount, metadataSHA)
	case agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_POINT_COMMIT,
		agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_SOURCE_CLEANUP,
		agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_SERVICE_RECOVERY,
		agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_TERMINAL:
		if input.UploadVerified == nil {
			return Result{}, invalidUpload()
		}
		return verifyPersisted(ctx, input, artifact, metadataCount, metadataSHA)
	default:
		return Result{}, invalidUpload()
	}
}

func putAndVerify(ctx context.Context, input UploadInput, artifact backupobject.Artifact,
	metadataCount uint32, metadataSHA [32]byte,
) (Result, error) {
	reader, err := input.Stored.OpenPrefix(ctx)
	if err != nil {
		return Result{}, err
	}
	object, putErr := input.Store.PutExact(ctx, artifact, reader)
	closeErr := reader.Close()
	if putErr == nil && !sameArtifact(object.Artifact, artifact) {
		putErr = invalidUpload()
	}
	if putErr == nil {
		putErr = object.Discriminator.Validate()
	}
	if putErr != nil {
		if ctx.Err() != nil {
			return Result{}, joinUploadError(putErr, closeErr)
		}
		completed := uploadCompleted(input.Authority, input.Prepared, metadataCount, metadataSHA, nil)
		if err := publishCompleted(ctx, input, completed); err != nil {
			return Result{}, joinUploadError(err, closeErr)
		}
		result, err := headAndVerify(ctx, input, artifact, completed, metadataCount, metadataSHA)
		if err != nil {
			return Result{}, joinUploadError(err, closeErr)
		}
		if closeErr != nil {
			return Result{}, errs.Wrap(errs.KindStorageUnavailable, closeErr)
		}
		return result, nil
	}
	completed := uploadCompleted(input.Authority, input.Prepared, metadataCount, metadataSHA, &object)
	if err := publishCompleted(ctx, input, completed); err != nil {
		return Result{}, joinUploadError(err, closeErr)
	}
	if closeErr != nil {
		return Result{}, errs.Wrap(errs.KindStorageUnavailable, closeErr)
	}
	return headAndVerify(ctx, input, artifact, completed, metadataCount, metadataSHA)
}

func publishCompleted(ctx context.Context, input UploadInput, completed *agentpb.BackupUploadCompleted) error {
	return input.Publish(ctx, &agentpb.BackupCheckpointRequest{
		Checkpoint: &agentpb.BackupCheckpointRequest_UploadCompleted{UploadCompleted: completed},
	})
}

func headAndVerify(ctx context.Context, input UploadInput, artifact backupobject.Artifact,
	completed *agentpb.BackupUploadCompleted, metadataCount uint32, metadataSHA [32]byte,
) (Result, error) {
	var expected *backupobject.Discriminator
	if returned := completed.GetReturnedObject(); returned != nil {
		discriminator, err := discriminatorFromWire(returned)
		if err != nil {
			return Result{}, err
		}
		expected = &discriminator
	}
	head, err := input.Store.HeadExact(ctx, artifact, expected)
	if err != nil {
		return Result{}, err
	}
	if !head.Present {
		return Result{}, errs.New(errs.KindStorageUnavailable, "backup artifact upload outcome remains unresolved")
	}
	if !sameArtifact(head.Object.Artifact, artifact) || head.Object.Discriminator.Validate() != nil ||
		expected != nil && head.Object.Discriminator != *expected {
		return Result{}, invalidUpload()
	}
	verified := &agentpb.BackupUploadVerified{
		PointId: input.Prepared.PointId, Evidence: cloneEvidence(input.Prepared.Evidence),
		Object: objectToWire(input.Authority, head.Object), MetadataCount: metadataCount,
		MetadataSha256: append([]byte(nil), metadataSHA[:]...),
	}
	if err := input.Publish(ctx, &agentpb.BackupCheckpointRequest{
		Checkpoint: &agentpb.BackupCheckpointRequest_UploadVerified{UploadVerified: verified},
	}); err != nil {
		return Result{}, err
	}
	return Result{Object: head.Object, Verified: proto.Clone(verified).(*agentpb.BackupUploadVerified)}, nil
}

func verifyPersisted(ctx context.Context, input UploadInput, artifact backupobject.Artifact,
	metadataCount uint32, metadataSHA [32]byte,
) (Result, error) {
	validated, err := input.Authority.validateCheckpoint(&agentpb.BackupCheckpointRequest{
		Checkpoint: &agentpb.BackupCheckpointRequest_UploadVerified{UploadVerified: input.UploadVerified},
	})
	if err != nil {
		return Result{}, err
	}
	verified := validated.GetUploadVerified()
	if verified.PointId != input.Prepared.PointId || !proto.Equal(verified.Evidence, input.Prepared.Evidence) ||
		!wireObjectMatchesTarget(verified.Object, input.Authority.capture.Target) ||
		verified.MetadataCount != metadataCount || !bytes.Equal(verified.MetadataSha256, metadataSHA[:]) {
		return Result{}, invalidUpload()
	}
	discriminator, err := discriminatorFromWire(verified.Object)
	if err != nil {
		return Result{}, err
	}
	head, err := input.Store.HeadExact(ctx, artifact, &discriminator)
	if err != nil {
		return Result{}, err
	}
	if !head.Present || !sameArtifact(head.Object.Artifact, artifact) || head.Object.Discriminator != discriminator {
		return Result{}, invalidUpload()
	}
	return Result{Object: head.Object, Verified: proto.Clone(verified).(*agentpb.BackupUploadVerified)}, nil
}

func validateCompleted(
	input UploadInput,
	metadataCount uint32,
	metadataSHA [32]byte,
) (*agentpb.BackupUploadCompleted, error) {
	validated, err := input.Authority.validateCheckpoint(&agentpb.BackupCheckpointRequest{
		Checkpoint: &agentpb.BackupCheckpointRequest_UploadCompleted{UploadCompleted: input.UploadCompleted},
	})
	if err != nil {
		return nil, err
	}
	completed := validated.GetUploadCompleted()
	if completed.PointId != input.Prepared.PointId || !proto.Equal(completed.Evidence, input.Prepared.Evidence) ||
		!proto.Equal(completed.Target, input.Authority.capture.Target) || completed.MetadataCount != metadataCount ||
		!bytes.Equal(completed.MetadataSha256, metadataSHA[:]) {
		return nil, invalidUpload()
	}
	return completed, nil
}

func uploadCompleted(authority *Authority, prepared *agentpb.BackupArtifactPrepared,
	metadataCount uint32, metadataSHA [32]byte, object *backupobject.Object,
) *agentpb.BackupUploadCompleted {
	completed := &agentpb.BackupUploadCompleted{
		PointId: prepared.PointId, Evidence: cloneEvidence(prepared.Evidence),
		Target:        proto.Clone(authority.capture.Target).(*agentpb.BackupObjectTarget),
		MetadataCount: metadataCount, MetadataSha256: append([]byte(nil), metadataSHA[:]...),
	}
	if object == nil {
		completed.Outcome = &agentpb.BackupUploadCompleted_Unknown{Unknown: &agentpb.BackupPutOutcomeUnknown{}}
	} else {
		completed.Outcome = &agentpb.BackupUploadCompleted_ReturnedObject{
			ReturnedObject: objectToWire(authority, *object),
		}
	}
	return completed
}

func objectToWire(authority *Authority, object backupobject.Object) *agentpb.BackupObjectIdentity {
	target := authority.capture.Target
	wire := &agentpb.BackupObjectIdentity{
		Connector: proto.Clone(target.Connector).(*agentpb.BackupConnectorAuthority),
		Bucket:    target.Bucket, ObjectKey: target.ObjectKey,
	}
	if object.Discriminator.Kind == backupobject.DiscriminatorVersionID {
		wire.Discriminator = &agentpb.BackupObjectIdentity_VersionId{
			VersionId: &agentpb.BackupS3VersionId{Value: object.Discriminator.Value},
		}
	} else {
		wire.Discriminator = &agentpb.BackupObjectIdentity_Etag{
			Etag: &agentpb.BackupS3ETag{Value: object.Discriminator.Value},
		}
	}
	return wire
}

func discriminatorFromWire(object *agentpb.BackupObjectIdentity) (backupobject.Discriminator, error) {
	if object == nil {
		return backupobject.Discriminator{}, invalidUpload()
	}
	var discriminator backupobject.Discriminator
	switch value := object.GetDiscriminator().(type) {
	case *agentpb.BackupObjectIdentity_VersionId:
		if value == nil || value.VersionId == nil {
			return discriminator, invalidUpload()
		}
		discriminator = backupobject.Discriminator{Kind: backupobject.DiscriminatorVersionID, Value: value.VersionId.Value}
	case *agentpb.BackupObjectIdentity_Etag:
		if value == nil || value.Etag == nil {
			return discriminator, invalidUpload()
		}
		discriminator = backupobject.Discriminator{Kind: backupobject.DiscriminatorETag, Value: value.Etag.Value}
	default:
		return discriminator, invalidUpload()
	}
	return discriminator, discriminator.Validate()
}

func wireObjectMatchesTarget(object *agentpb.BackupObjectIdentity, target *agentpb.BackupObjectTarget) bool {
	return object != nil && target != nil && object.Bucket == target.Bucket && object.ObjectKey == target.ObjectKey &&
		proto.Equal(object.Connector, target.Connector)
}

func cloneEvidence(value *agentpb.BackupArtifactEvidence) *agentpb.BackupArtifactEvidence {
	return proto.Clone(value).(*agentpb.BackupArtifactEvidence)
}

func sameArtifact(left, right backupobject.Artifact) bool {
	if left.Key != right.Key || left.EnvironmentID != right.EnvironmentID || left.SourceID != right.SourceID ||
		left.RecoveryPointID != right.RecoveryPointID || left.SourceFormat != right.SourceFormat ||
		left.Encryption != right.Encryption || left.Evidence != right.Evidence || (left.KeyEra == nil) != (right.KeyEra == nil) {
		return false
	}
	return left.KeyEra == nil || *left.KeyEra == *right.KeyEra
}

func joinUploadError(primary, secondary error) error {
	if secondary == nil {
		return primary
	}
	kind, ok := errs.KindOf(primary)
	if !ok {
		kind = errs.KindStorageUnavailable
	}
	return errs.WrapJoined(kind, primary, secondary)
}

func invalidUpload() error {
	return errs.New(errs.KindStateConflict, "backup artifact upload state is invalid")
}
