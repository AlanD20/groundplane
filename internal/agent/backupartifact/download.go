package backupartifact

import (
	"context"
	"crypto/sha256"
	"hash"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type DownloadInput struct {
	Authority *RestoreAuthority
	Stage     *backupstage.Stage
	Store     backupobject.Store
	Identity  []byte
	// Recovered files must already have the Controller's exact startup
	// disposition. Partial files cannot be passed as complete representations.
	Stored *backupstage.Artifact
	Source *backupstage.Artifact
}

type DownloadResult struct {
	Source         *backupstage.Artifact
	Stored         *backupstage.Artifact
	SourceEvidence backupstage.ArtifactEvidence
	StoredEvidence backupstage.ArtifactEvidence
}

// Download selects only the sealed version/ETag, independently verifies all
// stored bytes, then authenticates and verifies the complete source. It does
// not announce format validation or authorize any live mutation. Interrupted
// partial files remain owned by Stage for startup classification.
func Download(ctx context.Context, input DownloadInput) (DownloadResult, error) {
	if ctx == nil || input.Authority == nil || input.Authority.restore == nil || input.Stage == nil ||
		input.Store == nil {
		return DownloadResult{}, invalidRestore()
	}
	if err := ctx.Err(); err != nil {
		return DownloadResult{}, err
	}
	object := input.Authority.object
	if object.Artifact.Encryption == backupobject.EncryptionAge {
		if _, err := ageinfra.IdentityRecipient(input.Identity, input.Authority.restore.Encryption.RecipientSha256); err != nil {
			return DownloadResult{}, err
		}
	} else if len(input.Identity) != 0 || input.Source != nil && input.Source != input.Stored {
		return DownloadResult{}, invalidRestore()
	}
	stored := input.Stored
	if stored == nil {
		if input.Source != nil {
			return DownloadResult{}, invalidRestore()
		}
		name := executionplan.BackupStoredStagingFinal
		if object.Artifact.Encryption == backupobject.EncryptionNone {
			name = executionplan.BackupSourceStagingFinal
		}
		var err error
		stored, err = input.Stage.CreateFile(ctx, name)
		if err != nil {
			return DownloadResult{}, err
		}
		writer := newArtifactWriter(ctx, stored, object.Artifact.Evidence.StoredSizeBytes)
		if err := input.Store.GetExact(ctx, object, writer); err != nil {
			return DownloadResult{}, err
		}
		if !writer.matches(object.Artifact.Evidence.StoredSizeBytes, object.Artifact.Evidence.StoredSHA256) {
			return DownloadResult{}, invalidRestore()
		}
	}
	storedEvidence, err := verifyStaged(
		ctx,
		stored,
		object.Artifact.Evidence.StoredSizeBytes,
		object.Artifact.Evidence.StoredSHA256,
	)
	if err != nil {
		return DownloadResult{}, err
	}
	if object.Artifact.Encryption == backupobject.EncryptionNone {
		if storedEvidence.Name != executionplan.BackupSourceStagingFinal {
			return DownloadResult{}, invalidRestore()
		}
		return DownloadResult{
			Source:         stored,
			Stored:         stored,
			SourceEvidence: storedEvidence,
			StoredEvidence: storedEvidence,
		}, nil
	}
	if storedEvidence.Name != executionplan.BackupStoredStagingFinal {
		return DownloadResult{}, invalidRestore()
	}
	source := input.Source
	if source == nil {
		source, err = input.Stage.CreateFile(ctx, executionplan.BackupSourceStagingFinal)
		if err != nil {
			return DownloadResult{}, err
		}
		if err := decryptSource(ctx, input.Identity, stored, source, object.Artifact.Evidence); err != nil {
			return DownloadResult{}, err
		}
	}
	sourceEvidence, err := verifyStaged(
		ctx,
		source,
		object.Artifact.Evidence.SourceSizeBytes,
		object.Artifact.Evidence.SourceSHA256,
	)
	if err != nil {
		return DownloadResult{}, err
	}
	if sourceEvidence.Name != executionplan.BackupSourceStagingFinal {
		return DownloadResult{}, invalidRestore()
	}
	return DownloadResult{
		Source:         source,
		Stored:         stored,
		SourceEvidence: sourceEvidence,
		StoredEvidence: storedEvidence,
	}, nil
}

func decryptSource(ctx context.Context, identity []byte, stored, source *backupstage.Artifact,
	evidence backupobject.Evidence,
) (resultErr error) {
	reader, err := stored.Open(ctx)
	if err != nil {
		return err
	}
	defer func() { resultErr = joinUploadError(resultErr, reader.Close()) }()
	writer := newArtifactWriter(ctx, source, evidence.SourceSizeBytes)
	if err := ageinfra.DecryptStream(ctx, string(identity), reader, writer, evidence.SourceSizeBytes,
		func() { _ = reader.Close() }); err != nil {
		return err
	}
	if !writer.matches(evidence.SourceSizeBytes, evidence.SourceSHA256) {
		return invalidRestore()
	}
	return nil
}

func verifyStaged(ctx context.Context, artifact *backupstage.Artifact, size uint64, digest [32]byte) (
	_ backupstage.ArtifactEvidence, resultErr error,
) {
	reader, err := artifact.OpenPrefix(ctx)
	if err != nil {
		return backupstage.ArtifactEvidence{}, err
	}
	defer func() { resultErr = joinUploadError(resultErr, reader.Close()) }()
	buffer := make([]byte, 32<<10)
	defer clear(buffer)
	hasher := sha256.New()
	written, err := io.CopyBuffer(hasher, io.LimitReader(reader, int64(size)+1), buffer)
	if err != nil {
		return backupstage.ArtifactEvidence{}, err
	}
	var actual [32]byte
	copy(actual[:], hasher.Sum(nil))
	if written != int64(size) || actual != digest {
		return backupstage.ArtifactEvidence{}, invalidRestore()
	}
	physical, err := artifact.Publish(ctx)
	if err != nil {
		return backupstage.ArtifactEvidence{}, err
	}
	if physical.Size != size || physical.SHA256 != digest {
		return backupstage.ArtifactEvidence{}, invalidRestore()
	}
	return physical, nil
}

type artifactWriter struct {
	ctx       context.Context
	artifact  *backupstage.Artifact
	hasher    hash.Hash
	remaining uint64
	written   uint64
}

func newArtifactWriter(ctx context.Context, artifact *backupstage.Artifact, size uint64) *artifactWriter {
	return &artifactWriter{ctx: ctx, artifact: artifact, hasher: sha256.New(), remaining: size}
}

func (writer *artifactWriter) Write(content []byte) (int, error) {
	if uint64(len(content)) > writer.remaining {
		return 0, errs.New(errs.KindValidationFailed, "backup artifact exceeds its sealed byte length")
	}
	count, err := writer.artifact.Write(writer.ctx, content)
	if count > 0 {
		_, _ = writer.hasher.Write(content[:count]) // hash.Hash consumes all bytes without error.
		writer.written += uint64(count)
		writer.remaining -= uint64(count)
	}
	return count, err
}

func (writer *artifactWriter) matches(size uint64, digest [32]byte) bool {
	var actual [32]byte
	copy(actual[:], writer.hasher.Sum(nil))
	return writer.remaining == 0 && writer.written == size && actual == digest
}
