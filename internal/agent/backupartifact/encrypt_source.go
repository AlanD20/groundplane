package backupartifact

import (
	"context"
	"crypto/sha256"
	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"hash"
	"io"
)

type volumeArtifactWriter struct {
	ctx      context.Context
	artifact *backupstage.Artifact
}

func (writer *volumeArtifactWriter) Write(content []byte) (int, error) {
	return writer.artifact.Write(writer.ctx, content)
}

func EncryptSource(ctx context.Context, authority *agentpb.BackupEncryptionAuthority, identity []byte,
	stage *backupstage.Stage, source *backupstage.Artifact, sourceEvidence backupformat.Evidence,
	retained *backupstage.Artifact,
) (_ *backupstage.Artifact, _ backupstage.ArtifactEvidence, resultErr error) {
	recipient, err := ageinfra.IdentityRecipient(identity, authority.RecipientSha256)
	if err != nil {
		return nil, backupstage.ArtifactEvidence{}, err
	}
	if retained != nil {
		evidence, err := retained.Publish(ctx)
		if err != nil {
			return nil, backupstage.ArtifactEvidence{}, err
		}
		expected, err := backupformat.AgeStoredSize(sourceEvidence.SizeBytes)
		if err != nil || evidence.Size != expected {
			return nil, backupstage.ArtifactEvidence{}, errs.New(
				errs.KindStateConflict,
				"Encrypted Volume artifact differs from its captured source",
			)
		}
		reader, err := retained.Open(ctx)
		if err != nil {
			return nil, backupstage.ArtifactEvidence{}, err
		}
		defer func() {
			if closeErr := reader.Close(); closeErr != nil {
				resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
			}
		}()
		proof := &volumeSourceProof{hash: sha256.New()}
		if err := ageinfra.DecryptStream(ctx, string(identity), reader, proof,
			sourceEvidence.SizeBytes, func() { _ = reader.Close() }); err != nil {
			return nil, backupstage.ArtifactEvidence{}, err
		}
		if proof.size != sourceEvidence.SizeBytes || proof.sum() != sourceEvidence.SHA256 {
			return nil, backupstage.ArtifactEvidence{}, errs.New(
				errs.KindStateConflict,
				"Encrypted Volume artifact differs from its captured source",
			)
		}
		return retained, evidence, nil
	}
	stored, err := stage.CreateFile(ctx, executionplan.BackupStoredStagingFinal)
	if err != nil {
		return nil, backupstage.ArtifactEvidence{}, err
	}
	reader, err := source.Open(ctx)
	if err != nil {
		return nil, backupstage.ArtifactEvidence{}, err
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
		}
	}()
	if err := ageinfra.EncryptStream(ctx, recipient, reader,
		&volumeArtifactWriter{ctx: ctx, artifact: stored}, func() { _ = reader.Close() }); err != nil {
		return nil, backupstage.ArtifactEvidence{}, err
	}
	evidence, err := stored.Publish(ctx)
	if err != nil {
		return nil, backupstage.ArtifactEvidence{}, err
	}
	expected, err := backupformat.AgeStoredSize(sourceEvidence.SizeBytes)
	if err != nil || evidence.Size != expected {
		return nil, backupstage.ArtifactEvidence{}, errs.New(
			errs.KindStateConflict,
			"Encrypted Volume artifact differs from its captured source",
		)
	}
	return stored, evidence, nil
}

var _ io.Writer = (*volumeArtifactWriter)(nil)

type volumeSourceProof struct {
	hash hash.Hash
	size uint64
}

func (proof *volumeSourceProof) Write(content []byte) (int, error) {
	count, err := proof.hash.Write(content)
	proof.size += uint64(count)
	return count, err
}

func (proof *volumeSourceProof) sum() [sha256.Size]byte {
	var digest [sha256.Size]byte
	copy(digest[:], proof.hash.Sum(nil))
	return digest
}
