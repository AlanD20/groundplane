package backupconfiguration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"hash"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Encrypt wraps the completed source, or authenticates an already published
// stored artifact before reusing it. Random age ciphertext is never regenerated
// after the Prepared checkpoint selected its stored-byte evidence.
func Encrypt(ctx context.Context, authority *agentpb.BackupEncryptionAuthority, identity []byte,
	stage *backupstage.Stage, source *backupstage.Artifact, sourceEvidence backupconfig.ArtifactEvidence,
	stored *backupstage.Artifact,
) (_ *backupstage.Artifact, _ backupstage.ArtifactEvidence, resultErr error) {
	if ctx == nil || authority == nil || authority.Kind != agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE ||
		stage == nil || source == nil || authority.GetKeyEra() == 0 {
		return nil, backupstage.ArtifactEvidence{}, invalidCapture()
	}
	recipient, err := ageinfra.IdentityRecipient(identity, authority.RecipientSha256)
	if err != nil {
		return nil, backupstage.ArtifactEvidence{}, err
	}
	if stored != nil {
		return verifyEncryption(ctx, string(identity), sourceEvidence, stored)
	}
	stored, err = stage.CreateFile(ctx, executionplan.BackupStoredStagingFinal)
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
	if err := ageinfra.EncryptStream(ctx, recipient, reader, &stageSourceWriter{ctx: ctx, artifact: stored},
		func() { _ = reader.Close() }); err != nil {
		return nil, backupstage.ArtifactEvidence{}, err
	}
	evidence, err := stored.Publish(ctx)
	if err != nil {
		return nil, backupstage.ArtifactEvidence{}, err
	}
	expectedSize, err := backupconfig.AgeStoredSize(sourceEvidence.SizeBytes)
	if err != nil || evidence.Size != expectedSize {
		return nil, backupstage.ArtifactEvidence{}, invalidCapture()
	}
	return stored, evidence, nil
}

func verifyEncryption(ctx context.Context, identity string, source backupconfig.ArtifactEvidence,
	stored *backupstage.Artifact,
) (_ *backupstage.Artifact, _ backupstage.ArtifactEvidence, resultErr error) {
	evidence, err := stored.Publish(ctx)
	if err != nil {
		return nil, backupstage.ArtifactEvidence{}, err
	}
	expectedSize, err := backupconfig.AgeStoredSize(source.SizeBytes)
	if err != nil || evidence.Size != expectedSize {
		return nil, backupstage.ArtifactEvidence{}, invalidCapture()
	}
	reader, err := stored.Open(ctx)
	if err != nil {
		return nil, backupstage.ArtifactEvidence{}, err
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
		}
	}()
	proof := &plaintextProof{hash: sha256.New()}
	if err := ageinfra.DecryptStream(ctx, identity, reader, proof, source.SizeBytes,
		func() { _ = reader.Close() }); err != nil {
		return nil, backupstage.ArtifactEvidence{}, err
	}
	if proof.size != source.SizeBytes || !bytes.Equal(proof.hash.Sum(nil), source.SHA256[:]) {
		return nil, backupstage.ArtifactEvidence{}, invalidCapture()
	}
	return stored, evidence, nil
}

type plaintextProof struct {
	hash hash.Hash
	size uint64
}

func (proof *plaintextProof) Write(content []byte) (int, error) {
	count, err := proof.hash.Write(content)
	proof.size += uint64(count)
	return count, err
}
