package backupconfiguration

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/agent/backupartifact"
	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// RestoreArtifact owns an independently validated Config archive, not a live
// Entry generation. Its pass-two values remain private until transfer commits.
type RestoreArtifact struct {
	validated *backupconfig.ValidatedArtifact
	evidence  *agentpb.BackupRestoreArtifactValidated
	authority *agentpb.BackupConfigRestoreAuthority
}

// PrepareRestore downloads/decrypts only the selected Recovery Point, validates
// every canonical archive byte in a separately owned bounded spool, and seals
// the exact proof. The caller must publish that checkpoint before staging a
// complete new Entry generation; this function never mutates the Environment.
func PrepareRestore(ctx context.Context, input backupartifact.DownloadInput) (
	_ *RestoreArtifact, resultErr error,
) {
	sealed := input.Authority.Sealed()
	if ctx == nil || sealed.GetConfig() == nil || input.Stage == nil {
		return nil, invalidRestoreArtifact()
	}
	download, err := backupartifact.Download(ctx, input)
	if err != nil {
		return nil, err
	}
	reader, err := download.Source.OpenPrefix(ctx)
	if err != nil {
		return nil, err
	}
	readerClosed := false
	defer func() {
		if !readerClosed {
			closeErr := reader.Close()
			if closeErr != nil {
				resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
			}
		}
	}()
	createSpool := func(ctx context.Context) (backupconfig.ValidationSpool, error) {
		return input.Stage.CreateValidationSpool(ctx, download.SourceEvidence.Size)
	}
	validated, err := backupconfig.ValidateArtifact(ctx, reader, backupconfig.SourceEvidence{
		SizeBytes: download.SourceEvidence.Size, SHA256: download.SourceEvidence.SHA256,
	}, createSpool, backupconfigtransfer.EncodeRestoreMetadata)
	if err != nil {
		return nil, err
	}
	keep := false
	defer func() {
		if !keep {
			if closeErr := validated.Close(ctx); closeErr != nil {
				resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
			}
		}
	}()
	content := validated.Layout().Authority
	expected := sealed.GetConfig().GetExpectedArchive()
	sourceSHA256 := validated.SourceSHA256()
	if !restoreContentMatches(content, expected.GetContent()) ||
		!bytes.Equal(sourceSHA256[:], sealed.ExpectedEvidence.SourceSha256) {
		return nil, invalidRestoreArtifact()
	}
	sameInode := download.Source == download.Stored
	proof := &agentpb.BackupRestoreArtifactValidated{
		PointId: sealed.PointId, Object: proto.Clone(sealed.SourceObject).(*agentpb.BackupObjectIdentity),
		Evidence: proto.Clone(sealed.ExpectedEvidence).(*agentpb.BackupArtifactEvidence),
		Finals: &agentpb.BackupStagingFinals{
			SourceRelativeName: download.SourceEvidence.Name, StoredRelativeName: download.StoredEvidence.Name, SameInode: &sameInode,
		},
		Archive: &agentpb.BackupRestoreArtifactValidated_Config{
			Config: proto.Clone(expected).(*agentpb.BackupConfigArchiveEvidence),
		},
	}
	readerClosed = true
	if err := reader.Close(); err != nil {
		return nil, err
	}
	keep = true
	return &RestoreArtifact{validated: validated, evidence: proof, authority: proto.CloneOf(sealed.GetConfig())}, nil
}

func (artifact *RestoreArtifact) Evidence() *agentpb.BackupRestoreArtifactValidated {
	if artifact == nil || artifact.evidence == nil {
		return nil
	}
	return proto.Clone(artifact.evidence).(*agentpb.BackupRestoreArtifactValidated)
}

func (artifact *RestoreArtifact) BeginTransfer(ctx context.Context) (*backupconfig.PassTwo, error) {
	if artifact == nil || artifact.validated == nil {
		return nil, invalidRestoreArtifact()
	}
	return artifact.validated.BeginPassTwo(ctx)
}

func (artifact *RestoreArtifact) Close(ctx context.Context) error {
	if artifact == nil || artifact.validated == nil {
		return nil
	}
	return artifact.validated.Close(ctx)
}

// RequiredRestoreBytes includes both durable representations and the additional
// owned plaintext validation spool, using the exact same-filesystem bound.
func RequiredRestoreBytes(evidence *agentpb.BackupArtifactEvidence) (uint64, error) {
	if evidence == nil || evidence.SourceSizeBytes < 2*backupconfig.TarBlockBytes ||
		evidence.SourceSizeBytes > backupconfig.MaxSourceBytes {
		return 0, invalidRestoreArtifact()
	}
	stored, err := backupconfig.AgeStoredSize(evidence.SourceSizeBytes)
	if err != nil || evidence.StoredSizeBytes != stored {
		return 0, invalidRestoreArtifact()
	}
	return 2*evidence.SourceSizeBytes + stored, nil
}

func restoreContentMatches(actual backupconfig.ContentAuthority, expected *agentpb.BackupConfigContentAuthority) bool {
	return expected != nil && bytes.Equal(actual.ManifestSHA256[:], expected.ManifestSha256) &&
		actual.EntryCount == expected.EntryCount && actual.TotalSelectedValueBytes == expected.TotalSelectedValueBytes &&
		actual.ManifestSizeBytes == expected.ManifestSizeBytes && actual.SourceSizeBytes == expected.SourceSizeBytes
}

func invalidRestoreArtifact() error {
	return errs.New(errs.KindStateConflict, "Config restore archive differs from its sealed Recovery Point")
}
