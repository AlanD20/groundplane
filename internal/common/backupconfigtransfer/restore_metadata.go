package backupconfigtransfer

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// EncodeRestoreMetadata reproduces the typed restore descriptor from the
// validated archive. Capture-only resource revisions never enter this path.
func EncodeRestoreMetadata(ctx context.Context, direction backupconfig.TransferDirection,
	ordinal uint32, entry backupconfig.Entry,
) ([]byte, error) {
	if ctx == nil || direction != backupconfig.TransferRestore || ordinal == 0 || ordinal > backupconfig.MaxEntries {
		return nil, invalid("restore metadata encoding identity is invalid")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := backupconfig.ValidateEntry(entry); err != nil {
		return nil, err
	}
	secret := entry.Secret
	value := &agentpb.BackupConfigRestoreEntry{
		EntryId: entry.ID, Secret: &secret,
		Metadata: &agentpb.BackupConfigEntryMetadata{},
		Exposure: &agentpb.BackupConfigEntryExposure{},
		Source:   &agentpb.BackupConfigArtifactDesiredSource{},
		SelectedValue: &agentpb.BackupConfigSelectedValue{
			SizeBytes: entry.Value.SizeBytes, Sha256: append([]byte(nil), entry.Value.SHA256[:]...),
		},
	}
	switch entry.Metadata.Kind {
	case backupconfig.MetadataEnvironment:
		value.Metadata.Metadata = &agentpb.BackupConfigEntryMetadata_Environment{
			Environment: &agentpb.BackupConfigEnvironment{Name: entry.Metadata.Environment.Key},
		}
	case backupconfig.MetadataFile:
		file := entry.Metadata.File
		value.Metadata.Metadata = &agentpb.BackupConfigEntryMetadata_File{
			File: &agentpb.BackupConfigFile{Path: file.Path, Mode: file.Mode, Uid: file.UID, Gid: file.GID},
		}
	}
	switch entry.Exposure.Kind {
	case backupconfig.ExposureAll:
		value.Exposure.Exposure = &agentpb.BackupConfigEntryExposure_All{All: &agentpb.BackupConfigExposureAll{}}
	case backupconfig.ExposureServices:
		value.Exposure.Exposure = &agentpb.BackupConfigEntryExposure_Services{
			Services: &agentpb.BackupConfigExposureServices{
				ServiceIds: append([]string(nil), entry.Exposure.ServiceIDs...),
			},
		}
	}
	switch entry.Source.Kind {
	case backupconfig.SourceLiteral:
		value.Source.Source = &agentpb.BackupConfigArtifactDesiredSource_Literal{
			Literal: &agentpb.BackupConfigLiteralSource{},
		}
	case backupconfig.SourceSecretReference:
		value.Source.Source = &agentpb.BackupConfigArtifactDesiredSource_SecretRef{
			SecretRef: &agentpb.BackupConfigArtifactSecretRefSource{
				AuthoredKey: entry.Source.SecretReference.AuthoredKey,
			},
		}
	case backupconfig.SourceFact:
		fact := &agentpb.BackupConfigArtifactFactSource{
			AttachId: entry.Source.Fact.AttachID,
			Fact:     entry.Source.Fact.Fact,
		}
		if entry.Source.Fact.GrantAttachID != "" {
			grant := entry.Source.Fact.GrantAttachID
			fact.GrantAttachId = &grant
		}
		value.Source.Source = &agentpb.BackupConfigArtifactDesiredSource_FactRef{FactRef: fact}
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(encoded) == 0 || len(encoded) > backupconfig.MaxCanonicalEntryBytes {
		return nil, invalid("restore metadata exceeds its per-Entry byte limit")
	}
	return encoded, nil
}
