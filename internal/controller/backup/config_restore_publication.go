package backup

import (
	"context"
	"encoding/hex"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/backupconfigmaterialization"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/entry"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ConfigRestorePublication is one complete replacement prepared from the
// verified generation and sealed predecessor. Preparation cannot publish a
// partial set, resolve current facts, or reread current Secret values.
type ConfigRestorePublication struct {
	SourceSeal       etcdstore.Versioned[backupconfiguration.ConfigRestoreGenerationRecord]
	RootRevision     int64
	DeleteEntries    []entries.Record
	Projection       projectionrecord.EnvironmentComposeProjection
	Identities       projectionrecord.EnvironmentOwnedIdentities
	Revision         blueprints.ConfigRestoreRevision
	Materializations []composerender.EnvironmentEntryMaterialization
	filePlan         *backupconfigmaterialization.Plan
	fileEntries      []backupconfig.Entry
	fileStepID       string
	FileProof        *agentpb.BackupConfigMaterializationVerified
	generation       *ConfigRestoreGeneration
	createdAt        time.Time
}

func BuildConfigRestorePublication(ctx context.Context, generation *ConfigRestoreGeneration,
	baselineInput projectionrecord.EnvironmentDesiredInput,
	baselineProjection projectionrecord.EnvironmentComposeProjection,
	baselineIdentities projectionrecord.EnvironmentOwnedIdentities,
	pointID, planID string,
) (*ConfigRestorePublication, error) {
	if ctx == nil || generation == nil {
		return nil, configSnapshotInvalid()
	}
	owner := generation.owner
	if baselineInput.EnvironmentID != owner.EnvironmentID || baselineInput.RevisionID != owner.BaselineRevisionID ||
		baselineProjection.EnvironmentID != owner.EnvironmentID || baselineProjection.RevisionID != owner.BaselineRevisionID ||
		baselineIdentities.EnvironmentID != owner.EnvironmentID || baselineIdentities.RevisionID != owner.BaselineRevisionID ||
		baselineInput.RenderGeneration == ^uint64(0) || baselineInput.RenderGeneration+1 != owner.RenderGeneration ||
		baselineProjection.RenderGeneration != baselineInput.RenderGeneration || baselineIdentities.RenderGeneration != baselineInput.RenderGeneration {
		return nil, configSnapshotGuardConflict()
	}
	serviceNames := make(map[string]string, len(baselineProjection.DesiredServices))
	for _, service := range baselineProjection.DesiredServices {
		serviceNames[service.Desired.ID] = service.Desired.Name
	}
	previousEntries := make(map[string]entries.Record, len(baselineProjection.Entries))
	for _, record := range baselineProjection.Entries {
		previousEntries[record.Entry.ID] = record
	}
	restored := make([]entries.Record, 0, generation.seal.Record.Completed.Content.EntryCount)
	fileEntries := make([]backupconfig.Entry, 0, generation.seal.Record.Completed.Content.EntryCount)
	var metadataBytes int
	if err := generation.VisitEntries(ctx, func(_ uint32, archived backupconfig.Entry, value []byte) error {
		record, err := restoreConfigEntryRecord(owner.EnvironmentID, owner.GenerationID, archived, value, serviceNames)
		if err != nil {
			return err
		}
		if previous, found := previousEntries[record.Entry.ID]; found {
			if previous.Entry.Kind != record.Entry.Kind || previous.Entry.Key != record.Entry.Key || previous.Entry.Path != record.Entry.Path {
				return configSnapshotGuardConflict()
			}
			record.BlueprintKey = previous.BlueprintKey
		}
		encoded, err := entries.EncodeRecord(record)
		metadataBytes += len(encoded)
		clear(encoded)
		if err != nil || metadataBytes > projectionrecord.EnvironmentBlueprintProjectionMaxBytes {
			return configSnapshotGuardConflict()
		}
		restored = append(restored, record)
		archived.Exposure.ServiceIDs = append([]string(nil), archived.Exposure.ServiceIDs...)
		fileEntries = append(fileEntries, archived)
		return nil
	}); err != nil {
		return nil, err
	}
	projection, materializations, err := composerender.ProjectEnvironmentEntryMutation(baselineProjection,
		composerender.EnvironmentEntryArtifactMutation{RevisionID: owner.Transfer.Binding.TaskID,
			ArtifactID: owner.GenerationID, PlanID: planID, RenderGeneration: owner.RenderGeneration, Entries: restored})
	if err != nil {
		return nil, err
	}
	if err := projectionrecord.ValidateEnvironmentComposeProjectionAdvance(baselineProjection, true, projection); err != nil {
		return nil, err
	}
	authored, err := entry.BlueprintAuthoring(restored)
	if err != nil {
		return nil, err
	}
	desired := projectionrecord.CloneEnvironmentDesiredInput(baselineInput)
	desired.RevisionID, desired.RenderGeneration, desired.Input.Entries = projection.RevisionID, projection.RenderGeneration, authored
	identities, err := projectionrecord.OwnedIdentitiesFromProjection(projection)
	if err != nil {
		return nil, err
	}
	identities.Attaches = append([]projectionrecord.OwnedIdentity(nil), baselineIdentities.Attaches...)
	identities.Scripts = append([]projectionrecord.OwnedIdentity(nil), baselineIdentities.Scripts...)
	identities, err = projectionrecord.RetainOwnedIdentityBirths(identities, baselineIdentities)
	if err != nil {
		return nil, err
	}
	dependencyDigest, err := blueprints.EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		return nil, err
	}
	sourceDigest, err := hex.DecodeString(owner.SourceSHA256)
	if err != nil || len(sourceDigest) != 32 {
		return nil, configSnapshotGuardConflict()
	}
	audit := blueprints.EnvironmentConfigRestoreAudit{
		BaseRevisionID: owner.BaselineRevisionID,
		PointID:        pointID,
		GenerationID:   owner.GenerationID,
	}
	copy(audit.SourceSHA256[:], sourceDigest)
	revision, err := blueprints.BuildConfigRestoreRevision(desired, audit, owner.BaselineHeadRevision, dependencyDigest)
	if err != nil {
		return nil, err
	}
	return &ConfigRestorePublication{
		SourceSeal:       generation.Seal(),
		DeleteEntries:    configRestoreDeletedEntries(baselineProjection.Entries, restored),
		Projection:       projection,
		Identities:       identities,
		Revision:         revision,
		Materializations: materializations,
		fileEntries:      fileEntries,
	}, nil
}

func configRestoreDeletedEntries(baseline, restored []entries.Record) []entries.Record {
	retained := make(map[string]struct{}, len(restored))
	for _, record := range restored {
		retained[record.Entry.ID] = struct{}{}
	}
	var result []entries.Record
	for _, record := range baseline {
		if _, keep := retained[record.Entry.ID]; !keep {
			result = append(result, record)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Entry.ID < result[j].Entry.ID })
	return result
}

func restoreConfigEntryRecord(environmentID, generationID string, archived backupconfig.Entry,
	value []byte, serviceNames map[string]string,
) (entries.Record, error) {
	if backupconfig.ValidateEntry(archived) != nil {
		return entries.Record{}, configSnapshotInvalid()
	}
	desired := core.EnvEntry{ID: archived.ID, Secret: archived.Secret}
	if archived.Metadata.Kind == backupconfig.MetadataEnvironment {
		desired.Kind, desired.Key = core.EntryKindEnv, archived.Metadata.Environment.Key
	} else {
		file := archived.Metadata.File
		uid, gid := file.UID, file.GID
		desired.Kind, desired.Path, desired.UID, desired.GID = core.EntryKindFile, file.Path, &uid, &gid
	}
	if archived.Exposure.Kind == backupconfig.ExposureAll {
		desired.Exposure = []string{"all"}
	} else {
		for _, serviceID := range archived.Exposure.ServiceIDs {
			name, found := serviceNames[serviceID]
			if !found {
				return entries.Record{}, configSnapshotGuardConflict()
			}
			desired.Exposure = append(desired.Exposure, name)
		}
		sort.Strings(desired.Exposure)
	}
	switch archived.Source.Kind {
	case backupconfig.SourceLiteral:
		desired.Source.Kind = core.SourceLiteral
		if !archived.Secret {
			if !utf8.Valid(value) {
				return entries.Record{}, configSnapshotInvalid()
			}
			desired.Source.Literal = string(value)
		}
	case backupconfig.SourceSecretReference:
		desired.Source = core.EntrySource{
			Kind:      core.SourceSecretRef,
			SecretRef: archived.Source.SecretReference.AuthoredKey,
		}
	case backupconfig.SourceFact:
		fact := archived.Source.Fact
		desired.Source = core.EntrySource{
			Kind: core.SourceFact,
			Fact: &core.FactRef{Attach: fact.AttachID, Grant: fact.GrantAttachID, Key: fact.Fact},
		}
	default:
		return entries.Record{}, configSnapshotInvalid()
	}
	return entries.NewRecord(environmentID, desired, generationID)
}

func (publication *ConfigRestorePublication) Clear() {
	publication.Revision.Clear()
	clear(publication.Projection.ComposeArtifact)
	clear(publication.Projection.NormalizedCompose)
}
