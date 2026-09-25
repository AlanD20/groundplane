package blueprint

import (
	"context"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backuppolicymutations "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	blueprintplanning "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintplanning"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type environmentBlueprintBackupRepository interface {
	PrepareEnvironmentBlueprintBackupPolicy(
		context.Context,
		blueprintplanning.EnvironmentBlueprintBackupPolicyInput,
	) (blueprintplanning.BlueprintBackupPolicyPreparation, error)
	ValidateEnvironmentBlueprintBackupPolicy(
		context.Context,
		blueprintplanning.EnvironmentBlueprintBackupPolicyInput,
	) error
	GetEnvironmentBlueprintBackupPolicySnapshot(
		context.Context,
		string,
		int64,
	) (environmentBlueprintBackupPolicySnapshot, error)
}

type environmentBlueprintBackupKeyFactory interface {
	Create(context.Context) (backuppolicymutations.BackupPolicyInitialKeyMaterial, error)
}

type environmentBlueprintBackupPolicySnapshot struct {
	policy         backuppolicy.BackupPolicyRecord
	sources        []backuppolicy.BackupSourceRecord
	connectorName  string
	connectorFound bool
	found          bool
}

func environmentBlueprintBackupValidationTargets(
	snapshot environmentBlueprintSnapshot,
	parsed blueprintparser.Result,
	currentAttaches []etcdstore.Versioned[attachrecord.Record],
) ([]projectionrecord.EnvironmentVolumeIdentity, []etcdstore.Versioned[attachrecord.Record], error) {
	volumeSlugs, err := environmentBlueprintVolumeSlugsFromVolumes(
		parsed.Project, snapshot.volumes, snapshot.hasHead,
	)
	if err != nil {
		return nil, nil, err
	}
	byKey := make(map[string]projectionrecord.EnvironmentVolumeIdentity, len(snapshot.volumes))
	for _, volume := range snapshot.volumes {
		byKey[volume.Key] = volume
	}
	at := snapshot.environment.Record.CreatedAt
	if at.IsZero() {
		at = time.Unix(0, 0).UTC()
	}
	for key, label := range volumeSlugs {
		volume, found := byKey[key]
		if !found {
			volume = projectionrecord.EnvironmentVolumeIdentity{
				ID:  ids.DeriveAt(ids.KindVolume, at, snapshot.environment.Record.ID, "validate-volume/"+key),
				Key: key,
			}
		}
		volume.Slug = label
		byKey[key] = volume
	}
	volumes := make([]projectionrecord.EnvironmentVolumeIdentity, 0, len(byKey))
	for _, volume := range byKey {
		volumes = append(volumes, volume)
	}
	attaches := append([]etcdstore.Versioned[attachrecord.Record](nil), currentAttaches...)
	attachIDs := make(map[string]string, len(attaches)+len(parsed.Extensions.Attachments))
	for _, attach := range attaches {
		attachIDs[attach.Record.Name] = attach.Record.ID
	}
	for name := range parsed.Extensions.Attachments {
		if attachIDs[name] == "" {
			attachIDs[name] = ids.DeriveAt(ids.KindAttach, at, snapshot.environment.Record.ID, "validate-attach/"+name)
		}
	}
	for name, spec := range parsed.Extensions.Attachments {
		found := false
		for _, attach := range currentAttaches {
			found = found || attach.Record.Name == name
		}
		if found {
			continue
		}
		credentialID := attachIDs[spec.Credential.Attach]
		if spec.Credential.Mode == "new" {
			credentialID = attachIDs[name]
		}
		attaches = append(attaches, etcdstore.Versioned[attachrecord.Record]{Record: attachrecord.Record{
			ID: attachIDs[name], EnvironmentID: snapshot.environment.Record.ID,
			Name: name, CredentialAttachID: credentialID,
		}})
	}
	return volumes, attaches, nil
}

func (repository *durableRepository) PrepareEnvironmentBlueprintBackupPolicy(
	ctx context.Context,
	input blueprintplanning.EnvironmentBlueprintBackupPolicyInput,
) (blueprintplanning.BlueprintBackupPolicyPreparation, error) {
	return repository.backups.PrepareEnvironmentBlueprintBackupPolicy(ctx, input)
}

func (repository *durableRepository) ValidateEnvironmentBlueprintBackupPolicy(
	ctx context.Context,
	input blueprintplanning.EnvironmentBlueprintBackupPolicyInput,
) error {
	return repository.backups.ValidateEnvironmentBlueprintBackupPolicy(ctx, input)
}

func (repository *durableRepository) GetEnvironmentBlueprintBackupPolicySnapshot(
	ctx context.Context,
	environmentID string,
	revision int64,
) (environmentBlueprintBackupPolicySnapshot, error) {
	stored, err := repository.backups.GetEnvironmentBlueprintBackupPolicySnapshot(ctx, environmentID, revision)
	if err != nil {
		return environmentBlueprintBackupPolicySnapshot{}, err
	}
	return environmentBlueprintBackupPolicySnapshot{
		policy: stored.Policy, sources: stored.Sources, connectorName: stored.ConnectorName,
		connectorFound: stored.ConnectorFound, found: stored.Found,
	}, nil
}

func (service *Service) prepareEnvironmentBlueprintBackup(
	ctx context.Context,
	environmentID string,
	taskID string,
	readRevision int64,
	authored *core.BackupSpec,
	projection projectionrecord.EnvironmentComposeProjection,
	attaches preparedBlueprintAttaches,
	allocateNamed func(ids.Kind, string) string,
	createdAt time.Time,
) (*projectionrecord.EnvironmentBlueprintBackupPolicy, blueprintplanning.BlueprintBackupPolicyPreparation, error) {
	if service.backups == nil {
		return nil, blueprintplanning.BlueprintBackupPolicyPreparation{}, errs.New(
			errs.KindInternal, "Environment Blueprint Backup repository is not configured",
		)
	}
	if authored == nil {
		authored = &core.BackupSpec{}
	}
	sources := make([]blueprintplanning.EnvironmentBlueprintBackupPolicySourceInput, len(authored.Sources))
	for index, source := range authored.Sources {
		targetID, err := resolveEnvironmentBlueprintBackupTarget(
			environmentID, source, projection.Volumes, attaches.effective,
		)
		if err != nil {
			return nil, blueprintplanning.BlueprintBackupPolicyPreparation{}, err
		}
		sources[index] = blueprintplanning.EnvironmentBlueprintBackupPolicySourceInput{
			CandidateID: allocateNamed(
				ids.KindBackupSource,
				"backup-source/"+string(source.Kind)+"/"+targetID,
			),
			Kind: source.Kind, TargetID: targetID,
		}
	}
	prepared, err := service.backups.PrepareEnvironmentBlueprintBackupPolicy(
		ctx,
		blueprintplanning.EnvironmentBlueprintBackupPolicyInput{
			EnvironmentID: environmentID, TaskID: taskID, ReadRevision: readRevision,
			Enabled: authored.Enabled, Frequency: authored.Frequency, Keep: authored.Keep,
			Encryption: authored.Encryption, ConnectorName: authored.Connector, Sources: sources,
			Projection: projection, AttachPreparation: attaches.publication, CreatedAt: createdAt,
		},
	)
	if err != nil {
		return nil, blueprintplanning.BlueprintBackupPolicyPreparation{}, err
	}
	if prepared.RequiresInitialKey() {
		if service.backupKeys == nil {
			prepared.Clear()
			return nil, blueprintplanning.BlueprintBackupPolicyPreparation{}, errs.New(
				errs.KindInternal, "Environment Blueprint Backup key factory is not configured",
			)
		}
		material, createErr := service.backupKeys.Create(ctx)
		if createErr != nil {
			prepared.Clear()
			return nil, blueprintplanning.BlueprintBackupPolicyPreparation{}, createErr
		}
		defer clear(material.Ciphertext)
		if supplyErr := prepared.SupplyInitialKey(material); supplyErr != nil {
			prepared.Clear()
			return nil, blueprintplanning.BlueprintBackupPolicyPreparation{}, supplyErr
		}
	}
	return prepared.Projection(), prepared, nil
}

func (service *Service) validateEnvironmentBlueprintBackup(
	ctx context.Context,
	environmentID string,
	readRevision int64,
	authored *core.BackupSpec,
	volumes []projectionrecord.EnvironmentVolumeIdentity,
	attaches []etcdstore.Versioned[attachrecord.Record],
) error {
	if authored == nil {
		authored = &core.BackupSpec{}
	}
	if service.backups == nil {
		return errs.New(errs.KindInternal, "Environment Blueprint Backup repository is not configured")
	}
	sources := make([]blueprintplanning.EnvironmentBlueprintBackupPolicySourceInput, len(authored.Sources))
	for index, source := range authored.Sources {
		targetID, err := resolveEnvironmentBlueprintBackupTarget(environmentID, source, volumes, attaches)
		if err != nil {
			return err
		}
		sources[index] = blueprintplanning.EnvironmentBlueprintBackupPolicySourceInput{
			Kind:     source.Kind,
			TargetID: targetID,
		}
	}
	return service.backups.ValidateEnvironmentBlueprintBackupPolicy(
		ctx,
		blueprintplanning.EnvironmentBlueprintBackupPolicyInput{
			EnvironmentID: environmentID, ReadRevision: readRevision,
			Enabled: authored.Enabled, Frequency: authored.Frequency, Keep: authored.Keep,
			Encryption: authored.Encryption, ConnectorName: authored.Connector, Sources: sources,
		},
	)
}

func resolveEnvironmentBlueprintBackupTarget(
	environmentID string,
	source core.BackupSourceSpec,
	volumes []projectionrecord.EnvironmentVolumeIdentity,
	attaches []etcdstore.Versioned[attachrecord.Record],
) (string, error) {
	switch source.Kind {
	case core.BackupSourceConfig:
		if source.Ref == "" {
			return environmentID, nil
		}
	case core.BackupSourceVolume:
		for _, volume := range volumes {
			if volume.Slug == source.Ref {
				return volume.ID, nil
			}
		}
	case core.BackupSourceAttach:
		for _, attach := range attaches {
			if attach.Record.Name == source.Ref {
				if !attach.Record.OwnsCredential() {
					return "", errs.New(
						errs.KindValidationFailed,
						"Blueprint Backup Attach source must own credentials",
					)
				}
				return attach.Record.ID, nil
			}
		}
	}
	return "", errs.New(errs.KindValidationFailed, "Blueprint Backup source label was not found")
}
