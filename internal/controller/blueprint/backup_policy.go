package blueprint

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type environmentBlueprintBackupRepository interface {
	PrepareEnvironmentBlueprintBackupPolicy(
		context.Context,
		etcd.EnvironmentBlueprintBackupPolicyInput,
	) (etcd.BlueprintBackupPolicyPreparation, error)
	ValidateEnvironmentBlueprintBackupPolicy(
		context.Context,
		etcd.EnvironmentBlueprintBackupPolicyInput,
	) error
	GetEnvironmentBlueprintBackupPolicySnapshot(
		context.Context,
		string,
		int64,
	) (environmentBlueprintBackupPolicySnapshot, error)
}

type environmentBlueprintBackupKeyFactory interface {
	Create(context.Context) (etcd.BackupPolicyInitialKeyMaterial, error)
}

type environmentBlueprintBackupPolicySnapshot struct {
	policy         etcd.BackupPolicyRecord
	sources        []etcd.BackupSourceRecord
	connectorName  string
	connectorFound bool
	found          bool
}

func (snapshot environmentBlueprintBackupPolicySnapshot) projection() *etcd.EnvironmentBlueprintBackupPolicy {
	if !snapshot.found {
		return nil
	}
	result := &etcd.EnvironmentBlueprintBackupPolicy{
		Enabled: snapshot.policy.Enabled, Frequency: snapshot.policy.Frequency,
		Keep: snapshot.policy.Keep, Encryption: snapshot.policy.Encryption,
		ConnectorID: snapshot.policy.ConnectorID,
		Sources:     make([]etcd.EnvironmentBlueprintBackupPolicySource, len(snapshot.sources)),
	}
	for index, source := range snapshot.sources {
		result.Sources[index] = etcd.EnvironmentBlueprintBackupPolicySource{
			ID: source.ID, Kind: source.Kind, TargetID: source.TargetID,
		}
	}
	return result
}

func (repository *durableRepository) PrepareEnvironmentBlueprintBackupPolicy(
	ctx context.Context,
	input etcd.EnvironmentBlueprintBackupPolicyInput,
) (etcd.BlueprintBackupPolicyPreparation, error) {
	return repository.backups.PrepareEnvironmentBlueprintBackupPolicy(ctx, input)
}

func (repository *durableRepository) ValidateEnvironmentBlueprintBackupPolicy(
	ctx context.Context,
	input etcd.EnvironmentBlueprintBackupPolicyInput,
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
	projection etcd.EnvironmentComposeProjection,
	attaches preparedBlueprintAttaches,
	allocateNamed func(ids.Kind, string) string,
	createdAt time.Time,
) (*etcd.EnvironmentBlueprintBackupPolicy, etcd.BlueprintBackupPolicyPreparation, error) {
	if service.backups == nil {
		return nil, etcd.BlueprintBackupPolicyPreparation{}, errs.New(
			errs.KindInternal, "Environment Blueprint Backup repository is not configured",
		)
	}
	if authored == nil {
		snapshot, err := service.backups.GetEnvironmentBlueprintBackupPolicySnapshot(ctx, environmentID, readRevision)
		if err != nil {
			return nil, etcd.BlueprintBackupPolicyPreparation{}, err
		}
		prepared, err := service.backups.PrepareEnvironmentBlueprintBackupPolicy(
			ctx,
			etcd.EnvironmentBlueprintBackupPolicyInput{
				EnvironmentID: environmentID, TaskID: taskID, ReadRevision: readRevision,
				Retain: true, Projection: projection, AttachPreparation: attaches.publication,
				CreatedAt: createdAt,
			},
		)
		if err != nil {
			return nil, etcd.BlueprintBackupPolicyPreparation{}, err
		}
		return snapshot.projection(), prepared, nil
	}
	sources := make([]etcd.EnvironmentBlueprintBackupPolicySourceInput, len(authored.Sources))
	for index, source := range authored.Sources {
		targetID, err := resolveEnvironmentBlueprintBackupTarget(environmentID, source, projection, attaches.effective)
		if err != nil {
			return nil, etcd.BlueprintBackupPolicyPreparation{}, err
		}
		sources[index] = etcd.EnvironmentBlueprintBackupPolicySourceInput{
			CandidateID: allocateNamed(
				ids.KindBackupSource,
				"backup-source/"+string(source.Kind)+"/"+targetID,
			),
			Kind: source.Kind, TargetID: targetID,
		}
	}
	prepared, err := service.backups.PrepareEnvironmentBlueprintBackupPolicy(
		ctx,
		etcd.EnvironmentBlueprintBackupPolicyInput{
			EnvironmentID: environmentID, TaskID: taskID, ReadRevision: readRevision,
			Enabled: authored.Enabled, Frequency: authored.Frequency, Keep: authored.Keep,
			Encryption: authored.Encryption, ConnectorName: authored.Connector, Sources: sources,
			Projection: projection, AttachPreparation: attaches.publication, CreatedAt: createdAt,
		},
	)
	if err != nil {
		return nil, etcd.BlueprintBackupPolicyPreparation{}, err
	}
	if prepared.RequiresInitialKey() {
		if service.backupKeys == nil {
			prepared.Clear()
			return nil, etcd.BlueprintBackupPolicyPreparation{}, errs.New(
				errs.KindInternal, "Environment Blueprint Backup key factory is not configured",
			)
		}
		material, createErr := service.backupKeys.Create(ctx)
		if createErr != nil {
			prepared.Clear()
			return nil, etcd.BlueprintBackupPolicyPreparation{}, createErr
		}
		defer clear(material.Ciphertext)
		if supplyErr := prepared.SupplyInitialKey(material); supplyErr != nil {
			prepared.Clear()
			return nil, etcd.BlueprintBackupPolicyPreparation{}, supplyErr
		}
	}
	return prepared.Projection(), prepared, nil
}

func (service *Service) validateEnvironmentBlueprintBackup(
	ctx context.Context,
	environmentID string,
	readRevision int64,
	authored *core.BackupSpec,
	projection etcd.EnvironmentComposeProjection,
	attaches []etcd.Versioned[attachrecord.Record],
) error {
	if authored == nil {
		return nil
	}
	if service.backups == nil {
		return errs.New(errs.KindInternal, "Environment Blueprint Backup repository is not configured")
	}
	sources := make([]etcd.EnvironmentBlueprintBackupPolicySourceInput, len(authored.Sources))
	for index, source := range authored.Sources {
		targetID, err := resolveEnvironmentBlueprintBackupTarget(environmentID, source, projection, attaches)
		if err != nil {
			return err
		}
		sources[index] = etcd.EnvironmentBlueprintBackupPolicySourceInput{Kind: source.Kind, TargetID: targetID}
	}
	return service.backups.ValidateEnvironmentBlueprintBackupPolicy(
		ctx,
		etcd.EnvironmentBlueprintBackupPolicyInput{
			EnvironmentID: environmentID, ReadRevision: readRevision,
			Enabled: authored.Enabled, Frequency: authored.Frequency, Keep: authored.Keep,
			Encryption: authored.Encryption, ConnectorName: authored.Connector, Sources: sources,
		},
	)
}

func resolveEnvironmentBlueprintBackupTarget(
	environmentID string,
	source core.BackupSourceSpec,
	projection etcd.EnvironmentComposeProjection,
	attaches []etcd.Versioned[attachrecord.Record],
) (string, error) {
	switch source.Kind {
	case core.BackupSourceConfig:
		if source.Ref == "" {
			return environmentID, nil
		}
	case core.BackupSourceVolume:
		for _, volume := range projection.Volumes {
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

func (service *Service) environmentBlueprintAuthoringBackup(
	ctx context.Context,
	snapshot environmentBlueprintSnapshot,
	attaches []etcd.Versioned[attachrecord.Record],
) (*core.BackupSpec, error) {
	if service.backups == nil {
		return nil, nil
	}
	stored, err := service.backups.GetEnvironmentBlueprintBackupPolicySnapshot(
		ctx, snapshot.environment.Record.ID, 0,
	)
	if err != nil || !stored.found {
		return nil, err
	}
	configured := stored.policy.Frequency != "" || stored.policy.Keep != 0 ||
		stored.policy.Encryption != "" || stored.policy.ConnectorID != "" || len(stored.sources) != 0
	if !configured {
		return &core.BackupSpec{}, nil
	}
	if stored.policy.Enabled && !stored.connectorFound {
		return nil, errs.New(errs.KindInternal, "enabled Blueprint Backup Connector is missing")
	}
	result := &core.BackupSpec{
		Enabled: stored.policy.Enabled, Frequency: stored.policy.Frequency,
		Keep: stored.policy.Keep, Encryption: stored.policy.Encryption,
		Sources: make([]core.BackupSourceSpec, len(stored.sources)),
	}
	if stored.connectorFound {
		result.Connector = stored.connectorName
	}
	attachNames := make(map[string]string, len(attaches))
	for _, attach := range attaches {
		attachNames[attach.Record.ID] = attach.Record.Name
	}
	volumeSlugs := make(map[string]string)
	if snapshot.hasHead {
		for _, volume := range snapshot.projection.Record.Volumes {
			volumeSlugs[volume.ID] = volume.Slug
		}
	}
	for index, source := range stored.sources {
		ref := ""
		switch source.Kind {
		case core.BackupSourceConfig:
			if source.TargetID != snapshot.environment.Record.ID {
				return nil, errs.New(errs.KindInternal, "Blueprint Backup config source is corrupt")
			}
		case core.BackupSourceAttach:
			ref = attachNames[source.TargetID]
		case core.BackupSourceVolume:
			ref = volumeSlugs[source.TargetID]
		}
		if source.Kind != core.BackupSourceConfig && ref == "" {
			return nil, errs.New(errs.KindInternal, "Blueprint Backup source target is missing")
		}
		result.Sources[index] = core.BackupSourceSpec{Kind: source.Kind, Ref: ref}
	}
	return result, nil
}
