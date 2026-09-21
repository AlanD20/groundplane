package etcd

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	environmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) prepareManualPostgresSource(
	ctx context.Context,
	attempt backupruntime.BackupRunSourceAttemptRecord,
	consumerEnvironmentID string,
	fixedRevision int64,
	resolvePostgres BackupPostgresIdentityResolver,
) (backupruntime.BackupRunSourceAttemptRecord, error) {
	read, err := repository.ReadFixedKeys(
		ctx, []string{attachrecord.AttachKey(attempt.TargetID), attachrecord.AttachFactsKey(attempt.TargetID)}, fixedRevision,
	)
	if err != nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, err
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[0] == nil || read.Values[1] == nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(
			errs.KindStateConflict,
			"postgres Attach evidence is unavailable",
		)
	}
	attach, attachErr := attachrecord.DecodeAttachRecord(read.Values[0].Value)
	facts, factsErr := attachrecord.DecodeAttachEncryptedFacts(read.Values[1].Value)
	if attachErr != nil || factsErr != nil || attach.ID != attempt.TargetID ||
		attach.EnvironmentID != consumerEnvironmentID || !attach.OwnsCredential() ||
		attach.Status != backupAttachStatusReady || facts.AttachID != attach.ID {
		clear(facts.Ciphertext)
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(
			errs.KindStateConflict,
			"postgres Attach evidence changed",
		)
	}
	defer clear(facts.Ciphertext)
	backingRead, err := repository.ReadFixedKeys(ctx, []string{
		hierarchyrecord.ProjectKey(attach.BackingProjectID),
		hierarchyrecord.EnvironmentKey(attach.BackingEnvironmentID),
		blueprints.EnvironmentBlueprintHeadKey(attach.BackingEnvironmentID),
	}, fixedRevision)
	if err != nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, err
	}
	defer etcdstore.ClearValues(backingRead.Values)
	if backingRead.Values[0] == nil || backingRead.Values[1] == nil ||
		backingRead.Values[2] == nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(
			errs.KindStateConflict,
			"postgres backing evidence is unavailable",
		)
	}
	project, projectErr := hierarchyrecord.DecodeProject(backingRead.Values[0].Value)
	environment, environmentErr := hierarchyrecord.DecodeEnvironment(backingRead.Values[1].Value)
	service, serviceErr := environmentqueries.FindServiceAtRevision(ctx, repository.store, attach.BackingServiceID, fixedRevision)
	if projectErr != nil || environmentErr != nil || serviceErr != nil || project.Kind != hierarchyrecord.ProjectKindBacking ||
		environment.ProjectID != project.ID ||
		environment.ID != attach.BackingEnvironmentID ||
		service.Record.EnvironmentID != environment.ID ||
		service.Record.Desired.Adapter != "postgres:16" ||
		service.Record.Desired.Image != "postgres:16-alpine" ||
		service.Revision != backingRead.Values[2].ModRevision {
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(
			errs.KindStateConflict,
			"postgres:16 backing evidence changed",
		)
	}
	identity := BackupPostgresIdentity{}
	err = resolvePostgres(
		ctx,
		etcdstore.Versioned[attachrecord.Record]{
			Record:       attach,
			Revision:     read.Values[0].ModRevision,
			ReadRevision: fixedRevision,
		},
		facts,
		func(value BackupPostgresIdentity) error {
			identity = value
			return nil
		},
	)
	if err != nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, err
	}
	if identity.Database == "" || identity.Role == "" {
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(
			errs.KindStateConflict,
			"postgres Attach identity is incomplete",
		)
	}
	attempt.TargetRevision = read.Values[0].ModRevision
	attempt.Format = backupruntime.BackupRuntimeFormatPostgres
	attempt.Snapshot.Postgres = &backupruntime.BackupPostgresSourceSnapshot{
		ConsumerEnvironmentID:      consumerEnvironmentID,
		AttachID:                   attach.ID,
		AttachRevision:             read.Values[0].ModRevision,
		BackingProjectID:           project.ID,
		BackingProjectRevision:     backingRead.Values[0].ModRevision,
		BackingEnvironmentID:       environment.ID,
		BackingEnvironmentRevision: backingRead.Values[1].ModRevision,
		BackingServiceID:           service.Record.Desired.ID,
		BackingServiceRevision:     backingRead.Values[2].ModRevision,
		AttachFactsRevision:        read.Values[1].ModRevision,
		Database:                   identity.Database,
		Role:                       identity.Role,
	}
	return attempt, nil
}
