package backupplanning

import (
	"context"
	"encoding/json"

	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backingpostgresrelease "github.com/AlanD20/groundplane/internal/infra/etcd/backingpostgresrelease"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	environmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Planner) prepareManualDatabaseSource(
	ctx context.Context,
	attempt backupruntime.BackupRunSourceAttemptRecord,
	consumerEnvironmentID string,
	fixedRevision int64,
	resolveDatabase BackupDatabaseIdentityResolver,
) (backupruntime.BackupRunSourceAttemptRecord, error) {
	read, err := repository.reader.ReadFixedKeys(
		ctx,
		[]string{
			attachrecord.AttachKey(attempt.TargetID),
			attachrecord.AttachFactsKey(attempt.TargetID),
		},
		fixedRevision,
	)
	if err != nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, err
	}
	defer etcdstore.ClearValues(read.Values)
	if len(read.Values) != 2 || read.Values[0] == nil || read.Values[1] == nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(errs.KindStateConflict,
			"database Attach evidence is unavailable")
	}
	attach, attachErr := attachrecord.DecodeAttachRecord(read.Values[0].Value)
	facts, factsErr := attachrecord.DecodeAttachEncryptedFacts(read.Values[1].Value)
	if attachErr != nil || factsErr != nil || attach.ID != attempt.TargetID ||
		attach.EnvironmentID != consumerEnvironmentID || !attach.OwnsCredential() ||
		attach.Status != backupAttachStatusReady || facts.AttachID != attach.ID {
		clear(facts.Ciphertext)
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(errs.KindStateConflict,
			"database Attach evidence changed")
	}
	defer clear(facts.Ciphertext)

	backingRead, err := repository.reader.ReadFixedKeys(ctx, []string{
		hierarchyrecord.ProjectKey(attach.BackingProjectID),
		hierarchyrecord.EnvironmentKey(attach.BackingEnvironmentID),
		blueprints.EnvironmentBlueprintHeadKey(attach.BackingEnvironmentID),
	}, fixedRevision)
	if err != nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, err
	}
	defer etcdstore.ClearValues(backingRead.Values)
	if len(backingRead.Values) != 3 || backingRead.Values[0] == nil || backingRead.Values[1] == nil ||
		backingRead.Values[2] == nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(errs.KindStateConflict,
			"database backing evidence is unavailable")
	}
	project, projectErr := hierarchyrecord.DecodeProject(backingRead.Values[0].Value)
	environment, environmentErr := hierarchyrecord.DecodeEnvironment(backingRead.Values[1].Value)
	service, serviceErr := environmentqueries.FindServiceAtRevision(ctx, repository.store,
		attach.BackingServiceID, fixedRevision)
	if projectErr != nil || environmentErr != nil || serviceErr != nil ||
		project.Kind != hierarchyrecord.ProjectKindBacking || environment.ProjectID != project.ID ||
		environment.ID != attach.BackingEnvironmentID || service.Record.EnvironmentID != environment.ID ||
		service.Revision != backingRead.Values[2].ModRevision {
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(errs.KindStateConflict,
			"database backing evidence changed")
	}

	identity := BackupDatabaseIdentity{}
	err = resolveDatabase(ctx, etcdstore.Versioned[attachrecord.Record]{Record: attach,
		Revision: read.Values[0].ModRevision, ReadRevision: fixedRevision}, facts,
		func(value BackupDatabaseIdentity) error { identity = value; return nil })
	if err != nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, err
	}
	if identity.Database == "" || identity.Role == "" {
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(errs.KindStateConflict,
			"database Attach identity is incomplete")
	}

	attempt.TargetRevision = read.Values[0].ModRevision
	switch service.Record.Desired.Adapter {
	case "postgres":
		releaseRead, err := repository.reader.ReadFixedKeys(ctx, []string{
			backingpostgresrelease.Key(environment.ID, service.Record.Desired.ID),
		}, fixedRevision)
		if err != nil {
			return backupruntime.BackupRunSourceAttemptRecord{}, err
		}
		defer etcdstore.ClearValues(releaseRead.Values)
		if len(releaseRead.Values) != 1 || releaseRead.Values[0] == nil {
			return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(errs.KindStateConflict,
				"PostgreSQL managed release evidence is unavailable")
		}
		release, err := backingpostgresrelease.Decode(releaseRead.Values[0].Value)
		if err != nil || release.EnvironmentID != environment.ID || release.ServiceID != service.Record.Desired.ID ||
			service.Record.Runtime.PostgresToolsImage != release.Release.Image {
			return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(errs.KindStateConflict,
				"PostgreSQL managed release evidence changed")
		}
		encoded, err := json.Marshal(release.Release)
		if err != nil {
			return backupruntime.BackupRunSourceAttemptRecord{}, errs.Wrap(errs.KindInternal, err)
		}
		attempt.Format = backupruntime.BackupRuntimeFormatPostgres
		attempt.Snapshot.Postgres = &backupruntime.BackupPostgresSourceSnapshot{
			ConsumerEnvironmentID: consumerEnvironmentID, AttachID: attach.ID, AttachRevision: read.Values[0].ModRevision,
			BackingProjectID: project.ID, BackingProjectRevision: backingRead.Values[0].ModRevision,
			BackingEnvironmentID: environment.ID, BackingEnvironmentRevision: backingRead.Values[1].ModRevision,
			BackingServiceID: service.Record.Desired.ID, BackingServiceRevision: backingRead.Values[2].ModRevision,
			ConsumerServiceID: attach.ServiceID, AttachFactsRevision: read.Values[1].ModRevision,
			Database: identity.Database, Role: identity.Role, ManagedReleaseIndex: string(encoded),
		}
	case "mysql":
		if service.Record.Desired.AdapterVersion != "8.4" || service.Record.Runtime.PostgresToolsImage != "" {
			return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(errs.KindStateConflict,
				"MySQL 8.4 backing evidence changed")
		}
		attempt.Format = backupruntime.BackupRuntimeFormatMySQL
		attempt.Snapshot.MySQL = &backupruntime.BackupMySQLSourceSnapshot{
			ConsumerEnvironmentID: consumerEnvironmentID, AttachID: attach.ID, AttachRevision: read.Values[0].ModRevision,
			BackingProjectID: project.ID, BackingProjectRevision: backingRead.Values[0].ModRevision,
			BackingEnvironmentID: environment.ID, BackingEnvironmentRevision: backingRead.Values[1].ModRevision,
			BackingServiceID: service.Record.Desired.ID, BackingServiceRevision: backingRead.Values[2].ModRevision,
			ConsumerServiceID: attach.ServiceID, AttachFactsRevision: read.Values[1].ModRevision,
			Database: identity.Database, Role: identity.Role, AdapterVersion: service.Record.Desired.AdapterVersion,
		}
	default:
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(errs.KindStrategyNotImplemented,
			"backup Attach database family is not implemented")
	}
	return attempt, nil
}
