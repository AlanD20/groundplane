package backupplanning

import (
	"encoding/hex"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ValidateBackupPostgresPublicationEvidence(
	values []*etcdstore.KeyValue,
	source backupruntime.BackupRunSourceAttemptRecord,
	snapshot backupruntime.BackupPostgresSourceSnapshot,
) error {
	if len(values) != 5 || values[0] == nil || values[1] == nil || values[2] == nil ||
		values[3] == nil || values[4] == nil || values[0].ModRevision != source.TargetRevision ||
		values[1].ModRevision != snapshot.AttachFactsRevision ||
		values[2].ModRevision != snapshot.BackingProjectRevision ||
		values[3].ModRevision != snapshot.BackingEnvironmentRevision ||
		values[4].ModRevision != snapshot.BackingServiceRevision {
		return errs.New(errs.KindStateConflict, "postgres backup publication evidence changed")
	}
	attach, attachErr := attachrecord.DecodeAttachRecord(values[0].Value)
	facts, factsErr := attachrecord.DecodeAttachEncryptedFacts(values[1].Value)
	defer clear(facts.Ciphertext)
	project, projectErr := hierarchyrecord.DecodeProject(values[2].Value)
	environment, environmentErr := hierarchyrecord.DecodeEnvironment(values[3].Value)
	if attachErr != nil || factsErr != nil || projectErr != nil || environmentErr != nil {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if attach.ID != source.TargetID || attach.EnvironmentID != snapshot.ConsumerEnvironmentID ||
		string(attach.Status) != "ready" || attach.BackingProjectID != snapshot.BackingProjectID ||
		attach.BackingEnvironmentID != snapshot.BackingEnvironmentID ||
		attach.BackingServiceID != snapshot.BackingServiceID || facts.AttachID != source.TargetID ||
		project.ID != snapshot.BackingProjectID || project.Kind != hierarchyrecord.ProjectKindBacking ||
		environment.ID != snapshot.BackingEnvironmentID || environment.ProjectID != project.ID {
		return errs.New(errs.KindStateConflict, "postgres backup publication evidence changed")
	}
	return nil
}

func ValidateBackupVolumePublicationEvidence(
	values []*etcdstore.KeyValue,
	source backupruntime.BackupRunSourceAttemptRecord,
	snapshot backupruntime.BackupVolumeSourceSnapshot,
) error {
	const offset = 3
	if len(values) != len(snapshot.Services)+offset || values[0] == nil || values[1] == nil || values[2] == nil ||
		values[0].ModRevision != snapshot.EnvironmentRevision ||
		values[1].ModRevision != source.TargetRevision || values[2].ModRevision != snapshot.ProjectionRoot {
		return errs.New(errs.KindStateConflict, "volume backup publication evidence changed")
	}
	environment, environmentErr := hierarchyrecord.DecodeEnvironment(values[0].Value)
	revisionID, headErr := idempotencyrecord.DecodeTaskReference(values[1].Value)
	seal, sealErr := blueprints.DecodeEnvironmentBlueprintSeal(values[2].Value)
	if environmentErr != nil || headErr != nil || sealErr != nil {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	if environment.ID != snapshot.EnvironmentID || environment.VolumeDir != snapshot.AuthorizedVolumeDir ||
		revisionID != snapshot.DesiredRevisionID || seal.EnvironmentID != snapshot.EnvironmentID ||
		seal.RevisionID != snapshot.DesiredRevisionID || seal.RenderGeneration != snapshot.RenderGeneration ||
		hex.EncodeToString(seal.DependencyDigest[:]) != snapshot.DependencyDigest ||
		snapshot.VolumeID != source.TargetID || snapshot.DockerVolumeName != "gp_vol_"+source.TargetID {
		return errs.New(errs.KindStateConflict, "volume projection evidence changed")
	}
	for index, expected := range snapshot.Services {
		value := values[index+offset]
		if expected.ServiceRevision == 0 {
			if value != nil || expected.PriorIntent != backupruntime.BackupServiceIntentRunning {
				return errs.New(errs.KindStateConflict, "volume service publication evidence changed")
			}
			continue
		}
		if value == nil || value.ModRevision != expected.ServiceRevision {
			return errs.New(errs.KindStateConflict, "volume service publication evidence changed")
		}
		service, decodeErr := servicerecord.DecodeServiceRuntimeRecord(value.Value)
		if decodeErr != nil {
			return backupruntime.CorruptBackupRuntimeRecord()
		}
		if service.ServiceID != expected.ServiceID ||
			service.EnvironmentID != snapshot.EnvironmentID ||
			string(service.Runtime.RuntimeIntent) != string(expected.PriorIntent) {
			return errs.New(errs.KindStateConflict, "volume service publication evidence changed")
		}
	}
	return nil
}

func equalBackupMountPaths(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
