package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingpostgresrelease"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func backingServiceCreationConditions(creation BackingServiceCreation,
	publication environmentBlueprintPublicationEvidence, creationStageKey string,
) []keyvalue.Condition {
	conditions := []keyvalue.Condition{
		{Key: creationStageKey, ModRevision: creation.Stage.Revision},
		{Key: blueprints.EnvironmentBlueprintRootKey(creation.Environment.ID, creation.Task.ID), ModRevision: publication.rootRevision},
		{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
		{Key: blueprints.EnvironmentBlueprintHeadKey(creation.Environment.ID)},
		{Key: hierarchyrecord.ProjectKey(creation.Project.ID)},
		{Key: hierarchyrecord.ProjectSlugKey(creation.Project)},
		{Key: hierarchyrecord.ProjectOwnerKey(creation.Project)},
		{Key: hierarchydeletion.HierarchyCoordinationKey(string(hierarchydeletion.HierarchyDeletionTargetProject), creation.Project.ID)},
		{Key: deletions.TombstoneKey("project", creation.Project.ID)},
		{Key: hierarchyrecord.EnvironmentKey(creation.Environment.ID)},
		{Key: hierarchyrecord.EnvironmentNameKey(creation.Project.ID, creation.Environment.Name)},
		{Key: hierarchyrecord.EnvironmentOwnerKey(creation.Project.ID, creation.Environment.ID)},
		{Key: deletions.TombstoneKey("environment", creation.Environment.ID)},
		{Key: hierarchyrecord.EnvironmentMutationEpochKey(creation.Environment.ID)},
		{Key: hierarchydeletion.HierarchyCoordinationKey(string(hierarchydeletion.HierarchyDeletionTargetEnvironment), creation.Environment.ID)},
		{Key: scripts.ScriptSetActiveKey(creation.Environment.ID)},
		{Key: networkreservations.EnvironmentPoolRegistryKey, ModRevision: creation.PoolRegistry.Revision},
		{Key: deletions.TombstoneKey("zone", creation.Zone.Desired.ID)},
		{Key: networkreservations.ZonePoolRegistryKey(creation.Environment.ID)},
		{Key: servicerecord.ServiceRuntimeKey(creation.Service.Desired.ID)},
		{Key: deletions.TombstoneKey("service", creation.Service.Desired.ID)},
	}
	for _, component := range creation.Components {
		conditions = append(conditions, keyvalue.Condition{Key: componentrecord.RecordKey(component.Desired.ID)},
			keyvalue.Condition{Key: componentrecord.EnvironmentOwnerKey(creation.Environment.ID, component.Desired.ID)},
			keyvalue.Condition{Key: componentrecord.EnvironmentSingletonKey(component.Desired)})
	}
	for index, entry := range creation.Entries {
		generationKey := entryvalues.SecretKey(entry.Entry.ID, entry.CurrentValueGenerationID)
		if creation.EntryValues[index].Plain != nil {
			generationKey = entryvalues.PlainKey(entry.Entry.ID, entry.CurrentValueGenerationID)
		}
		conditions = append(conditions, keyvalue.Condition{Key: entryrecord.RecordKey(entry.Entry.ID)},
			keyvalue.Condition{Key: entryrecord.EntryOwnerKey(creation.Environment.ID, entry.Entry.ID)}, keyvalue.Condition{Key: generationKey})
	}
	for _, secret := range creation.Secrets {
		conditions = append(conditions, keyvalue.Condition{Key: secretrecord.RecordKey(secret.Secret.ID)},
			keyvalue.Condition{Key: secretrecord.SecretOwnerKey(secret.Secret)}, keyvalue.Condition{Key: secretrecord.SecretScopedKey(secret.Secret)},
			keyvalue.Condition{Key: secretrecord.ValueKey(secret.Secret.ID)}, keyvalue.Condition{Key: deletions.TombstoneKey("secret", secret.Secret.ID)})
	}
	if creation.PostgresRelease != nil {
		conditions = append(conditions, keyvalue.Condition{Key: backingpostgresrelease.Key(creation.Environment.ID, creation.Service.Desired.ID)})
	}
	return conditions
}

func classifyBackingServiceCreation(creation BackingServiceCreation, conditions []keyvalue.Condition) idempotencyPlanClassifier {
	classify := resourceWorkPublicationConflict(conditions)
	return func(revision int64, values []*keyvalue.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "backing creation compare evidence is incomplete")
		}
		for index, condition := range conditions {
			value := values[index]
			if value == nil {
				continue
			}
			if value.Key != condition.Key || value.ModRevision <= 0 || value.ModRevision > revision {
				return errs.New(errs.KindInternal, "backing creation compare evidence has an invalid identity")
			}
			if condition.Key == hierarchyrecord.ProjectSlugKey(creation.Project) {
				return errs.New(errs.KindNameConflict, "backing-service slug is already in use")
			}
			if condition.Key == hierarchyrecord.EnvironmentNameKey(creation.Project.ID, creation.Environment.Name) {
				return errs.New(errs.KindNameConflict, "backing-service Environment name is already in use")
			}
		}
		return classify(revision, values)
	}
}
