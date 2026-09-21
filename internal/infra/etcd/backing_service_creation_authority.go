package etcd

import (
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	deletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	entryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	networkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func backingServiceCreationConditions(
	creation BackingServiceCreation,
	publication environmentBlueprintPublicationEvidence,
	creationStageKey string,
) []etcdstore.Condition {
	conditions := []etcdstore.Condition{
		{Key: creationStageKey, ModRevision: creation.Stage.Revision},
		{Key: taskjournal.TaskStorageKey(creation.Task.ID)},
		{Key: taskjournal.TaskOperationIndexKey(creation.Task.OperationID, creation.Task.ID)},
		{Key: taskjournal.TaskActiveOperationKey(creation.Task.OperationID)},
		{Key: taskjournal.TaskQueueKey(creation.Task.Executor, creation.Task.ID)},
		{
			Key:         blueprints.EnvironmentBlueprintRootKey(creation.Environment.ID, creation.Task.ID),
			ModRevision: publication.rootRevision,
		},
		{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
		{Key: blueprints.EnvironmentBlueprintHeadKey(creation.Environment.ID)},
		{Key: hierarchyrecord.ProjectKey(creation.Project.ID)},
		{Key: hierarchyrecord.ProjectSlugKey(creation.Project)},
		{Key: hierarchyrecord.ProjectOwnerKey(creation.Project)},
		{
			Key: hierarchydeletion.HierarchyCoordinationKey(
				string(hierarchydeletion.HierarchyDeletionTargetProject),
				creation.Project.ID,
			),
		},
		{Key: deletions.TombstoneKey("project", creation.Project.ID)},
		{Key: hierarchyrecord.EnvironmentKey(creation.Environment.ID)},
		{Key: hierarchyrecord.EnvironmentNameKey(creation.Project.ID, creation.Environment.Name)},
		{Key: hierarchyrecord.EnvironmentOwnerKey(creation.Project.ID, creation.Environment.ID)},
		{Key: deletions.TombstoneKey("environment", creation.Environment.ID)},
		{Key: hierarchyrecord.EnvironmentMutationEpochKey(creation.Environment.ID)},
		{
			Key: hierarchydeletion.HierarchyCoordinationKey(
				string(hierarchydeletion.HierarchyDeletionTargetEnvironment),
				creation.Environment.ID,
			),
		},
		{Key: scriptrecord.ScriptSetActiveKey(creation.Environment.ID)},
		{Key: networkreservations.EnvironmentPoolRegistryKey, ModRevision: creation.PoolRegistry.Revision},
		{Key: deletions.TombstoneKey("zone", creation.Zone.Desired.ID)},
		{Key: networkreservations.ZonePoolRegistryKey(creation.Environment.ID)},
		{Key: servicerecord.ServiceRuntimeKey(creation.Service.Desired.ID)},
		{Key: deletions.TombstoneKey("service", creation.Service.Desired.ID)},
	}
	for _, component := range creation.Components {
		conditions = append(
			conditions,
			etcdstore.Condition{Key: componentrecord.RecordKey(component.Desired.ID)},
			etcdstore.Condition{
				Key: componentrecord.EnvironmentOwnerKey(creation.Environment.ID, component.Desired.ID),
			},
			etcdstore.Condition{
				Key: componentrecord.EnvironmentKindKey(creation.Environment.ID, component.Desired.Kind),
			},
		)
	}
	for index, entry := range creation.Entries {
		generationKey := entryvalues.SecretKey(entry.Entry.ID, entry.CurrentValueGenerationID)
		if creation.EntryValues[index].Plain != nil {
			generationKey = entryvalues.PlainKey(entry.Entry.ID, entry.CurrentValueGenerationID)
		}
		conditions = append(conditions,
			etcdstore.Condition{Key: entryrecord.RecordKey(entry.Entry.ID)},
			etcdstore.Condition{Key: entryrecord.EntryOwnerKey(creation.Environment.ID, entry.Entry.ID)},
			etcdstore.Condition{Key: generationKey},
		)
	}
	for _, secret := range creation.Secrets {
		conditions = append(conditions,
			etcdstore.Condition{Key: secretrecord.RecordKey(secret.Secret.ID)},
			etcdstore.Condition{Key: secretrecord.SecretOwnerKey(secret.Secret)},
			etcdstore.Condition{Key: secretrecord.SecretScopedKey(secret.Secret)},
			etcdstore.Condition{Key: secretrecord.ValueKey(secret.Secret.ID)},
			etcdstore.Condition{Key: deletions.TombstoneKey("secret", secret.Secret.ID)},
		)
	}
	return conditions
}

func classifyBackingServiceCreation(
	creation BackingServiceCreation,
	publication environmentBlueprintPublicationEvidence,
	want int,
) idempotencyPlanClassifier {
	const (
		creationStageCondition = iota
		taskCondition
		operationTaskCondition
		activeOperationCondition
		queuedTaskCondition
		blueprintRootCondition
		blueprintDescriptorCondition
		blueprintLocatorCondition
		blueprintHeadCondition
		projectCondition
		projectSlugCondition
		projectOwnerCondition
		projectCoordinationCondition
		projectTombstoneCondition
		environmentCondition
		environmentNameCondition
		environmentOwnerCondition
		environmentTombstoneCondition
		environmentEpochCondition
		environmentCoordinationCondition
		environmentPoolCondition
		zoneTombstoneCondition
		zonePoolCondition
		serviceCondition
		serviceTombstoneCondition
		componentConditionStart
	)
	return func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != want {
			return errs.New(errs.KindInternal, "Backing-service creation compare evidence is incomplete")
		}
		if values[operationTaskCondition] != nil {
			activeTaskID, err := idempotencyrecord.DecodeTaskReference(values[operationTaskCondition].Value)
			if err != nil {
				return err
			}
			return errs.Newf(
				errs.KindStateConflict,
				"operation %s already has active task %s",
				creation.Task.OperationID,
				activeTaskID,
			)
		}
		for _, index := range []int{creationStageCondition, taskCondition, activeOperationCondition, queuedTaskCondition} {
			if values[index] != nil {
				return errs.New(errs.KindInternal, "Backing-service creation collided with durable Task state")
			}
		}
		if values[blueprintRootCondition] == nil ||
			values[blueprintRootCondition].ModRevision != publication.rootRevision ||
			values[blueprintDescriptorCondition] == nil ||
			values[blueprintDescriptorCondition].ModRevision != publication.descriptorRevision ||
			values[blueprintLocatorCondition] == nil ||
			values[blueprintLocatorCondition].ModRevision != publication.locatorRevision {
			return errs.New(errs.KindStateConflict, "Backing-service sealed staging evidence changed")
		}
		if values[blueprintHeadCondition] != nil {
			return errs.New(errs.KindStateConflict, "Backing-service desired state already exists")
		}
		if values[projectSlugCondition] != nil {
			return errs.New(errs.KindNameConflict, "Backing-service slug is already in use")
		}
		if values[environmentNameCondition] != nil {
			return errs.New(errs.KindNameConflict, "Backing-service Environment name is already in use")
		}
		poolRegistry := values[environmentPoolCondition]
		if (creation.PoolRegistry.Revision == 0 && poolRegistry != nil) ||
			(creation.PoolRegistry.Revision > 0 &&
				(poolRegistry == nil || poolRegistry.ModRevision != creation.PoolRegistry.Revision)) {
			return recordcodec.StateConflict("environment pool registry", "global")
		}
		for _, index := range []int{
			blueprintHeadCondition, projectCondition, projectOwnerCondition,
			projectCoordinationCondition, environmentCondition, environmentOwnerCondition,
			environmentEpochCondition, environmentCoordinationCondition,
			zonePoolCondition, serviceCondition,
		} {
			if values[index] != nil {
				return errs.New(errs.KindStateConflict, "Backing-service stable identity is already in use")
			}
		}
		for _, index := range []int{projectTombstoneCondition, environmentTombstoneCondition, zoneTombstoneCondition, serviceTombstoneCondition} {
			if values[index] != nil {
				return errs.New(errs.KindResourceInUse, "Backing-service deletion is in progress")
			}
		}
		for index := componentConditionStart; index < len(values); index++ {
			if values[index] != nil {
				return errs.New(errs.KindStateConflict, "Backing-service Component identity is already in use")
			}
		}
		return errs.New(errs.KindStateConflict, "Backing-service creation raced")
	}
}
