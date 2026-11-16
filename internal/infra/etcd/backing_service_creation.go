package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	backingpostgresrelease "github.com/AlanD20/groundplane/internal/infra/etcd/backingpostgresrelease"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingprovisionwork"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	networkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maximumBackingServiceTransactionOperations        = 128
	maximumBackingServiceTransactionRequestOperations = 128
)

// BackingServiceCreation is the complete durable input for one backing
// facade. The repository publishes every public member and its first desired
// revision in one transaction so readers can never observe a partial facade.
type BackingServiceCreation struct {
	VolumeRoot      string
	Stage           etcdstore.Versioned[BackingServiceCreationStage]
	PoolRegistry    etcdstore.Versioned[networkreservations.EnvironmentPoolRegistry]
	Project         hierarchyrecord.ProjectRecord
	Environment     hierarchyrecord.EnvironmentRecord
	Components      []componentrecord.Record
	Zone            zonerecord.Record
	Service         servicerecord.ServiceRecord
	Secrets         []secretrecord.Record
	SecretValues    []secretrecord.EncryptedValue
	Entries         []entryrecord.Record
	EntryValues     []entryrecord.EntryValueGeneration
	Claim           blueprints.EnvironmentBlueprintStageClaim
	Revision        blueprints.EnvironmentDesiredRevisionIdentity
	Projection      projectionrecord.EnvironmentComposeProjection
	Task            workexecution.Task
	Work            backingprovisionwork.Publication
	PostgresRelease *backingpostgresrelease.Record
	Marker          idempotencyrecord.IdempotencyMarker
}

// PrepareBackingServiceCreation atomically creates one backing facade and
// queues the Agent reconciliation pinned to its immutable desired revision.
// Blueprint audit and projection chunks must already be sealed by the normal
// staging protocol.
func (repository *HierarchyRepository) PrepareBackingServiceCreation(
	ctx context.Context,
	creation BackingServiceCreation,
) (workqueue.Change, error) {
	if err := validateBackingServiceCreation(ctx, creation); err != nil {
		return workqueue.Change{}, err
	}
	name, err := repository.store.Get(ctx, hierarchyrecord.ProjectSlugKey(creation.Project))
	if err != nil {
		return workqueue.Change{}, err
	}
	if name == nil {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Backing service name lookup is incomplete")
	}
	if name.Entry != nil {
		clear(name.Entry.Value)
		return workqueue.Change{}, errs.New(errs.KindNameConflict, "Backing service slug is already in use")
	}
	task, item, err := creation.Work.Input.Work()
	if err != nil {
		return workqueue.Change{}, err
	}
	host, err := hostbindings.ReadLocal(ctx, repository.store, 0)
	if err != nil {
		return workqueue.Change{}, err
	}
	if host.Binding.HostID != item.Placement.HostID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "backing creation target host changed")
	}
	configuration, err := creation.Work.ConfigurationConditions(ctx, host.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	publication, err := repository.prepareEnvironmentBlueprintPublication(
		ctx,
		creation.Claim,
		creation.Revision,
		creation.Projection,
		desiredPublicationOwner{TaskID: task.ID, EnvironmentID: creation.Environment.ID,
			RevisionID: task.ID, RenderGeneration: 1},
		creation.Marker,
		0,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(publication.publishedDescriptor)
	identityPublication, err := prepareEnvironmentDesiredIdentityPublication(
		ctx,
		repository.store,
		creation.Projection,
		creation.Claim.SourceKind,
		0,
		creation.Stage.ReadRevision,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer identityPublication.clear()

	projectValue, err := hierarchyrecord.EncodeProject(creation.Project)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(projectValue)
	environmentValue, err := hierarchyrecord.EncodeEnvironment(creation.Environment)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(environmentValue)
	projectCoordinationValue, err := hierarchydeletion.EncodeInitialCoordination(
		hierarchydeletion.HierarchyDeletionTargetProject,
		creation.Project.ID,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(projectCoordinationValue)
	environmentCoordinationValue, err := hierarchydeletion.EncodeInitialCoordination(
		hierarchydeletion.HierarchyDeletionTargetEnvironment,
		creation.Environment.ID,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(environmentCoordinationValue)
	scriptSetValue, err := scriptrecord.EncodeScriptSetGeneration(scriptrecord.SetGenerationRecord{
		EnvironmentID: creation.Environment.ID, GenerationID: creation.Environment.ID,
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(scriptSetValue)
	poolRegistryValue, err := recordcodec.Encode("environment_pool_registry", creation.PoolRegistry.Record)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(poolRegistryValue)
	zoneRegistryValue, err := recordcodec.Encode("zone_pool_registry", networkreservations.ZonePoolRegistry{
		Reservations: map[string]string{creation.Zone.Desired.ID: creation.Zone.Desired.Subnet},
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(zoneRegistryValue)
	serviceValue, err := servicerecord.EncodeServiceRuntimeRecord(
		servicerecord.NewServiceRuntimeRecord(creation.Service),
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(serviceValue)
	var postgresReleaseValue []byte
	if creation.PostgresRelease != nil {
		postgresReleaseValue, err = backingpostgresrelease.Encode(*creation.PostgresRelease)
		if err != nil {
			return workqueue.Change{}, err
		}
		defer clear(postgresReleaseValue)
	}
	epochValue, err := backupruntime.EncodeEnvironmentMutationEpochRecord(backupruntime.EnvironmentMutationEpochRecord{
		EnvironmentID: creation.Environment.ID,
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(epochValue)
	taskReference, err := idempotencyrecord.EncodeTaskReference(creation.Task.ID)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(taskReference)
	componentValues := make([][]byte, len(creation.Components))
	for index := range creation.Components {
		componentValues[index], err = componentrecord.EncodeRecord(creation.Components[index])
		if err != nil {
			return workqueue.Change{}, err
		}
		defer clear(componentValues[index])
	}
	entryValues := make([][]byte, len(creation.Entries))
	entryGenerationKeys := make([]string, len(creation.Entries))
	entryGenerationValues := make([][]byte, len(creation.Entries))
	for index := range creation.Entries {
		entryValues[index], err = entryrecord.EncodeRecord(creation.Entries[index])
		if err != nil {
			return workqueue.Change{}, err
		}
		defer clear(entryValues[index])
		entryGenerationKeys[index], entryGenerationValues[index], err = entryrecord.PrepareEntryGeneration(
			creation.Entries[index],
			creation.EntryValues[index],
		)
		if err != nil {
			return workqueue.Change{}, err
		}
		defer clear(entryGenerationValues[index])
	}
	secretValues := make([][]byte, len(creation.Secrets))
	secretEncryptedValues := make([][]byte, len(creation.Secrets))
	for index := range creation.Secrets {
		secretValues[index], err = secretrecord.EncodeRecord(creation.Secrets[index])
		if err != nil {
			return workqueue.Change{}, err
		}
		defer clear(secretValues[index])
		secretEncryptedValues[index], err = secretrecord.EncodeEncryptedValue(creation.SecretValues[index])
		if err != nil {
			return workqueue.Change{}, err
		}
		defer clear(secretEncryptedValues[index])
	}

	creationStageKey, err := backingServiceCreationStageKey(creation.Stage.Record.Locator)
	if err != nil {
		return workqueue.Change{}, err
	}
	conditions := backingServiceCreationConditions(creation, publication, creationStageKey)
	conditions = append(conditions, identityPublication.conditions...)
	conditions = append(conditions, creation.Work.Change.Conditions...)
	conditions = append(conditions, host.Conditions...)
	conditions = append(conditions, configuration...)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationDelete, Key: creationStageKey},
		{Type: etcdstore.MutationPut, Key: publication.descriptorKey, Value: publication.publishedDescriptor},
		{Type: etcdstore.MutationDelete, Key: publication.locatorKey},
		{
			Type:  etcdstore.MutationPut,
			Key:   blueprints.EnvironmentBlueprintHeadKey(creation.Environment.ID),
			Value: taskReference,
		},
		{Type: etcdstore.MutationPut, Key: hierarchyrecord.ProjectKey(creation.Project.ID), Value: projectValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   hierarchyrecord.ProjectSlugKey(creation.Project),
			Value: []byte(creation.Project.ID),
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   hierarchyrecord.ProjectOwnerKey(creation.Project),
			Value: []byte(creation.Project.ID),
		},
		{
			Type: etcdstore.MutationPut,
			Key: hierarchydeletion.HierarchyCoordinationKey(
				string(hierarchydeletion.HierarchyDeletionTargetProject),
				creation.Project.ID,
			),
			Value: projectCoordinationValue,
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   hierarchyrecord.EnvironmentKey(creation.Environment.ID),
			Value: environmentValue,
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   hierarchyrecord.EnvironmentNameKey(creation.Project.ID, creation.Environment.Name),
			Value: []byte(creation.Environment.ID),
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   hierarchyrecord.EnvironmentOwnerKey(creation.Project.ID, creation.Environment.ID),
			Value: []byte(creation.Environment.ID),
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   hierarchyrecord.EnvironmentMutationEpochKey(creation.Environment.ID),
			Value: epochValue,
		},
		{
			Type: etcdstore.MutationPut,
			Key: hierarchydeletion.HierarchyCoordinationKey(
				string(hierarchydeletion.HierarchyDeletionTargetEnvironment),
				creation.Environment.ID,
			),
			Value: environmentCoordinationValue,
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   scriptrecord.ScriptSetActiveKey(creation.Environment.ID),
			Value: scriptSetValue,
		},
		{Type: etcdstore.MutationPut, Key: networkreservations.EnvironmentPoolRegistryKey, Value: poolRegistryValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   networkreservations.ZonePoolRegistryKey(creation.Environment.ID),
			Value: zoneRegistryValue,
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   servicerecord.ServiceRuntimeKey(creation.Service.Desired.ID),
			Value: serviceValue,
		},
	}
	if creation.PostgresRelease != nil {
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut,
			Key:   backingpostgresrelease.Key(creation.Environment.ID, creation.Service.Desired.ID),
			Value: postgresReleaseValue})
	}
	for index, component := range creation.Components {
		mutations = append(
			mutations,
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   componentrecord.RecordKey(component.Desired.ID),
				Value: componentValues[index],
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   componentrecord.EnvironmentOwnerKey(creation.Environment.ID, component.Desired.ID),
				Value: []byte(component.Desired.ID),
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   componentrecord.EnvironmentSingletonKey(component.Desired),
				Value: []byte(component.Desired.ID),
			},
		)
	}
	if len(creation.Components) > 0 {
		mutations = append(mutations, componentrecord.WriteFenceMutation(creation.Task.ID))
	}
	for index, entry := range creation.Entries {
		mutations = append(
			mutations,
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   entryrecord.RecordKey(entry.Entry.ID),
				Value: entryValues[index],
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   entryrecord.EntryOwnerKey(creation.Environment.ID, entry.Entry.ID),
				Value: []byte(entry.Entry.ID),
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   entryGenerationKeys[index],
				Value: entryGenerationValues[index],
			},
		)
	}
	for index, secret := range creation.Secrets {
		mutations = append(
			mutations,
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   secretrecord.RecordKey(secret.Secret.ID),
				Value: secretValues[index],
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   secretrecord.SecretOwnerKey(secret.Secret),
				Value: []byte(secret.Secret.ID),
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   secretrecord.SecretScopedKey(secret.Secret),
				Value: []byte(secret.Secret.ID),
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   secretrecord.ValueKey(secret.Secret.ID),
				Value: secretEncryptedValues[index],
			},
		)
	}
	mutations = append(mutations, identityPublication.mutations...)
	mutations = append(mutations, creation.Work.Change.Mutations...)
	conditions, err = resourcePublicationConditions(conditions)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer etcdstore.ClearMutationValues(mutations)
	if err := validateBackingServicePublicationBudget(conditions, mutations); err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{
		Conditions: append([]etcdstore.Condition(nil), conditions...),
		Mutations:  cloneBackingCreationMutations(mutations),
	}, nil
}

func cloneBackingCreationMutations(mutations []etcdstore.Mutation) []etcdstore.Mutation {
	out := append([]etcdstore.Mutation(nil), mutations...)
	for i := range out {
		out[i].Value = append([]byte(nil), out[i].Value...)
	}
	return out
}
