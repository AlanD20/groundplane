package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	deletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprovision"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	networkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreateEnvironmentWithWork atomically publishes the provisioning
// Environment, its indexes, initial Components, the owning Task,
// captured work input, native queue membership, and request replay under
// Project/Tenant deletion fences. The same transaction creates the canonical
// Environment mutation epoch consumed by every later persistence mutation.
func (repository *HierarchyRepository) CreateEnvironmentWithWork(
	ctx context.Context,
	volumeRoot string,
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	poolRegistry etcdstore.Versioned[networkreservations.EnvironmentPoolRegistry],
	record hierarchyrecord.EnvironmentRecord,
	components []componentrecord.Record,
	input environmentprovision.Input,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := hierarchyrecord.ValidateProject(project.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant || project.Revision <= 0 ||
		project.ReadRevision < project.Revision {
		return IdempotencyTransactionResult{}, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	if err := hierarchyrecord.ValidateEnvironment(record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := hierarchyrecord.ValidateEnvironmentVolumeDir(volumeRoot, project.Record, record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateInitialEnvironmentComponents(record.ID, components); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if poolRegistry.Revision < 0 || poolRegistry.ReadRevision < poolRegistry.Revision ||
		poolRegistry.Record.Reservations[record.ID] != record.NetworkPool {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Environment pool reservation does not match its provisioning record",
		)
	}
	if err := networkreservations.ValidateEnvironmentPoolRegistry(poolRegistry.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := input.Validate(); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if input.Environment != record || input.Scope.ProjectID != project.Record.ID ||
		input.Scope.TenantID != project.Record.TenantID || input.VolumeRoot != volumeRoot {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "Environment creation capture does not own its provisioning record")
	}
	if marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeProject || marker.Locator.ScopeID != project.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "Environment creation replay must be Project-scoped")
	}
	task, item, err := input.Work()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	work, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer etcdstore.ClearMutationValues(work.Mutations)
	captured, err := environmentprovision.Encode(input)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(captured)
	host, err := hostbindings.ReadLocal(ctx, repository.store, 0)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if host.Binding.HostID != input.HostID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Environment target host changed")
	}

	environmentValue, err := hierarchyrecord.EncodeEnvironment(record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(environmentValue)
	poolRegistryValue, err := recordcodec.Encode("environment_pool_registry", poolRegistry.Record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(poolRegistryValue)
	componentValues := make([][]byte, len(components))
	for index := range components {
		componentValues[index], err = componentrecord.EncodeRecord(components[index])
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clear(componentValues[index])
	}
	epochValue, err := backupruntime.EncodeEnvironmentMutationEpochRecord(backupruntime.EnvironmentMutationEpochRecord{
		EnvironmentID: record.ID,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epochValue)
	coordinationValue, err := hierarchydeletion.EncodeInitialCoordination(
		hierarchydeletion.HierarchyDeletionTargetEnvironment,
		record.ID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(coordinationValue)
	scriptSetValue, err := scriptrecord.EncodeScriptSetGeneration(scriptrecord.SetGenerationRecord{
		EnvironmentID: record.ID, GenerationID: record.ID,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(scriptSetValue)
	coordinationKey := hierarchydeletion.HierarchyCoordinationKey(
		string(hierarchydeletion.HierarchyDeletionTargetEnvironment),
		record.ID,
	)
	scriptSetKey := scriptrecord.ScriptSetActiveKey(record.ID)

	conditions := []etcdstore.Condition{
		{Key: hierarchyrecord.EnvironmentKey(record.ID)},
		{Key: hierarchyrecord.EnvironmentNameKey(record.ProjectID, record.Name)},
		{Key: hierarchyrecord.EnvironmentOwnerKey(record.ProjectID, record.ID)},
		{Key: hierarchyrecord.ProjectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletions.TombstoneKey("environment", record.ID)},
		{Key: deletions.TombstoneKey("project", project.Record.ID)},
		{Key: deletions.TombstoneKey("tenant", project.Record.TenantID)},
		{Key: networkreservations.EnvironmentPoolRegistryKey, ModRevision: poolRegistry.Revision},
		{Key: hierarchyrecord.EnvironmentMutationEpochKey(record.ID)},
		{Key: coordinationKey},
		{Key: scriptSetKey},
	}
	for _, component := range components {
		conditions = append(conditions,
			etcdstore.Condition{Key: componentrecord.RecordKey(component.Desired.ID)},
			etcdstore.Condition{Key: componentrecord.EnvironmentOwnerKey(record.ID, component.Desired.ID)},
			etcdstore.Condition{Key: componentrecord.EnvironmentSingletonKey(component.Desired)},
		)
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentKey(record.ID), Value: environmentValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   hierarchyrecord.EnvironmentNameKey(record.ProjectID, record.Name),
			Value: []byte(record.ID),
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   hierarchyrecord.EnvironmentOwnerKey(record.ProjectID, record.ID),
			Value: []byte(record.ID),
		},
		{Type: etcdstore.MutationPut, Key: networkreservations.EnvironmentPoolRegistryKey, Value: poolRegistryValue},
		{Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentMutationEpochKey(record.ID), Value: epochValue},
		{Type: etcdstore.MutationPut, Key: coordinationKey, Value: coordinationValue},
		{Type: etcdstore.MutationPut, Key: scriptSetKey, Value: scriptSetValue},
	}
	for index, component := range components {
		mutations = append(
			mutations,
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   componentrecord.RecordKey(component.Desired.ID),
				Value: componentValues[index],
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   componentrecord.EnvironmentOwnerKey(record.ID, component.Desired.ID),
				Value: []byte(component.Desired.ID),
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   componentrecord.EnvironmentSingletonKey(component.Desired),
				Value: []byte(component.Desired.ID),
			},
		)
	}
	if len(components) > 0 {
		mutations = append(mutations, componentrecord.WriteFenceMutation(task.ID))
	}
	taskTenant, err := loadTaskInitiationTenant(ctx, repository.store, project)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentCreationTaskInitiation(taskTenant, project, record, taskjournal.TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	conditions = append(conditions, work.Conditions...)
	conditions = append(conditions, etcdstore.Condition{Key: environmentprovision.InputKey(input.WorkItemID)})
	conditions = append(conditions, host.Conditions...)
	mutations = append(mutations, work.Mutations...)
	mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: environmentprovision.InputKey(input.WorkItemID), Value: captured})
	compareCount := len(conditions)
	classify := func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != compareCount {
			return errs.New(errs.KindInternal, "Environment creation compare evidence is incomplete")
		}
		if values[1] != nil {
			return errs.New(errs.KindNameConflict, "Environment name is already in use")
		}
		if values[3] == nil {
			return errs.New(errs.KindProjectNotFound, "Project was not found")
		}
		return errs.New(errs.KindStateConflict, "Environment creation admission changed")
	}
	conditions, classify, err = prepareTaskInitiationFences(initiation, conditions, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newIdempotencyMutationPlanForMarker(idempotencyrecord.IdempotencyMarkerTask, conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func validateInitialEnvironmentComponents(environmentID string, components []componentrecord.Record) error {
	if len(components) != 2 {
		return errs.New(errs.KindValidationFailed, "Environment creation requires exactly two initial Components")
	}
	wantKinds := map[core.ComponentKind]bool{
		core.ComponentKindIngressCaddy:   false,
		core.ComponentKindEdgeCloudflare: false,
	}
	seenIDs := make(map[string]struct{}, len(components))
	for _, component := range components {
		if err := componentrecord.ValidateRecord(component); err != nil {
			return err
		}
		if component.Desired.Owner != core.ComponentOwnerEnvironment || component.Desired.OwnerID != environmentID {
			return errs.New(errs.KindValidationFailed, "Initial Component owner must be the new Environment")
		}
		if component.Desired.Enabled || !component.Desired.Config.Empty() {
			return errs.New(errs.KindValidationFailed, "Initial Components must have empty disabled desired state")
		}
		if len(component.Runtime.GeneratedServices) != 0 || component.Runtime.PinnedIPv4 != "" ||
			component.Runtime.Healthy {
			return errs.New(errs.KindValidationFailed, "Initial Components must have empty runtime state")
		}
		if _, exists := seenIDs[component.Desired.ID]; exists {
			return errs.New(errs.KindValidationFailed, "Initial Component IDs must be unique")
		}
		seenIDs[component.Desired.ID] = struct{}{}
		if seen, exists := wantKinds[component.Desired.Kind]; !exists || seen {
			return errs.New(
				errs.KindValidationFailed,
				"Initial Component kinds must be unique Caddy and Cloudflare singletons",
			)
		}
		wantKinds[component.Desired.Kind] = true
	}
	return nil
}
