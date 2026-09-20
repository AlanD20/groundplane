package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"slices"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// ValidateServiceRemovalReferences enforces Remove's no-cascade contract at a
// fixed durable view. The removal tombstone and Environment mutation fence
// close races with new references during task publication.
func (repository *ServiceRepository) ValidateServiceRemovalReferences(
	ctx context.Context,
	current etcdstore.Versioned[ServiceRecord],
	projection etcdstore.Versioned[EnvironmentComposeProjection],
) error {
	if err := validateServiceVersion(current); err != nil {
		return err
	}
	if projection.Revision <= 0 || projection.ReadRevision < projection.Revision ||
		projection.Record.EnvironmentID != current.Record.EnvironmentID ||
		validateEnvironmentComposeProjection(projection.Record) != nil {
		return errs.New(errs.KindValidationFailed, "Service removal projection is invalid")
	}
	if current.Record.BackingNetworkID != "" || current.Record.Desired.Adapter != "" {
		return errs.New(errs.KindResourceInUse, "Backing Services are removed through their backing lifecycle")
	}
	for _, service := range projection.Record.DesiredServices {
		if service.Desired.ID == current.Record.Desired.ID {
			continue
		}
		if _, referenced := service.Desired.DependsOn[current.Record.Desired.Name]; referenced {
			return errs.New(errs.KindResourceInUse, "Service is referenced by another desired resource")
		}
	}
	for _, component := range projection.Record.Components {
		if slices.Contains(component.Runtime.GeneratedServices, current.Record.Desired.ID) {
			return errs.New(errs.KindResourceInUse, "Component-generated Services are removed through their Component")
		}
	}
	for _, prefix := range []string{
		"/v1/indexes/attaches/by-service/service/" + current.Record.Desired.ID + "/",
		"/v1/indexes/attaches/by-backing-service/service/" + current.Record.Desired.ID + "/",
	} {
		page, err := repository.store.Range(
			ctx,
			etcdstore.RangeRequest{Prefix: prefix, Limit: 1, Revision: projection.ReadRevision},
		)
		if err != nil {
			return err
		}
		if page == nil {
			return errs.New(errs.KindInternal, "Service removal reference read is empty")
		}
		if len(page.Values) != 0 {
			return errs.New(errs.KindResourceInUse, "Service is referenced by an Attach")
		}
	}
	if err := repository.scanServiceRemovalRecords(ctx, projection.ReadRevision, current.Record); err != nil {
		return err
	}
	_, err := prepareServiceScriptAbsence(ctx, repository.store, current.Record.Desired.ID, projection.ReadRevision)
	return err
}

func (repository *ServiceRepository) scanServiceRemovalRecords(
	ctx context.Context,
	revision int64,
	current ServiceRecord,
) error {
	projection, found, err := currentEnvironmentProjectionAtRevision(
		ctx, repository.store, current.EnvironmentID, revision,
	)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	for _, route := range projection.Record.DesiredRoutes {
		if route.Desired.TargetServiceID == current.Desired.ID {
			return errs.New(errs.KindResourceInUse, "Service is referenced by another desired resource")
		}
	}
	return nil
}

// BeginServiceRemovalWithTask seals ownership of an already-staged candidate
// without changing the active desired head or deleting the visible Service.
func (repository *ServiceRepository) BeginServiceRemovalWithTask(
	ctx context.Context,
	tenant etcdstore.Versioned[hierarchyrecord.TenantRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	current etcdstore.Versioned[ServiceRecord],
	projection etcdstore.Versioned[EnvironmentComposeProjection],
	tombstone deletionrecord.DeletionTombstoneRecord,
	intent ServiceRemovalIntent,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateServiceLifecycleHierarchy(&tenant, project, environment, current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := deletionrecord.ValidateDeletionTombstone(tombstone); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateServiceRemovalIntent(intent); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateServiceRemovalTaskOwner(task, intent); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if projection.Revision != intent.CurrentProjectionRevision ||
		!sameRouteRemovalProjection(projection.Record, intent.CurrentProjection) ||
		intent.ServiceRevision != current.Revision || intent.ExpectedHeadRevision != projection.Revision ||
		tombstone.TargetKind != deletionrecord.DeletionTargetService || tombstone.TargetID != current.Record.Desired.ID ||
		tombstone.TargetRevision != current.Revision || tombstone.TaskID != task.ID ||
		tombstone.Phase != deletionrecord.DeletionPhaseHostEffects || !tombstone.CreatedAt.Equal(task.CreatedAt) ||
		!tombstone.UpdatedAt.Equal(tombstone.CreatedAt) || task.Status != TaskStatusPending {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Service removal state does not match its Task",
		)
	}
	wantReplay := idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetService, ID: current.Record.Desired.ID}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending || marker.TaskID != task.ID ||
		marker.Locator != intent.Claim.Locator || marker.ReplayTarget == nil || *marker.ReplayTarget != wantReplay ||
		!sameBlueprintProtectedIntent(marker.Intent, intent.Claim.Intent) ||
		!marker.CreatedAt.Equal(task.CreatedAt) || !marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Service removal marker does not match its Task",
		)
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	mutationContext, err := loadOrdinaryEnvironmentMutationContext(
		ctx, repository.store, environment.Record.ID, hierarchyrecord.EnvironmentKey(environment.Record.ID),
		project.Record.ID, tenant.Record.ID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	versionedTenant, versionedProject, versionedEnvironment, err := mutationContext.versionHierarchy(
		&tenant,
		project,
		environment,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := repository.ValidateServiceRemovalReferences(ctx, current, projection); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		environmentBlueprintHeadKey(environment.Record.ID),
		environmentComposeProjectionKey(environment.Record.ID),
		serviceLifecycleActiveKey(current.Record.Desired.ID),
		componentTaskActiveEnvironmentKey(environment.Record.ID),
	}, Revision: mutationContext.readRevision})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if indexes == nil || len(indexes.Values) != 4 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		indexes.Values[2] != nil || indexes.Values[3] != nil ||
		indexes.Values[0].ModRevision != intent.ExpectedHeadRevision ||
		indexes.Values[1].ModRevision != projection.Revision {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Service removal baseline changed")
	}
	hierarchy := &HierarchyRepository{store: repository.store}
	publication, err := hierarchy.prepareEnvironmentDirectPublication(
		ctx, intent.Claim,
		EnvironmentDesiredRevisionIdentity{EnvironmentID: intent.EnvironmentID, RevisionID: intent.Claim.RevisionID},
		intent.CandidateProjection,
		idempotencyrecord.IdempotencyMarker{Locator: intent.Claim.Locator, Intent: intent.Claim.Intent},
		intent.ExpectedHeadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(publication.publishedDescriptor)
	task = cloneTaskRecord(task)
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = marker.Locator.Key
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	tombstoneValue, err := deletionrecord.EncodeDeletionTombstone(tombstone)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(tombstoneValue)
	intentValue, err := encodeServiceRemovalIntent(intent)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(intentValue)
	conditions := []etcdstore.Condition{
		{Key: taskKey(task.ID)}, {Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)}, {Key: taskQueueKey(task.Executor, task.ID)},
		serviceDesiredCondition(current),
		serviceRuntimeCondition(current),
		{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetService), current.Record.Desired.ID)},
		{Key: serviceRemovalIntentKey(task.ID)},
		{Key: environmentComposeProjectionKey(environment.Record.ID), ModRevision: indexes.Values[1].ModRevision},
		{Key: serviceLifecycleActiveKey(current.Record.Desired.ID)},
		{Key: componentTaskActiveEnvironmentKey(environment.Record.ID)},
		{
			Key:         environmentBlueprintRootKey(intent.EnvironmentID, intent.Claim.RevisionID),
			ModRevision: publication.rootRevision,
		},
		{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{
			Type:  etcdstore.MutationPut,
			Key:   deletionTombstoneKey(string(deletionrecord.DeletionTargetService), current.Record.Desired.ID),
			Value: tombstoneValue,
		},
		{Type: etcdstore.MutationPut, Key: serviceRemovalIntentKey(task.ID), Value: intentValue},
		{Type: etcdstore.MutationPut, Key: componentTaskActiveEnvironmentKey(environment.Record.ID), Value: []byte(task.ID)},
	}
	originalClassify := func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "Service removal compare evidence is incomplete")
		}
		if values[4] == nil {
			return errs.New(errs.KindServiceNotFound, "Service was not found")
		}
		if values[4].ModRevision != current.Revision {
			return stateConflict("service", current.Record.Desired.ID)
		}
		if !conditionMatchesRead(serviceRuntimeCondition(current), values[5]) {
			return stateConflict("service runtime", current.Record.Desired.ID)
		}
		if values[6] != nil || values[7] != nil || values[9] != nil || values[10] != nil {
			return errs.New(errs.KindResourceInUse, "Service removal or Environment mutation is already active")
		}
		return errs.New(errs.KindStateConflict, "Service removal state changed")
	}
	binding, err := mutationContext.bind(ctx, repository.store, conditions, mutations, true)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer binding.clear()
	defer clearMutationValues(binding.mutations)
	if err := validateBoundServiceConditions(binding, current, 4, 5); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := binding.preparedConflict(originalClassify); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	scriptConditions, err := prepareServiceScriptAbsence(
		ctx, repository.store, current.Record.Desired.ID, mutationContext.readRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	finalConditions := append(binding.conditions, scriptConditions...)
	initiation, err := newEnvironmentTaskInitiation(
		versionedTenant,
		versionedProject,
		versionedEnvironment,
		TaskActorOperator,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(task, initiation, finalConditions, binding.mutations,
		func(revision int64, values []*etcdstore.KeyValue) error {
			if len(values) != len(finalConditions) {
				return errs.New(errs.KindInternal, "Service removal Script compare evidence is incomplete")
			}
			if err := classifyServiceScriptReferences(current.Record.Desired.ID, values[len(binding.conditions):]); err != nil {
				return err
			}
			return binding.classify(revision, values[:len(binding.conditions)], originalClassify)
		})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}
