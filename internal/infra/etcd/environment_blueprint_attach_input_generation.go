package etcd

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const blueprintAttachInputCleanupTimeout = 30 * time.Second

// BlueprintAttachInputGenerationPublication pins every mutable backing source
// used to prepare one immutable Custom Attach input generation.
type BlueprintAttachInputGenerationPublication struct {
	Generation         attachinputs.Generation
	BackingProject     etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	BackingEnvironment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	BackingService     etcdstore.Versioned[servicerecord.ServiceRecord]
}

type preparedBlueprintAttachInputGenerations struct {
	conditions   []etcdstore.Condition
	mutations    []etcdstore.Mutation
	preparations []preparedBlueprintAttachSecretPins
	generations  []attachinputs.Generation
}

type preparedBlueprintAttachSecretPins struct {
	repository  *tasksecretpins.Repository
	operationID string
}

func (prepared *preparedBlueprintAttachInputGenerations) clear() {
	if prepared == nil {
		return
	}
	etcdstore.ClearMutationValues(prepared.mutations)
	for index := range prepared.generations {
		attachinputs.Clear(&prepared.generations[index])
	}
	*prepared = preparedBlueprintAttachInputGenerations{}
}

func (repository *EnvironmentBlueprintRepository) prepareBlueprintAttachInputGenerations(
	ctx context.Context,
	parent TaskRecord,
	inputs []BlueprintAttachInputGenerationPublication,
) (preparedBlueprintAttachInputGenerations, error) {
	prepared := preparedBlueprintAttachInputGenerations{}
	if len(inputs) == 0 {
		return prepared, nil
	}
	inputs = append([]BlueprintAttachInputGenerationPublication(nil), inputs...)
	sort.Slice(inputs, func(left, right int) bool {
		return inputs[left].Generation.AttachID < inputs[right].Generation.AttachID
	})
	conditionRevisions := make(map[string]int64)
	appendCondition := func(condition etcdstore.Condition) error {
		if previous, exists := conditionRevisions[condition.Key]; exists {
			if previous != condition.ModRevision {
				return errs.New(errs.KindStateConflict, "Blueprint Attach input sources diverged")
			}
			return nil
		}
		conditionRevisions[condition.Key] = condition.ModRevision
		prepared.conditions = append(prepared.conditions, condition)
		return nil
	}
	fail := func(cause error) (preparedBlueprintAttachInputGenerations, error) {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), blueprintAttachInputCleanupTimeout)
		defer cancel()
		for index := len(prepared.preparations) - 1; index >= 0; index-- {
			cause = errors.Join(cause, prepared.preparations[index].repository.Abandon(
				cleanupCtx, prepared.preparations[index].operationID,
			))
		}
		prepared.clear()
		return preparedBlueprintAttachInputGenerations{}, cause
	}
	for index := range inputs {
		candidate := inputs[index]
		generation := candidate.Generation
		if err := attachinputs.ValidateDraft(generation); err != nil {
			return fail(err)
		}
		if generation.ParentTaskID != parent.ID || generation.RevisionID != parent.ID ||
			candidate.BackingProject.Revision <= 0 || candidate.BackingEnvironment.Revision <= 0 ||
			candidate.BackingService.Revision <= 0 ||
			candidate.BackingProject.Record.ID != generation.BackingProjectID ||
			candidate.BackingProject.Record.Kind != hierarchyrecord.ProjectKindBacking ||
			candidate.BackingProject.Record.TenantID != "" ||
			candidate.BackingEnvironment.Record.ID != generation.BackingEnvironmentID ||
			candidate.BackingEnvironment.Record.ProjectID != generation.BackingProjectID ||
			candidate.BackingEnvironment.Record.Name != "main" ||
			candidate.BackingEnvironment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady ||
			candidate.BackingService.Record.EnvironmentID != generation.BackingEnvironmentID ||
			candidate.BackingService.Record.Desired.ID != generation.BackingServiceID ||
			candidate.BackingService.Record.BackingNetworkID != generation.BackingNetworkID ||
			candidate.BackingService.Record.Desired.Adapter != generation.AdapterKey ||
			candidate.BackingService.Record.Desired.Authentication != generation.Authentication ||
			candidate.BackingService.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning ||
			candidate.BackingService.Record.Desired.Hooks == nil ||
			!attachinputs.MatchesConfiguration(generation, *candidate.BackingService.Record.Desired.Hooks) {
			return fail(errs.New(errs.KindValidationFailed, "Blueprint Attach input source identity is invalid"))
		}
		if err := appendCondition(etcdstore.Condition{
			Key: hierarchyrecord.ProjectKey(generation.BackingProjectID), ModRevision: candidate.BackingProject.Revision,
		}); err != nil {
			return fail(err)
		}
		if err := appendCondition(etcdstore.Condition{
			Key: hierarchyrecord.EnvironmentKey(generation.BackingEnvironmentID), ModRevision: candidate.BackingEnvironment.Revision,
		}); err != nil {
			return fail(err)
		}
		for _, condition := range []etcdstore.Condition{
			servicerecord.ServiceDesiredCondition(candidate.BackingService),
			servicerecord.ServiceRuntimeCondition(candidate.BackingService),
			{Key: attachrecord.AttachKey(generation.AttachID)},
			{Key: attachrecord.AttachNameKey(generation.EnvironmentID, generation.AttachName)},
			{Key: attachinputs.Key(generation.RevisionID, generation.AttachID)},
		} {
			if err := appendCondition(condition); err != nil {
				return fail(err)
			}
		}
		if len(generation.SecretSources) == 0 {
			var err error
			generation, err = attachinputs.BindSecretPins(generation, 0, "")
			if err != nil {
				return fail(err)
			}
		} else {
			pins, err := tasksecretpins.NewEtcdRepository(repository.store, generation.BackingProjectID)
			if err != nil {
				return fail(err)
			}
			secretPreparation, err := pins.Prepare(
				ctx, generation.OperationID, parent.ID, generation.SecretSources,
			)
			if err != nil {
				return fail(err)
			}
			prepared.preparations = append(prepared.preparations, preparedBlueprintAttachSecretPins{
				repository: pins, operationID: generation.OperationID,
			})
			generation, err = attachinputs.BindSecretPins(
				generation, secretPreparation.MembershipCount(), secretPreparation.MembershipSHA256(),
			)
			if err != nil {
				return fail(err)
			}
			activation, err := tasksecretpins.Activation(secretPreparation)
			if err != nil {
				return fail(err)
			}
			activationConditions, activationMutations, err := tasksecretpins.EtcdFragment(activation)
			activation.Clear()
			if err != nil {
				return fail(err)
			}
			for _, condition := range activationConditions {
				if err := appendCondition(condition); err != nil {
					etcdstore.ClearMutationValues(activationMutations)
					return fail(err)
				}
			}
			prepared.mutations = append(prepared.mutations, activationMutations...)
		}
		value, err := attachinputs.Encode(generation)
		if err != nil {
			return fail(err)
		}
		prepared.mutations = append(prepared.mutations, etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   attachinputs.Key(generation.RevisionID, generation.AttachID),
			Value: value,
		})
		prepared.generations = append(prepared.generations, generation)
	}
	return prepared, nil
}

func (prepared *preparedBlueprintAttachInputGenerations) finish(
	ctx context.Context,
	store idempotencyRepositoryStore,
	parentTaskID string,
	result IdempotencyTransactionResult,
	publicationErr error,
) (IdempotencyTransactionResult, error) {
	if prepared == nil || len(prepared.generations) == 0 {
		return result, publicationErr
	}
	if publicationErr == nil && result.kind == idempotencyTransactionApplied {
		return result, nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), blueprintAttachInputCleanupTimeout)
	defer cancel()
	keys := []string{taskjournal.TaskStorageKey(parentTaskID)}
	for _, generation := range prepared.generations {
		keys = append(keys, attachinputs.Key(generation.RevisionID, generation.AttachID))
	}
	read, err := store.GetMany(cleanupCtx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return result, errors.Join(publicationErr, err)
	}
	if read == nil || len(read.Values) != len(keys) {
		return result, errors.Join(publicationErr, errs.New(
			errs.KindInternal, "Blueprint Attach input publication evidence is incomplete",
		))
	}
	defer etcdstore.ClearValues(read.Values)
	present := 0
	for _, value := range read.Values {
		if value != nil {
			present++
		}
	}
	if present == len(keys) {
		for index, generation := range prepared.generations {
			stored, err := attachinputs.Decode(read.Values[index+1].Value)
			if err != nil || stored.ID != generation.ID || stored.ParentTaskID != parentTaskID {
				attachinputs.Clear(&stored)
				return result, errors.Join(publicationErr, errs.New(
					errs.KindInternal, "Blueprint Attach input publication evidence is corrupt",
				))
			}
			attachinputs.Clear(&stored)
		}
		return result, publicationErr
	}
	if present != 0 {
		return result, errors.Join(publicationErr, errs.New(
			errs.KindInternal, "Blueprint Attach input publication is partially visible",
		))
	}
	for index := len(prepared.preparations) - 1; index >= 0; index-- {
		publicationErr = errors.Join(publicationErr, prepared.preparations[index].repository.Abandon(
			cleanupCtx, prepared.preparations[index].operationID,
		))
	}
	return result, publicationErr
}

// GetBlueprintAttachInputGeneration reads one immutable generation by the
// desired revision and Attach identity which jointly own its storage key.
func (repository *EnvironmentBlueprintRepository) GetBlueprintAttachInputGeneration(
	ctx context.Context,
	revisionID string,
	attachID string,
) (etcdstore.Versioned[attachinputs.Generation], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[attachinputs.Generation]{}, false, err
	}
	if ids.Validate(ids.KindTask, revisionID) != nil || ids.Validate(ids.KindAttach, attachID) != nil {
		return etcdstore.Versioned[attachinputs.Generation]{}, false, errs.New(
			errs.KindValidationFailed, "Blueprint Attach input generation identity is invalid",
		)
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{attachinputs.Key(revisionID, attachID)},
	})
	if err != nil {
		return etcdstore.Versioned[attachinputs.Generation]{}, false, err
	}
	if result == nil || len(result.Values) != 1 {
		return etcdstore.Versioned[attachinputs.Generation]{}, false, errs.New(
			errs.KindInternal, "Blueprint Attach input generation read is incomplete",
		)
	}
	defer etcdstore.ClearValues(result.Values)
	if result.Values[0] == nil {
		return etcdstore.Versioned[attachinputs.Generation]{ReadRevision: result.ReadRevision}, false, nil
	}
	value := result.Values[0]
	generation, err := attachinputs.Decode(value.Value)
	if err != nil || generation.RevisionID != revisionID || generation.AttachID != attachID {
		attachinputs.Clear(&generation)
		if err != nil {
			return etcdstore.Versioned[attachinputs.Generation]{}, false, err
		}
		return etcdstore.Versioned[attachinputs.Generation]{}, false, errs.New(
			errs.KindInternal, "Blueprint Attach input generation key is corrupt",
		)
	}
	return etcdstore.Versioned[attachinputs.Generation]{
		Record: generation, Revision: value.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}
