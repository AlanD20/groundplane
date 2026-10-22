package etcd

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachprovision"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const blueprintAttachInputCleanupTimeout = 30 * time.Second

// BlueprintAttachInputGenerationPublication pins every mutable backing source
// used to prepare one immutable Attach input generation.
type BlueprintAttachInputGenerationPublication struct {
	Generation         attachinputs.Generation
	BackingProject     etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	BackingEnvironment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	BackingService     etcdstore.Versioned[servicerecord.ServiceRecord]
}

type preparedBlueprintAttachInputGenerations struct {
	conditions   []etcdstore.Condition
	mutations    []etcdstore.Mutation
	preparations []attachprovision.InputPublication
	generations  []attachinputs.Generation
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
	parent workexecution.Task,
	inputs []BlueprintAttachInputGenerationPublication,
) (preparedBlueprintAttachInputGenerations, error) {
	prepared := preparedBlueprintAttachInputGenerations{}
	if parent.Validate() != nil || parent.Action != workexecution.ActionApply || parent.PublicationComplete ||
		parent.Scope.EnvironmentID == "" || parent.TargetID != parent.Scope.EnvironmentID {
		return prepared, errs.New(errs.KindValidationFailed, "Attach inputs require an unpublished Blueprint Apply")
	}
	if len(inputs) == 0 {
		return prepared, nil
	}
	inputOwner, err := attachprovision.NewInputRepository(repository.store)
	if err != nil {
		return prepared, err
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
			cause = errors.Join(cause, prepared.preparations[index].Abandon(cleanupCtx))
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
			generation.EnvironmentID != parent.Scope.EnvironmentID ||
			(index > 0 && inputs[index-1].Generation.AttachID == generation.AttachID) ||
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
			candidate.BackingService.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
			return fail(errs.New(errs.KindValidationFailed, "Blueprint Attach input source identity is invalid"))
		}
		if generation.Binding == nil && generation.Provision == nil &&
			(candidate.BackingService.Record.Desired.Hooks == nil ||
				!attachinputs.MatchesConfiguration(generation, *candidate.BackingService.Record.Desired.Hooks)) {
			return fail(errs.New(errs.KindValidationFailed, "Blueprint Attach hook input differs from its source"))
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
		sourceConditions := []etcdstore.Condition{
			servicerecord.ServiceDesiredCondition(candidate.BackingService),
			servicerecord.ServiceRuntimeCondition(candidate.BackingService),
			{Key: attachinputs.Key(generation.RevisionID, generation.AttachID)},
		}
		if generation.Binding != nil {
			sourceConditions = append(sourceConditions, generation.Binding.Conditions()...)
		} else {
			if generation.Provision != nil {
				sourceConditions = append(sourceConditions, generation.Provision.Conditions()...)
			}
			sourceConditions = append(sourceConditions,
				etcdstore.Condition{Key: attachrecord.AttachKey(generation.AttachID)},
				etcdstore.Condition{Key: attachrecord.AttachNameKey(generation.EnvironmentID, generation.AttachName)},
			)
		}
		for _, condition := range sourceConditions {
			if err := appendCondition(condition); err != nil {
				return fail(err)
			}
		}
		workID := ids.DeriveAt(ids.KindWorkItem, parent.CreatedAt, parent.ID,
			"blueprint-resource/"+string(ids.KindAttach)+"/"+generation.AttachID)
		capture, err := inputOwner.PrepareInput(ctx, parent, workID, generation, nil)
		if err != nil {
			return fail(err)
		}
		prepared.preparations = append(prepared.preparations, capture)
		prepared.mutations = append(prepared.mutations, capture.Change.Mutations...)
		prepared.generations = append(prepared.generations, capture.Generation)
		for _, condition := range capture.Change.Conditions {
			if err := appendCondition(condition); err != nil {
				return fail(err)
			}
		}
	}
	return prepared, nil
}

func (prepared *preparedBlueprintAttachInputGenerations) finish(
	ctx context.Context,
	store idempotencyRepositoryStore,
	parentTaskID string,
	result etcdstore.TransactionResult,
	publicationErr error,
) (etcdstore.TransactionResult, error) {
	if prepared == nil || len(prepared.generations) == 0 {
		return result, publicationErr
	}
	if publicationErr == nil && result.Succeeded {
		return result, nil
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), blueprintAttachInputCleanupTimeout)
	defer cancel()
	keys := []string{workqueue.TaskKey(parentTaskID)}
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
	for index, value := range read.Values {
		if value != nil {
			if value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision {
				return result, errors.Join(
					publicationErr,
					errs.New(errs.KindInternal, "Attach input publication returned invalid storage evidence"),
				)
			}
			present++
		}
	}
	if present == len(keys) {
		storedTask, err := workqueue.DecodeTask(read.Values[0].Value)
		if err != nil || storedTask.ID != parentTaskID || storedTask.Action != workexecution.ActionApply {
			return result, errors.Join(
				publicationErr,
				errs.New(errs.KindInternal, "Attach input publication has an invalid owning Task"),
			)
		}
		for index, generation := range prepared.generations {
			stored, err := attachinputs.Decode(read.Values[index+1].Value)
			expected, expectedErr := attachinputs.WorkInput(generation)
			actual, actualErr := attachinputs.WorkInput(stored)
			if err != nil || expectedErr != nil || actualErr != nil || expected != actual ||
				stored.ParentTaskID != parentTaskID || stored.EnvironmentID != storedTask.Scope.EnvironmentID {
				attachinputs.Clear(&stored)
				return result, errors.Join(publicationErr, errs.New(
					errs.KindInternal, "Blueprint Attach input publication evidence is corrupt",
				))
			}
			attachinputs.Clear(&stored)
		}
		return result, publicationErr
	}
	if read.Values[0] != nil {
		return result, errors.Join(publicationErr, errs.New(
			errs.KindInternal, "Blueprint Attach input publication is partially visible",
		))
	}
	for index := len(prepared.preparations) - 1; index >= 0; index-- {
		publicationErr = errors.Join(publicationErr, prepared.preparations[index].Abandon(cleanupCtx))
	}
	return result, publicationErr
}
