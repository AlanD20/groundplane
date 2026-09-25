package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintAttachChildPublication struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
}

func (publication *blueprintAttachChildPublication) clear() {
	if publication == nil {
		return
	}
	etcdstore.ClearMutationValues(publication.mutations)
	*publication = blueprintAttachChildPublication{}
}

func (repository *TaskRepository) prepareBlueprintAttachChildPublication(
	ctx context.Context,
	child TaskRecord,
	unit blueprintunits.Unit,
	revision int64,
) (blueprintAttachChildPublication, error) {
	parentID := child.Params[taskjournal.TaskBlueprintParentParam]
	attachID := child.Params[attachinputs.TaskAttachIDParam]
	if unit.Removal || unit.Target.Kind != ids.KindAttach || unit.Target.ID != attachID ||
		child.Type != taskjournal.TaskUpdate || child.Target != child.Owner.EnvironmentID ||
		child.Params[blueprints.EnvironmentDesiredRevisionParam] != parentID || len(child.Params) != 3 ||
		ids.Validate(ids.KindTask, parentID) != nil || ids.Validate(ids.KindAttach, attachID) != nil {
		return blueprintAttachChildPublication{}, errs.New(
			errs.KindValidationFailed, "Blueprint Attach child publication identity is invalid",
		)
	}
	key := attachinputs.Key(parentID, attachID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return blueprintAttachChildPublication{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return blueprintAttachChildPublication{}, errs.New(
			errs.KindStateConflict, "Blueprint Attach input generation is unavailable",
		)
	}
	defer etcdstore.ClearValues(read.Values)
	value := read.Values[0]
	generation, err := attachinputs.Decode(value.Value)
	if err != nil {
		return blueprintAttachChildPublication{}, err
	}
	defer attachinputs.Clear(&generation)
	if generation.ParentTaskID != parentID || generation.EnvironmentID != child.Owner.EnvironmentID ||
		generation.AttachID != attachID || generation.OperationID != child.OperationID ||
		generation.OwnerKind != attachinputs.OwnerParent || generation.OwnerTaskID != parentID ||
		generation.Transfer != nil || !blueprintAttachChildConfigurationMatches(child, generation) {
		return blueprintAttachChildPublication{}, errs.New(
			errs.KindStateConflict, "Blueprint Attach input generation ownership changed",
		)
	}
	transferred, err := attachinputs.TransferToChild(generation, child.ID)
	if err != nil {
		return blueprintAttachChildPublication{}, err
	}
	generationValue, err := attachinputs.Encode(transferred)
	if err != nil {
		return blueprintAttachChildPublication{}, err
	}
	publication := blueprintAttachChildPublication{
		conditions: []etcdstore.Condition{{Key: key, ModRevision: value.ModRevision}},
		mutations:  []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: generationValue}},
	}
	fail := func(cause error) (blueprintAttachChildPublication, error) {
		publication.clear()
		return blueprintAttachChildPublication{}, cause
	}
	record, err := attachrecord.NewPendingAttachRecord(
		generation.AttachID,
		generation.EnvironmentID,
		generation.AttachName,
		generation.BackingProjectID,
		generation.BackingEnvironmentID,
		generation.BackingServiceID,
		generation.BackingNetworkID,
		generation.ConsumerServiceID,
		generation.CredentialOwnerID,
		nil,
		generation.FactSets,
		child.ID,
		child.CreatedAt,
	)
	if err != nil {
		return fail(err)
	}
	record.HookBundle = true
	if err := attachrecord.ValidateAttachRecord(record); err != nil {
		return fail(err)
	}
	recordValue, err := attachrecord.EncodeAttachRecord(record)
	if err != nil {
		return fail(err)
	}
	factsValue, err := attachrecord.EncodeAttachEncryptedFacts(generation.GeneratedInputs)
	if err != nil {
		clear(recordValue)
		return fail(err)
	}
	intent := attachrecord.BlueprintAttachTaskIntent{
		TaskID: child.ID, EnvironmentID: generation.EnvironmentID,
		Status: taskjournal.TaskStatusPending, OwnsEnvironmentFence: false,
		Candidates: []attachrecord.Record{record}, CreatedAt: child.CreatedAt,
	}
	intentValue, err := attachrecord.EncodeBlueprintAttachTaskIntent(intent)
	if err != nil {
		clear(recordValue)
		clear(factsValue)
		return fail(err)
	}
	publication.conditions = append(publication.conditions,
		etcdstore.Condition{Key: attachrecord.AttachKey(record.ID)},
		etcdstore.Condition{Key: attachrecord.AttachNameKey(record.EnvironmentID, record.Name)},
		etcdstore.Condition{Key: attachrecord.AttachOwnerKey(record.EnvironmentID, record.ID)},
		etcdstore.Condition{Key: attachrecord.AttachServiceKey(record.ServiceID, record.ID)},
		etcdstore.Condition{Key: attachrecord.AttachBackingServiceKey(record.BackingServiceID, record.ID)},
		etcdstore.Condition{Key: attachrecord.AttachBackingProjectKey(record.BackingProjectID, record.ID)},
		etcdstore.Condition{Key: attachrecord.AttachFactsKey(record.ID)},
		etcdstore.Condition{Key: attachrecord.BlueprintAttachTaskIntentKey(child.ID)},
	)
	publication.mutations = append(publication.mutations,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachKey(record.ID), Value: recordValue},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachNameKey(record.EnvironmentID, record.Name), Value: []byte(record.ID)},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachOwnerKey(record.EnvironmentID, record.ID), Value: []byte(record.ID)},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachServiceKey(record.ServiceID, record.ID), Value: []byte(record.ID)},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachBackingServiceKey(record.BackingServiceID, record.ID), Value: []byte(record.ID)},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachBackingProjectKey(record.BackingProjectID, record.ID), Value: []byte(record.ID)},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachFactsKey(record.ID), Value: factsValue},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.BlueprintAttachTaskIntentKey(child.ID), Value: intentValue},
	)
	if generation.ResolvedInputs != nil {
		hookValue, err := taskconfiguration.EncodeBackingHookEncryptedInputs(*generation.ResolvedInputs)
		if err != nil {
			return fail(err)
		}
		hookKey := taskconfiguration.BackingHookTaskInputKey(child.OperationID)
		publication.conditions = append(publication.conditions, etcdstore.Condition{Key: hookKey})
		publication.mutations = append(publication.mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: hookKey, Value: hookValue,
		})
	}
	if generation.SecretPins != nil {
		pins, err := tasksecretpins.NewEtcdRepository(repository.store, generation.BackingProjectID)
		if err != nil {
			return fail(err)
		}
		root, found, err := pins.LoadActive(ctx, generation.OperationID)
		if err != nil {
			return fail(err)
		}
		binding := generation.SecretPins
		if !found || root.Phase() != tasksecretpins.RootPhaseActive ||
			root.TaskID() != parentID || root.AttemptID() != parentID ||
			root.MembershipCount() != binding.Count || root.MembershipSHA256() != binding.SHA256 {
			return fail(errs.New(errs.KindStateConflict, "Blueprint Attach Secret pin ownership changed"))
		}
		fragment, err := tasksecretpins.RetainForRetry(root, child.ID)
		if err != nil {
			return fail(err)
		}
		conditions, mutations, err := tasksecretpins.EtcdFragment(fragment)
		if err != nil {
			fragment.Clear()
			return fail(err)
		}
		publication.conditions = append(publication.conditions, conditions...)
		publication.mutations = append(publication.mutations, mutations...)
	}
	return publication, nil
}

func blueprintAttachChildConfigurationMatches(child TaskRecord, generation attachinputs.Generation) bool {
	if generation.ResolvedInputs == nil {
		return generation.SecretPins == nil && child.Configuration == nil
	}
	if child.Configuration == nil || child.Configuration.BackingHookInputs == nil ||
		child.Configuration.Current.ID != "" || child.Configuration.Prior != nil ||
		child.Configuration.PriorRevision != 0 {
		return false
	}
	expected := &taskconfiguration.TaskBackingHookInputSet{
		ProjectID:        generation.BackingProjectID,
		CiphertextSHA256: generation.ResolvedInputs.CiphertextSHA256,
		SecretSources:    generation.SecretSources,
	}
	if !taskconfiguration.SameTaskBackingHookInputSet(child.Configuration.BackingHookInputs, expected) {
		return false
	}
	if generation.SecretPins == nil {
		return child.Configuration.SecretPins == nil
	}
	return child.Configuration.SecretPins != nil && *child.Configuration.SecretPins == *generation.SecretPins
}
