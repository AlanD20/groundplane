package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *EnvironmentBlueprintRepository) prepareBlueprintAttachConnection(
	ctx context.Context, parent TaskRecord, generation attachinputs.Generation, revision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	if !generation.SettlesInController() || generation.Binding != nil {
		return nil, nil, errs.New(errs.KindValidationFailed, "Attach connection requires provisioning")
	}
	createdAt := parent.CreatedAt
	if generation.Provision != nil {
		createdAt = generation.Provision.Candidate.CreatedAt
	}
	record, err := generation.PendingRecord(parent.ID, createdAt)
	if err != nil {
		return nil, nil, err
	}
	record.Status = core.AttachReady
	value, err := attachrecord.EncodeAttachRecord(record)
	if err != nil {
		return nil, nil, err
	}
	conditions := []etcdstore.Condition{{Key: attachrecord.AttachKey(record.ID)}}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: attachrecord.AttachKey(record.ID), Value: value},
	}
	fail := func(err error) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
		etcdstore.ClearMutationValues(mutations)
		return nil, nil, err
	}
	for _, key := range []string{
		attachrecord.AttachNameKey(record.EnvironmentID, record.Name),
		attachrecord.AttachOwnerKey(record.EnvironmentID, record.ID),
		attachrecord.AttachServiceKey(record.ServiceID, record.ID),
		attachrecord.AttachBackingServiceKey(record.BackingServiceID, record.ID),
		attachrecord.AttachBackingProjectKey(record.BackingProjectID, record.ID),
	} {
		conditions = append(conditions, etcdstore.Condition{Key: key})
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: []byte(record.ID)},
		)
	}
	factsKey := attachrecord.AttachFactsKey(record.ID)
	conditions = append(conditions, etcdstore.Condition{Key: factsKey})
	if record.OwnsCredential() && (len(generation.FactSets) != 0 || record.HookBundle) {
		facts, err := attachrecord.EncodeAttachEncryptedFacts(generation.GeneratedInputs)
		if err != nil {
			return fail(err)
		}
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: factsKey, Value: facts})
	}
	credentialConditions, err := repository.blueprintAttachCredentialConditions(ctx, generation, revision)
	if err != nil {
		return fail(err)
	}
	conditions = append(conditions, credentialConditions...)
	if generation.Provision != nil {
		conditions = append(conditions, generation.Provision.Conditions()...)
	}
	return conditions, mutations, nil
}
