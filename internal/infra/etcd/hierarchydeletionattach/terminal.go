package hierarchydeletionattach

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type TerminalIdentity struct {
	ParentOperationID string
	AttachID          string
	PlanID            string
	TaskID            string
}

// Cleanup ciphertext retires only with the assigned child's successful effect
// proof, in the same transaction as native Secret source release.
func PrepareTerminal(
	ctx context.Context,
	store Reader,
	identity TerminalIdentity,
	revision int64,
) (Publication, error) {
	operationID := identity.ParentOperationID
	frozenKey := FrozenKey(operationID, identity.AttachID)
	planKey := ParentPlanKey(operationID, identity.PlanID)
	indexKey := PlanKey(identity.PlanID)
	read, err := store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{frozenKey, planKey, indexKey}, Revision: revision},
	)
	if err != nil {
		return Publication{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 3 || read.Values[0] == nil ||
		read.Values[1] == nil ||
		read.Values[2] == nil ||
		string(read.Values[2].Value) != operationID {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return Publication{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(read.Values)
	frozen, err := Decode(read.Values[0].Value)
	if err != nil {
		return Publication{}, err
	}
	defer Clear(&frozen)
	plan, err := Decode(read.Values[1].Value)
	if err != nil {
		return Publication{}, err
	}
	defer Clear(&plan)
	if frozen.ParentOperationID != operationID || frozen.Attach.ID != identity.AttachID ||
		plan.PlanID != identity.PlanID ||
		plan.TaskID != identity.TaskID ||
		plan.Attach.ID != identity.AttachID {
		return Publication{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	change := Publication{
		Conditions: []keyvalue.Condition{{Key: frozenKey, ModRevision: read.Values[0].ModRevision},
			{
				Key:         planKey,
				ModRevision: read.Values[1].ModRevision,
			}, {Key: indexKey, ModRevision: read.Values[2].ModRevision}},
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationDelete, Key: planKey},
			{Type: keyvalue.MutationDelete, Key: indexKey},
		}}
	if frozen.HookInputSet != nil {
		if frozen.HookInputs != nil {
			clear(frozen.HookInputs.Ciphertext)
		}
		frozen.HookInputs, frozen.HookInputSet, frozen.HookOperationID = nil, nil, ""
		value, err := Encode(frozen)
		if err != nil {
			return Publication{}, err
		}
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: frozenKey, Value: value},
		)
	}
	return change, nil
}
