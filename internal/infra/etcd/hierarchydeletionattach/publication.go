package hierarchydeletionattach

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
)

type Publication struct {
	Conditions []keyvalue.Condition
	Mutations  []keyvalue.Mutation
	values     [][]byte
}

func (publication *Publication) Clear() {
	keyvalue.ClearByteSlices(publication.values)
	keyvalue.ClearMutationValues(publication.Mutations)
}

// PrepareInputPublication keeps one frozen native hook envelope across retries,
// and creates the immutable PlanID input in the child publication transaction.
func PrepareInputPublication(
	ctx context.Context,
	store Reader,
	input Input,
	operationID string,
	retry bool,
) (Publication, error) {
	publication := Publication{}
	frozenKey := FrozenKey(input.ParentOperationID, input.Attach.ID)
	if input.HookInputSet != nil {
		input.HookOperationID = operationID
	}
	if err := Validate(input); err != nil {
		publication.Clear()
		return Publication{}, err
	}
	if input.HookInputs != nil {
		hookValue, err := taskconfiguration.EncodeBackingHookEncryptedInputs(*input.HookInputs)
		if err != nil {
			publication.Clear()
			return Publication{}, err
		}
		publication.values = append(publication.values, hookValue)
		hookKey := taskconfiguration.BackingHookTaskInputKey(operationID)
		if !retry {
			publication.Conditions = append(publication.Conditions, keyvalue.Condition{Key: hookKey})
			publication.Mutations = append(
				publication.Mutations,
				keyvalue.Mutation{Type: keyvalue.MutationPut, Key: hookKey, Value: hookValue},
			)
		} else {
			read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{hookKey}})
			if err != nil {
				publication.Clear()
				return Publication{}, err
			}
			if read == nil || len(read.Values) != 1 || read.Values[0] == nil || string(read.Values[0].Value) != string(hookValue) {
				if read != nil {
					keyvalue.ClearValues(read.Values)
				}
				publication.Clear()
				return Publication{}, hierarchydeletion.CorruptHierarchyDeletion()
			}
			publication.Conditions = append(publication.Conditions, keyvalue.Condition{Key: hookKey, ModRevision: read.Values[0].ModRevision})
			keyvalue.ClearValues(read.Values)
		}
		if !retry {
			frozen := input
			frozen.PlanID, frozen.TaskID, frozen.ActionOrdinal = "", "", -1
			frozenValue, err := Encode(frozen)
			if err != nil {
				publication.Clear()
				return Publication{}, err
			}
			publication.values = append(publication.values, frozenValue)
			publication.Mutations = append(
				publication.Mutations,
				keyvalue.Mutation{Type: keyvalue.MutationPut, Key: frozenKey, Value: frozenValue},
			)
		}
	}
	value, err := Encode(input)
	if err != nil {
		publication.Clear()
		return Publication{}, err
	}
	publication.values = append(publication.values, value)
	planKey := ParentPlanKey(input.ParentOperationID, input.PlanID)
	indexKey := PlanKey(input.PlanID)
	publication.Conditions = append(
		publication.Conditions,
		keyvalue.Condition{Key: planKey},
		keyvalue.Condition{Key: indexKey},
	)
	publication.Mutations = append(
		publication.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: planKey, Value: value},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: indexKey, Value: []byte(input.ParentOperationID)},
	)
	return publication, nil
}
