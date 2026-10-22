package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// stageBlueprintAttachInputs moves immutable capture bytes out of the head
// transaction. Its existing staging descriptor owns abandonment and expiry;
// no capture can execute without the atomically accepted parent and full graph.
// The descriptor compare also prevents an expired owner from writing late.
func (repository *EnvironmentBlueprintRepository) stageBlueprintAttachInputs(
	ctx context.Context,
	parent workexecution.Task,
	descriptor keyvalue.Condition,
	consumerGuards []keyvalue.Condition,
	prepared *preparedBlueprintAttachInputGenerations,
) error {
	if len(prepared.generations) == 0 {
		return nil
	}
	sealKey := blueprints.EnvironmentBlueprintRevisionPrefixFinal(
		parent.Scope.EnvironmentID,
		parent.ID,
	) + "attach-input-seal"
	guards := append(slices.Clone(consumerGuards), descriptor,
		keyvalue.Condition{Key: workqueue.TaskKey(parent.ID)}, keyvalue.Condition{Key: sealKey})
	digest := sha256.New()
	var remaining []keyvalue.Mutation
	for _, mutation := range prepared.mutations {
		if !slices.ContainsFunc(prepared.generations, func(generation blueprintattachinputs.Generation) bool {
			return mutation.Key == blueprintattachinputs.Key(parent.ID, generation.AttachID)
		}) {
			remaining = append(remaining, mutation)
			continue
		}
		// Captures are create-only. Other writers cannot alter them: execution
		// requires the parent, and collection requires an abandoned descriptor
		// or the terminal parent's expired retention.
		conditions := append(slices.Clone(guards), keyvalue.Condition{Key: mutation.Key})
		for _, generation := range prepared.generations {
			if generation.Binding == nil && mutation.Key == blueprintattachinputs.Key(parent.ID, generation.AttachID) {
				conditions = append(conditions, keyvalue.Condition{Key: attachments.AttachKey(generation.AttachID)},
					keyvalue.Condition{Key: attachments.AttachNameKey(generation.EnvironmentID, generation.AttachName)})
			}
		}
		budget, err := repository.store.MeasureTransaction(ctx, conditions, []keyvalue.Mutation{mutation})
		if err != nil {
			return err
		}
		if !budget.Fits() {
			return errs.New(
				errs.KindValidationFailed,
				"one captured Attach input exceeds the storage transaction limit",
			)
		}
		result, err := repository.store.Transact(ctx, conditions, []keyvalue.Mutation{mutation})
		if err != nil {
			return err
		}
		if !result.Succeeded {
			return errs.New(errs.KindStateConflict, "Blueprint Attach input staging changed before capture")
		}
		_, _ = digest.Write([]byte(mutation.Key)) // hash.Hash writes cannot fail.
		_, _ = digest.Write(mutation.Value)
		clear(mutation.Value)
	}
	seal := []byte(hex.EncodeToString(digest.Sum(nil)))
	defer clear(seal)
	result, err := repository.store.Transact(
		ctx,
		guards,
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: sealKey, Value: seal}},
	)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Blueprint Attach capture seal raced abandonment")
	}
	// Keep backing/binding comparisons and Secret-pin activation in acceptance.
	// Consumer births are checked above while fencing the head and effect epoch;
	// those same fences in acceptance reject any intervening consumer mutation.
	prepared.conditions = slices.DeleteFunc(prepared.conditions, func(condition keyvalue.Condition) bool {
		return slices.ContainsFunc(prepared.generations, func(generation blueprintattachinputs.Generation) bool {
			return condition.Key == blueprintattachinputs.Key(parent.ID, generation.AttachID) ||
				condition.Key == workqueue.ItemKey(generation.WorkItemID) ||
				generation.Binding == nil && (condition.Key == attachments.AttachKey(generation.AttachID) ||
					condition.Key == attachments.AttachNameKey(generation.EnvironmentID, generation.AttachName))
		})
	})
	prepared.conditions = append(prepared.conditions, keyvalue.Condition{Key: sealKey, ModRevision: result.Revision})
	prepared.mutations = remaining
	return nil
}
