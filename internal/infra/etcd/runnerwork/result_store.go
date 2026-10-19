package runnerwork

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) RecordResult(ctx context.Context, result Result) error {
	input, conditions, err := repository.attemptAuthority(ctx, result.Attempt, false)
	if err != nil {
		return err
	}
	value, err := EncodeResult(input, result)
	if err != nil {
		return err
	}
	if result.Effects == NoEffects {
		// A timeout or missing token is not proof of no effects. Every possible
		// creation grant must still be absent in the result transaction.
		steps := runnerallocation.CreationRuntimeSteps()
		if input.Operation == runnerallocation.RuntimeOperationRemove {
			steps = runnerallocation.RemovalRuntimeSteps()
		}
		for _, step := range steps {
			conditions = append(conditions, keyvalue.Condition{Key: GrantKey(result.Attempt.ID, step)})
		}
	}
	key := ResultKey(result.Attempt.ID)
	conditions = append(conditions, keyvalue.Condition{Key: key})
	transaction, err := repository.store.Transact(
		ctx,
		conditions,
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}},
	)
	defer keyvalue.ClearValues(transaction.FailureReads)
	if err != nil {
		return err
	}
	if transaction.Succeeded {
		return nil
	}
	if len(transaction.FailureReads) == len(conditions) {
		last := transaction.FailureReads[len(conditions)-1]
		if last != nil && bytes.Equal(last.Value, value) {
			return nil
		}
	}
	return errs.New(errs.KindStateConflict, "Runner result or execution ownership changed before recording")
}

func (repository *Repository) LoadResult(ctx context.Context, attempt workexecution.Attempt,
	revision int64,
) (keyvalue.Versioned[Result], error) {
	if attempt.Validate() != nil || revision < 0 {
		return keyvalue.Versioned[Result]{}, invalidResult()
	}
	keys := []string{InputKey(attempt.WorkItemID), ResultKey(attempt.ID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[Result]{}, err
	}
	if read == nil {
		return keyvalue.Versioned[Result]{}, errs.New(errs.KindInternal, "Runner result read is missing")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || len(read.Values) != len(keys) {
		return keyvalue.Versioned[Result]{}, errs.New(errs.KindInternal, "Runner result read is incomplete")
	}
	if read.Values[0] == nil || read.Values[1] == nil {
		return keyvalue.Versioned[Result]{}, errs.New(
			errs.KindRequestNotFound,
			"Runner execution result is unavailable",
		)
	}
	input, err := Decode(read.Values[0].Value)
	if err != nil {
		return keyvalue.Versioned[Result]{}, err
	}
	result, err := DecodeResult(input, read.Values[1].Value)
	if err != nil {
		return keyvalue.Versioned[Result]{}, err
	}
	stored := result.Attempt
	if stored.ID != attempt.ID || stored.Number != attempt.Number || stored.Executor != attempt.Executor ||
		stored.TaskID != attempt.TaskID || stored.WorkItemID != attempt.WorkItemID || stored.Input != attempt.Input || stored.Direction != attempt.Direction {
		return keyvalue.Versioned[Result]{}, errs.New(
			errs.KindStateConflict,
			"Runner result belongs to another attempt",
		)
	}
	if attempt.FinishedAt != nil {
		completion, err := result.Completion(input)
		if err != nil || attempt.Result == nil || *attempt.Result != completion.Evidence ||
			attempt.Outcome != completion.Outcome {
			return keyvalue.Versioned[Result]{}, errs.New(
				errs.KindStateConflict,
				"Runner result differs from the accepted attempt outcome",
			)
		}
	}
	return keyvalue.Versioned[Result]{
		Record:       result,
		Revision:     read.Values[1].ModRevision,
		ReadRevision: read.ReadRevision,
	}, nil
}
