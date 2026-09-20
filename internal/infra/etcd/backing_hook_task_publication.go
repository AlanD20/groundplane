package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type backingHookTaskPublication struct {
	task        TaskRecord
	input       *BackingHookEncryptedInputs
	pins        taskMaterializationProjectionChange
	cleanupPins bool
}

func prepareBackingHookTaskPublication(
	ctx context.Context,
	store hierarchyStore,
	task TaskRecord,
	input *BackingHookEncryptedInputs,
) (backingHookTaskPublication, error) {
	configured := task.Configuration != nil && task.Configuration.BackingHookInputs != nil
	if !configured {
		if input != nil {
			return backingHookTaskPublication{}, errs.New(
				errs.KindValidationFailed,
				"Backing hook encrypted inputs have no Task authority",
			)
		}
		return backingHookTaskPublication{task: task}, nil
	}
	if input == nil || input.OperationID != task.OperationID ||
		input.CiphertextSHA256 != task.Configuration.BackingHookInputs.CiphertextSHA256 {
		return backingHookTaskPublication{}, errs.New(
			errs.KindValidationFailed,
			"Backing hook encrypted inputs do not match their Task",
		)
	}
	if err := validateBackingHookEncryptedInputs(*input); err != nil {
		return backingHookTaskPublication{}, err
	}
	prepared, secretPins, err := prepareRecoverySecretPins(ctx, store, task)
	if err != nil {
		return backingHookTaskPublication{}, err
	}
	pins, err := recoverySecretPinActivation(secretPins)
	if err != nil {
		return backingHookTaskPublication{}, finishRecoverySecretPreparation(ctx, store, prepared, err)
	}
	owned := *input
	owned.Ciphertext = append([]byte(nil), input.Ciphertext...)
	owned.SecretSources = append([]tasksecretpinrecord.Record(nil), input.SecretSources...)
	return backingHookTaskPublication{
		task: prepared, input: &owned, pins: pins, cleanupPins: !secretPins.IsZero(),
	}, nil
}

func (publication *backingHookTaskPublication) clear() {
	if publication == nil {
		return
	}
	if publication.input != nil {
		clear(publication.input.Ciphertext)
		publication.input = nil
	}
	clearTaskMaterializationProjectionChange(publication.pins)
}

func (publication backingHookTaskPublication) finish(
	ctx context.Context,
	store hierarchyStore,
	cause error,
) error {
	if !publication.cleanupPins {
		return cause
	}
	return finishRecoverySecretPreparation(ctx, store, publication.task, cause)
}

func (publication backingHookTaskPublication) bind(
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	classify idempotencyPlanClassifier,
) ([]etcdstore.Condition, []etcdstore.Mutation, idempotencyPlanClassifier, error) {
	if publication.input == nil {
		return conditions, mutations, classify, nil
	}
	value, err := encodeBackingHookEncryptedInputs(*publication.input)
	if err != nil {
		return nil, nil, nil, err
	}
	key := backingHookTaskInputKey(publication.task.OperationID)
	conditions = append(conditions, etcdstore.Condition{Key: key})
	mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: value})
	conditions, mutations, classify = bindRecoverySecretPinPublication(
		publication.pins, conditions, mutations, classify,
	)
	return conditions, mutations, classify, nil
}
