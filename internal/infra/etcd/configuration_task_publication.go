package etcd

import "context"

// configurationTaskPublication prepares the existing runtime-configuration
// and Secret-pin authorities for one Task that will write files. It owns no
// terminal success behavior; generic acknowledgement remains authoritative.
type configurationTaskPublication struct {
	task        TaskRecord
	pins        taskMaterializationProjectionChange
	active      bool
	cleanupPins bool
}

func prepareConfigurationTaskPublication(
	ctx context.Context,
	store hierarchyStore,
	task TaskRecord,
	fixedRevision int64,
) (configurationTaskPublication, error) {
	if len(task.Materializations) == 0 {
		return configurationTaskPublication{task: task}, nil
	}
	prepared, err := prepareRuntimeConfigurationTask(ctx, store, task, fixedRevision)
	if err != nil {
		return configurationTaskPublication{}, err
	}
	prepared, secretPins, err := prepareRecoverySecretPins(ctx, store, prepared)
	if err != nil {
		return configurationTaskPublication{}, err
	}
	pins, err := recoverySecretPinActivation(secretPins)
	if err != nil {
		return configurationTaskPublication{}, finishRecoverySecretPreparation(ctx, store, prepared, err)
	}
	return configurationTaskPublication{
		task: prepared, pins: pins, active: true, cleanupPins: !secretPins.IsZero(),
	}, nil
}

func (publication *configurationTaskPublication) clear() {
	if publication == nil {
		return
	}
	clearTaskMaterializationProjectionChange(publication.pins)
}

func (publication configurationTaskPublication) finish(
	ctx context.Context,
	store hierarchyStore,
	cause error,
) error {
	if !publication.cleanupPins {
		return cause
	}
	return finishRecoverySecretPreparation(ctx, store, publication.task, cause)
}

func (publication configurationTaskPublication) bind(
	conditions []Condition,
	mutations []Mutation,
	classify idempotencyPlanClassifier,
) ([]Condition, []Mutation, idempotencyPlanClassifier, error) {
	if !publication.active {
		return conditions, mutations, classify, nil
	}
	conditions, classify, err := bindRuntimeConfigurationPublication(publication.task, conditions, classify)
	if err != nil {
		return nil, nil, nil, err
	}
	conditions, mutations, classify = bindRecoverySecretPinPublication(
		publication.pins, conditions, mutations, classify,
	)
	return conditions, mutations, classify, nil
}
