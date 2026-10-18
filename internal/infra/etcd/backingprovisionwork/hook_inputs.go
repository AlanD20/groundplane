package backingprovisionwork

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
)

// LoadHookInputs returns only the operation's retained envelope and comparisons.
// Its caller must authorize the current hook boundary before opening any values.
func (repository *Repository) LoadHookInputs(ctx context.Context, view workqueue.View) (Input, *taskconfiguration.BackingHookEncryptedInputs, []keyvalue.Condition, error) {
	input, capture, err := repository.Load(ctx, view)
	if err != nil {
		return Input{}, nil, nil, err
	}
	transferred := false
	defer func() {
		if !transferred {
			clear(input.Wire)
		}
	}()
	procedure, err := input.Procedure()
	if err != nil {
		return Input{}, nil, nil, err
	}
	if procedure.AfterStart == nil {
		return Input{}, nil, nil, invalid("backing creation has no captured after-start hook")
	}
	guards := []keyvalue.Condition{capture}
	if hook := input.Hook; hook != nil && hook.Pins != nil {
		pins, err := tasksecretpins.NewEtcdRepository(repository.store, procedure.ProjectId)
		if err != nil {
			return Input{}, nil, nil, err
		}
		root, found, err := pins.LoadActive(ctx, procedure.OperationId, view.ReadRevision)
		if err != nil {
			return Input{}, nil, nil, err
		}
		if !found {
			return Input{}, nil, nil, invalid("backing creation no longer holds its captured hook Secret values")
		}
		guard, err := root.AdmissionCondition(*hook.Pins)
		if err != nil {
			return Input{}, nil, nil, err
		}
		guards = append(guards, keyvalue.Condition{Key: guard.Key, ModRevision: guard.ModRevision})
	}
	reference := procedure.AfterStart.EncryptedInputs
	if reference == nil {
		transferred = true
		return input, nil, guards, nil
	}
	key := taskconfiguration.BackingHookTaskInputKey(reference.InputId)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if err != nil {
		return Input{}, nil, nil, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return Input{}, nil, nil, invalid("backing creation captured hook values are unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > view.ReadRevision {
		return Input{}, nil, nil, invalid("backing creation captured hook values have an invalid storage revision")
	}
	envelope, err := taskconfiguration.DecodeBackingHookEncryptedInputs(value.Value)
	if err != nil {
		return Input{}, nil, nil, err
	}
	if envelope.OperationID != procedure.OperationId || envelope.CiphertextSHA256 != hex.EncodeToString(reference.Sha256) {
		clear(envelope.Ciphertext)
		return Input{}, nil, nil, invalid("backing creation hook envelope differs from its captured input")
	}
	guards = append(guards, keyvalue.Condition{Key: key, ModRevision: value.ModRevision})
	transferred = true
	return input, &envelope, guards, nil
}
