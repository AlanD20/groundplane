package servicelifecyclework

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// LoadHookInputs reads the envelope pinned by this work, never current Secrets.
// The caller owns clearing returned ciphertext and joining the read conditions
// with its active-attempt authorization before delivering plaintext.
func (repository *Repository) LoadHookInputs(ctx context.Context, view workqueue.View) (Input, *taskconfiguration.BackingHookEncryptedInputs, []keyvalue.Condition, error) {
	captured, guard, err := repository.Load(ctx, view)
	if err != nil {
		return Input{}, nil, nil, err
	}
	guards := []keyvalue.Condition{guard}
	pins, err := repository.hookPinGuard(ctx, captured, view.ReadRevision)
	if err != nil {
		return Input{}, nil, nil, err
	}
	guards = append(guards, pins...)
	if captured.HookInputs == nil {
		return captured, nil, guards, nil
	}
	key := taskconfiguration.BackingHookTaskInputKey(captured.HookInputs.OperationID)
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return Input{}, nil, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return Input{}, nil, nil, conflict("captured Service lifecycle hook inputs are unavailable")
	}
	envelope, err := taskconfiguration.DecodeBackingHookEncryptedInputs(read.Values[0].Value)
	if err != nil {
		return Input{}, nil, nil, err
	}
	if envelope.OperationID != captured.HookInputs.OperationID || envelope.CiphertextSHA256 != captured.HookInputs.CiphertextSHA256 {
		clear(envelope.Ciphertext)
		return Input{}, nil, nil, conflict("Service lifecycle hook envelope differs from its captured authority")
	}
	guards = append(guards, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision})
	return captured, &envelope, guards, nil
}
