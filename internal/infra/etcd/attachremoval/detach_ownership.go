package attachremoval

import (
	"context"

	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *InputRepository) readOwnedDetach(ctx context.Context, view workqueue.View,
	captured DetachInput,
) (keyvalue.Versioned[attachrecord.Record], []keyvalue.Condition, error) {
	source := captured.Snapshot.Source
	keys := []string{attachrecord.AttachKey(source.Attach.ID), attachrecord.AttachFactsKey(source.Attach.ID)}
	read, err := repository.read(ctx, keys, view.ReadRevision)
	if err != nil {
		return keyvalue.Versioned[attachrecord.Record]{}, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return keyvalue.Versioned[attachrecord.Record]{}, nil, errs.New(errs.KindStateConflict, "Detach no longer owns an existing Attach")
	}
	current, err := attachrecord.DecodeAttachRecord(read.Values[0].Value)
	if err != nil || current.Operation != attachrecord.AttachOperationDetach || current.TaskID != view.Task.Task.ID ||
		!attachrecord.AttachImmutableEqual(current, source.Attach) ||
		(current.Status != "detaching" && current.Status != "failed") {
		return keyvalue.Versioned[attachrecord.Record]{}, nil, errs.New(errs.KindStateConflict, "Detach resource ownership changed")
	}
	if keyvalue.RevisionOf(read.Values[1]) != source.FactsRevision {
		return keyvalue.Versioned[attachrecord.Record]{}, nil, errs.New(errs.KindStateConflict, "Detach facts changed after their encrypted capture")
	}
	if read.Values[1] != nil {
		facts, err := attachrecord.DecodeAttachEncryptedFacts(read.Values[1].Value)
		if err != nil || facts.AttachID != current.ID || source.Facts == nil || facts.CiphertextSHA256 != source.Facts.CiphertextSHA256 {
			clear(facts.Ciphertext)
			return keyvalue.Versioned[attachrecord.Record]{}, nil, recordcodec.CorruptRecord()
		}
		clear(facts.Ciphertext)
	}
	return keyvalue.Versioned[attachrecord.Record]{Record: current, Revision: read.Values[0].ModRevision, ReadRevision: read.ReadRevision},
		[]keyvalue.Condition{{Key: keys[0], ModRevision: read.Values[0].ModRevision}, {Key: keys[1], ModRevision: source.FactsRevision}}, nil
}

// DetachResourceAdmission proves that this visible Task still owns the binding
// and captured facts. It cannot authorize commands by Task labels alone.
func (repository *InputRepository) DetachResourceAdmission(ctx context.Context, view workqueue.View) (workqueue.Change, error) {
	input, conditions, err := repository.LoadDetach(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer input.Clear()
	current, owned, err := repository.readOwnedDetach(ctx, view, input)
	if err != nil {
		return workqueue.Change{}, err
	}
	if current.Record.Status != "detaching" {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach must be explicitly retried before new forward execution")
	}
	return workqueue.Change{Conditions: append(conditions, owned...)}, nil
}

// PrepareDetachFailed retains the binding and encrypted facts after failure.
// Whether external effects are settled remains the separate execution result;
// this metadata change is never evidence that a command was undone.
func (repository *InputRepository) PrepareDetachFailed(ctx context.Context, view workqueue.View) (workqueue.Change, error) {
	input, conditions, err := repository.LoadDetach(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer input.Clear()
	current, owned, err := repository.readOwnedDetach(ctx, view, input)
	if err != nil {
		return workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: append(conditions, owned...)}
	if current.Record.Status == "failed" {
		return change, nil
	}
	failed, err := attachrecord.CompleteAttachDetaching(current.Record, view.Task.Task.ID, false)
	if err != nil {
		return workqueue.Change{}, err
	}
	raw, err := attachrecord.EncodeAttachRecord(failed)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: attachrecord.AttachKey(failed.ID), Value: raw}}
	return change, nil
}
