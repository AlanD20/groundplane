package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareBlueprintAttachRemovalClaim(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (blueprintAttachTaskChange, error) {
	value, intent, found, err := repository.readAuthoredAttachRemovalIntent(ctx, task, revision)
	if err != nil {
		return blueprintAttachTaskChange{}, err
	}
	if !found || intent.Status != taskjournal.TaskStatusPending ||
		validateAuthoredAttachRemovalTaskOwner(task, intent) != nil {
		return blueprintAttachTaskChange{}, errs.New(
			errs.KindStateConflict, "Blueprint Attach removal claim authority is unavailable",
		)
	}
	if err := repository.validateAuthoredAttachRemovalCurrent(ctx, task, intent, revision); err != nil {
		return blueprintAttachTaskChange{}, err
	}
	return blueprintAttachTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{{
			Key: environmentchanges.AttachRemovalIntentKey(task.ID), ModRevision: value.ModRevision,
		}},
	}, nil
}

func (repository *TaskRepository) prepareBlueprintAttachRemovalAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	terminalAt time.Time,
	revision int64,
) (blueprintAttachTaskChange, error) {
	value, intent, found, err := repository.readAuthoredAttachRemovalIntent(ctx, task, revision)
	if err != nil {
		return blueprintAttachTaskChange{}, err
	}
	if !found || validateAuthoredAttachRemovalTaskOwner(task, intent) != nil {
		return blueprintAttachTaskChange{}, errs.New(
			errs.KindStateConflict, "Blueprint Attach removal acknowledgement authority is unavailable",
		)
	}
	if err := repository.validateAuthoredAttachRemovalCurrent(ctx, task, intent, revision); err != nil {
		return blueprintAttachTaskChange{}, err
	}
	terminal, err := environmentchanges.TerminalAttachRemovalIntent(intent, terminalStatus, terminalAt)
	if err != nil {
		return blueprintAttachTaskChange{}, err
	}
	encoded, err := environmentchanges.EncodeAttachRemovalIntent(terminal)
	if err != nil {
		return blueprintAttachTaskChange{}, err
	}
	return blueprintAttachTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{{
			Key: environmentchanges.AttachRemovalIntentKey(task.ID), ModRevision: value.ModRevision,
		}},
		mutations: []etcdstore.Mutation{{
			Type: etcdstore.MutationPut,
			Key:  environmentchanges.AttachRemovalIntentKey(task.ID), Value: encoded,
		}},
		values: [][]byte{encoded},
	}, nil
}

func (repository *TaskRepository) validateBlueprintAttachRemovalAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) error {
	_, intent, found, err := repository.readAuthoredAttachRemovalIntent(ctx, task, revision)
	if err != nil {
		return err
	}
	if !found || task.FinishedAt == nil || intent.Status != terminalStatus || intent.TerminalAt == nil ||
		!intent.TerminalAt.Equal(*task.FinishedAt) || validateAuthoredAttachRemovalTaskOwner(task, intent) != nil {
		return errs.New(errs.KindStateConflict, "Blueprint Attach removal terminal authority changed")
	}
	return nil
}

func (repository *TaskRepository) validateAuthoredAttachRemovalCurrent(
	ctx context.Context,
	task TaskRecord,
	intent environmentchanges.AttachRemovalIntent,
	revision int64,
) error {
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{attachrecord.AttachKey(intent.AttachID)}, Revision: revision,
	})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "Blueprint Attach removal target is unavailable")
	}
	defer etcdstore.ClearValues(read.Values)
	current, err := attachrecord.DecodeAttachRecord(read.Values[0].Value)
	if err != nil || current.ID != intent.AttachID || current.Status != core.AttachDetaching ||
		current.Operation != attachrecord.AttachOperationDetach || current.TaskID != task.ID ||
		!attachrecord.AttachImmutableEqual(current, intent.Detaching) {
		return errs.New(errs.KindStateConflict, "Blueprint Attach removal target changed")
	}
	return nil
}

func (repository *TaskRepository) readAuthoredAttachRemovalIntent(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (*etcdstore.KeyValue, environmentchanges.AttachRemovalIntent, bool, error) {
	key := environmentchanges.AttachRemovalIntentKey(task.ID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, environmentchanges.AttachRemovalIntent{}, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return nil, environmentchanges.AttachRemovalIntent{}, false,
			errs.New(errs.KindInternal, "Blueprint Attach removal intent read is incomplete")
	}
	if read.Values[0] == nil {
		return nil, environmentchanges.AttachRemovalIntent{}, false, nil
	}
	intent, err := environmentchanges.DecodeAttachRemovalIntent(read.Values[0].Value)
	if err != nil {
		return nil, environmentchanges.AttachRemovalIntent{}, false, err
	}
	return read.Values[0], intent, true, nil
}
