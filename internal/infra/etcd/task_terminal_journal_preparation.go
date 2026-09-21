package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

type taskTerminalJournalPreparation struct {
	terminalValue         []byte
	markerValue           []byte
	retentionValue        []byte
	taskRetentionValue    []byte
	conditions            []etcdstore.Condition
	mutations             []etcdstore.Mutation
	materializationWriter taskMaterializationWriterRecord
}

func (prepared *taskTerminalJournalPreparation) clearPrimary() {
	clear(prepared.terminalValue)
	clear(prepared.markerValue)
	clear(prepared.retentionValue)
}

func (repository *TaskRepository) prepareTaskTerminalJournal(
	ctx context.Context,
	task, terminal TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	terminalAt time.Time,
	assignment taskassignments.TaskAssignmentRecord,
	taskValue, assignmentValue, assignmentIndexValue *etcdstore.KeyValue,
	claimKey string,
	readRevision int64,
	timeoutEvidenceConditions []etcdstore.Condition,
	recoveryAcknowledgement releaseRecoveryAcknowledgement,
	terminalScriptSourceRelease scriptTerminalSourceRelease,
) (taskTerminalJournalPreparation, error) {
	transitionedMarker, markerKey, retentionKey, err := prepareTerminalTaskMarker(
		terminal,
		terminalStatus,
		terminalAt,
	)
	if err != nil {
		return taskTerminalJournalPreparation{}, err
	}
	materializationEnvironmentID, materializes, err := taskEnvironmentWriter(task)
	if err != nil {
		return taskTerminalJournalPreparation{}, err
	}
	lifecycleKey, _, _, err := repository.assignmentLifecycleIndexAtRevision(
		ctx, assignment, assignmentValue, readRevision,
	)
	if err != nil {
		return taskTerminalJournalPreparation{}, err
	}
	companionKeys := []string{
		taskjournal.TaskActiveOperationKey(
			task.OperationID,
		), markerKey, taskjournal.TaskQueueKey(task.Executor, task.ID), retentionKey,
		lifecycleKey,
	}
	writerKey := ""
	materializationWriter := taskMaterializationWriterRecord{}
	if materializes {
		writerKey = taskjournal.TaskMaterializationWriterKey(materializationEnvironmentID)
		companionKeys = append(companionKeys, writerKey)
	}
	companions, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     companionKeys,
		Revision: readRevision,
	})
	if err != nil {
		return taskTerminalJournalPreparation{}, err
	}
	if len(companions.Values) != len(companionKeys) || companions.Values[0] == nil || companions.Values[1] == nil ||
		companions.Values[2] != nil || companions.Values[3] != nil || companions.Values[4] == nil ||
		companions.Values[4].ModRevision != assignmentValue.ModRevision ||
		!bytes.Equal(companions.Values[4].Value, assignmentValue.Value) {
		return taskTerminalJournalPreparation{}, errs.New(
			errs.KindInternal,
			"running Task lifecycle records are inconsistent",
		)
	}
	if err := validateTaskLifecycleCompanions(task, companions.Values[0], companions.Values[1]); err != nil {
		return taskTerminalJournalPreparation{}, err
	}
	if materializes {
		if companions.Values[5] == nil {
			return taskTerminalJournalPreparation{}, errs.New(
				errs.KindInternal,
				"task materialization writer is missing",
			)
		}
		materializationWriter, err = decodeTaskMaterializationWriter(companions.Values[5].Value)
		if err != nil || validateTaskMaterializationWriterForTask(
			materializationWriter, task, materializationEnvironmentID,
		) != nil {
			return taskTerminalJournalPreparation{}, corruptTaskMaterializationWriter()
		}
	}
	transitionedMarker, err = hydrateTerminalTaskMarker(transitionedMarker, companions.Values[1].Value)
	if err != nil {
		return taskTerminalJournalPreparation{}, err
	}
	terminalValue, err := EncodeTaskRecord(terminal)
	if err != nil {
		return taskTerminalJournalPreparation{}, err
	}
	markerValue, err := idempotencyrecord.EncodeIdempotencyMarker(transitionedMarker)
	clear(transitionedMarker.Intent.Ciphertext)
	clear(transitionedMarker.Response.Body)
	if err != nil {
		clear(terminalValue)
		return taskTerminalJournalPreparation{}, err
	}
	retentionValue, err := json.Marshal(idempotencyrecord.RetentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
	if err != nil {
		clear(terminalValue)
		clear(markerValue)
		return taskTerminalJournalPreparation{}, errs.Wrap(errs.KindInternal, err)
	}
	taskRetentionKey, taskRetentionValue, err := prepareTaskRetentionIndex(terminal)
	if err != nil {
		clear(terminalValue)
		clear(markerValue)
		clear(retentionValue)
		return taskTerminalJournalPreparation{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskValue.ModRevision},
		{Key: claimKey, ModRevision: assignmentValue.ModRevision},
		{Key: taskjournal.TaskAssignmentIndexKey(task.ID), ModRevision: assignmentIndexValue.ModRevision},
		{Key: taskjournal.TaskActiveOperationKey(task.OperationID), ModRevision: companions.Values[0].ModRevision},
		{Key: markerKey, ModRevision: companions.Values[1].ModRevision},
		{Key: taskjournal.TaskQueueKey(task.Executor, task.ID)},
		{Key: retentionKey},
		{Key: taskRetentionKey},
		{Key: lifecycleKey, ModRevision: companions.Values[4].ModRevision},
	}
	conditions = append(conditions, timeoutEvidenceConditions...)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: terminalValue},
		{Type: etcdstore.MutationDelete, Key: claimKey},
		{Type: etcdstore.MutationDelete, Key: taskjournal.TaskAssignmentIndexKey(task.ID)},
		{Type: etcdstore.MutationDelete, Key: taskjournal.TaskActiveOperationKey(task.OperationID)},
		{Type: etcdstore.MutationPut, Key: markerKey, Value: markerValue},
		{Type: etcdstore.MutationPut, Key: retentionKey, Value: retentionValue},
		{Type: etcdstore.MutationPut, Key: taskRetentionKey, Value: taskRetentionValue},
		{Type: etcdstore.MutationDelete, Key: lifecycleKey},
	}
	if recoveryAcknowledgement.final {
		conditions = append(conditions, recoveryAcknowledgement.conditions...)
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: taskassignments.ReleaseRecoveryKey(task.ID)},
		)
	}
	conditions = append(conditions, terminalScriptSourceRelease.conditions...)
	mutations = append(mutations, terminalScriptSourceRelease.mutations...)
	if materializes {
		conditions = append(
			conditions,
			etcdstore.Condition{Key: writerKey, ModRevision: companions.Values[5].ModRevision},
		)
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: writerKey})
	}
	return taskTerminalJournalPreparation{
		terminalValue: terminalValue, markerValue: markerValue,
		retentionValue: retentionValue, taskRetentionValue: taskRetentionValue,
		conditions: conditions, mutations: mutations,
		materializationWriter: materializationWriter,
	}, nil
}
