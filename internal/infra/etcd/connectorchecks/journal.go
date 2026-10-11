package connectorchecks

import (
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/s3connector"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Journal fences every write with the actual native Controller claim and its
// indexed copy. A saved destination or new credential value is never reread.
type Journal struct {
	repository   *Repository
	owner        Owner
	assignmentID string
}

func NewJournal(repository *Repository, owner Owner, assignmentID string) (*Journal, error) {
	if repository == nil || ids.Validate(ids.KindTask, owner.TaskID) != nil ||
		ids.Validate(ids.KindPlan, owner.PlanID) != nil || ids.Validate(ids.KindOperation, owner.OperationID) != nil ||
		ids.Validate(ids.KindEnvironment, owner.EnvironmentID) != nil ||
		ids.Validate(ids.KindAssignment, assignmentID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "connector check journal ownership is required")
	}
	return &Journal{repository: repository, owner: owner, assignmentID: assignmentID}, nil
}

func (journal *Journal) Start(ctx context.Context, now time.Time) error {
	return journal.mutate(ctx, func(record *Record, assignment taskassignments.TaskAssignmentRecord) error {
		if record.Phase != Ready || !validTime(now) || !now.Before(assignment.Deadline) {
			return errs.New(errs.KindStateConflict, "connector check cannot start again or beyond its deadline")
		}
		if now.Before(record.CreatedAt) {
			now = record.CreatedAt
		}
		record.Phase, record.StartedAt = Checking, &now
		return nil
	})
}

// ResumeCleanup is deliberately not a retry of the permission check. Even if
// no mutation is pending, an interrupted execution cannot claim a passing result.
func (journal *Journal) ResumeCleanup(ctx context.Context) error {
	return journal.mutate(ctx, func(record *Record, _ taskassignments.TaskAssignmentRecord) error {
		if record.Phase != Checking && record.Phase != Cleaning {
			return errs.New(errs.KindStateConflict, "connector check has no interrupted execution to clean up")
		}
		record.Phase = Cleaning
		return nil
	})
}

func (journal *Journal) WriteConnectionCheckCleanup(
	ctx context.Context,
	cleanup s3connector.ConnectionCheckCleanup,
) error {
	return journal.mutate(ctx, func(record *Record, _ taskassignments.TaskAssignmentRecord) error {
		if record.Phase != Checking && record.Phase != Cleaning ||
			cleanup.Validate(record.Connector.Connector.Prefix) != nil ||
			cleanup.OperationID != record.Cleanup.OperationID || cleanup.ObjectKey != record.Cleanup.ObjectKey ||
			cleanup.MultipartKey != record.Cleanup.MultipartKey || cleanup.PayloadSHA256 != record.Cleanup.PayloadSHA256 ||
			cleanup.PayloadSizeBytes != record.Cleanup.PayloadSizeBytes ||
			record.Phase == Cleaning && ((!record.Cleanup.ObjectPending && cleanup.ObjectPending) ||
				(!record.Cleanup.MultipartPending && cleanup.MultipartPending)) {
			return errs.New(errs.KindStateConflict, "connector check cleanup changed its captured authority")
		}
		cleanup.UploadIDs = slices.Clone(cleanup.UploadIDs)
		record.Cleanup = cleanup
		return nil
	})
}

func (journal *Journal) WriteConnectionCheckStage(ctx context.Context, stage s3connector.ConnectionCheckStage) error {
	return journal.mutate(ctx, func(record *Record, _ taskassignments.TaskAssignmentRecord) error {
		if !stage.Valid() || record.Phase != Checking && record.Phase != Cleaning ||
			record.Phase == Cleaning && stage.Name != s3connector.CheckCleanup {
			return errs.New(errs.KindStateConflict, "connector check stage does not belong to this execution")
		}
		if stage.Name != s3connector.CheckCleanup {
			names := []s3connector.ConnectionCheckStageName{
				s3connector.CheckWriteObject,
				s3connector.CheckReadObject,
				s3connector.CheckMultipart,
			}
			if len(record.Stages) >= len(names) || stage.Name != names[len(record.Stages)] ||
				len(record.Stages) != 0 && !record.Stages[len(record.Stages)-1].Succeeded {
				return errs.New(errs.KindStateConflict, "connector check stage is out of order")
			}
		}
		// Cleanup resumes the same stage, not the probe. Replace its last result
		// so repeated cleanup failures cannot exhaust the journal and strand it.
		if stage.Name == s3connector.CheckCleanup && len(record.Stages) != 0 &&
			record.Stages[len(record.Stages)-1].Name == s3connector.CheckCleanup {
			record.Stages[len(record.Stages)-1] = stage
		} else {
			record.Stages = append(record.Stages, stage)
		}
		return nil
	})
}

// FinishUnstarted records cancellation or a local setup failure without
// fabricating a StartedAt or allowing any remote mutation.
func (journal *Journal) FinishUnstarted(
	ctx context.Context,
	failure s3connector.ConnectionCheckFailure,
	now time.Time,
) error {
	return journal.mutate(ctx, func(record *Record, _ taskassignments.TaskAssignmentRecord) error {
		if record.Phase != Ready || !validTime(now) || failure == s3connector.CheckFailureNone {
			return errs.New(errs.KindStateConflict, "connector check cannot settle as unstarted")
		}
		if now.Before(record.CreatedAt) {
			now = record.CreatedAt
		}
		record.Clear()
		record.Phase, record.Outcome, record.Failure, record.FinishedAt = Finished, Failed, failure, &now
		return nil
	})
}

func (journal *Journal) Finish(
	ctx context.Context,
	outcome Outcome,
	failure s3connector.ConnectionCheckFailure,
	now time.Time,
) error {
	return journal.mutate(ctx, func(record *Record, _ taskassignments.TaskAssignmentRecord) error {
		if record.Phase != Checking && record.Phase != Cleaning || !validTime(now) ||
			cleanupPending(record.Cleanup) || record.Phase == Cleaning && outcome != Interrupted {
			return errs.New(errs.KindStateConflict, "connector check must finish exact cleanup before settling")
		}
		if now.Before(*record.StartedAt) {
			now = *record.StartedAt
		}
		record.Clear()
		record.Phase, record.Outcome, record.FinishedAt = Finished, outcome, &now
		record.Failure = failure
		return nil
	})
}

func (journal *Journal) mutate(ctx context.Context,
	change func(*Record, taskassignments.TaskAssignmentRecord) error,
) error {
	keys := []string{Key(journal.owner.TaskID), taskjournal.ControllerTaskClaimKey(journal.owner.TaskID),
		taskjournal.TaskAssignmentIndexKey(journal.owner.TaskID), OperationKey(journal.owner.OperationID)}
	read, err := journal.repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil || read.Values[3] == nil {
		return errs.New(errs.KindStateConflict, "connector check no longer has its Controller claim")
	}
	defer keyvalue.ClearValues(read.Values)
	record, err := Decode(read.Values[0].Value)
	if err != nil {
		return err
	}
	defer record.Clear()
	assignment, err := taskassignments.DecodeTaskAssignment(read.Values[1].Value)
	if err != nil || record.Owner != journal.owner || assignment.TaskID != journal.owner.TaskID ||
		assignment.AssignmentID != journal.assignmentID || assignment.Executor != taskjournal.TaskExecutorController ||
		assignment.ExecutionMode != taskassignments.TaskExecutionModeForward ||
		string(read.Values[3].Value) != journal.owner.TaskID ||
		!bytes.Equal(read.Values[1].Value, read.Values[2].Value) {
		return errs.New(errs.KindStateConflict, "connector check Controller ownership changed")
	}
	conditions := make([]keyvalue.Condition, len(keys))
	for index, key := range keys {
		conditions[index] = keyvalue.Condition{Key: key, ModRevision: read.Values[index].ModRevision}
	}
	pinConditions, err := journal.secretPinConditions(ctx, record, read.ReadRevision)
	if err != nil {
		return err
	}
	conditions = append(conditions, pinConditions...)
	reference := ReferenceKey(record.Connector.Connector.ID, record.Owner.TaskID)
	environmentReference := EnvironmentReferenceKey(record.Owner.EnvironmentID, record.Owner.TaskID)
	referenceRead, err := journal.repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{reference, environmentReference}, Revision: read.ReadRevision,
	})
	if err != nil {
		return err
	}
	if referenceRead == nil || referenceRead.ReadRevision != read.ReadRevision || len(referenceRead.Values) != 2 {
		return errs.New(errs.KindInternal, "connector check reference read is incomplete")
	}
	defer keyvalue.ClearValues(referenceRead.Values)
	if referenceRead.Values[0] == nil || string(referenceRead.Values[0].Value) != record.Owner.TaskID ||
		referenceRead.Values[1] == nil || string(referenceRead.Values[1].Value) != record.Owner.TaskID {
		return errs.New(errs.KindStateConflict, "connector check cleanup reference changed")
	}
	conditions = append(
		conditions,
		keyvalue.Condition{Key: reference, ModRevision: referenceRead.Values[0].ModRevision},
	)
	conditions = append(
		conditions,
		keyvalue.Condition{Key: environmentReference, ModRevision: referenceRead.Values[1].ModRevision},
	)
	if err := change(&record, assignment); err != nil {
		return err
	}
	value, err := Encode(record)
	if err != nil {
		return err
	}
	defer clear(value)
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: keys[0], Value: value}}
	if record.Phase == Finished {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete,
			Key: reference}, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[3]},
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: environmentReference})
	}
	result, err := journal.repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return err
	}
	keyvalue.ClearValues(result.FailureReads)
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "connector check journal write raced its owner")
	}
	return nil
}

func (journal *Journal) secretPinConditions(
	ctx context.Context,
	record Record,
	revision int64,
) ([]keyvalue.Condition, error) {
	var conditions []keyvalue.Condition
	for _, source := range record.SecretSources {
		key := tasksecretpinrecord.Key(source.Pin.SecretID, source.Pin.OperationID)
		read, err := journal.repository.store.GetMany(
			ctx,
			keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision},
		)
		if err != nil {
			return nil, err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
			return nil, errs.New(errs.KindStateConflict, "connector check exact Secret pin is unavailable")
		}
		pin, err := tasksecretpinrecord.Decode(read.Values[0].Value)
		condition := keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}
		keyvalue.ClearValues(read.Values)
		if err != nil || pin != source.Pin {
			return nil, errs.New(errs.KindStateConflict, "connector check exact Secret pin changed")
		}
		if !slices.Contains(conditions, condition) {
			conditions = append(conditions, condition)
		}
	}
	return conditions, nil
}
