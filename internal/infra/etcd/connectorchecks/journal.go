package connectorchecks

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/s3connector"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Journal fences every write with the active work item, attempt and executor.
// A saved destination or new credential value is never reread.
type Journal struct {
	repository *Repository
	owner      Owner
	attemptID  string
	identity   workexecution.Identity
}

func NewJournal(repository *Repository, owner Owner, attemptID string, identity workexecution.Identity) (*Journal, error) {
	if repository == nil || ids.Validate(ids.KindTask, owner.TaskID) != nil ||
		ids.Validate(ids.KindPlan, owner.PlanID) != nil || ids.Validate(ids.KindOperation, owner.OperationID) != nil ||
		ids.Validate(ids.KindEnvironment, owner.EnvironmentID) != nil ||
		ids.Validate(ids.KindWorkItem, owner.WorkItemID) != nil || ids.Validate(ids.KindAttempt, attemptID) != nil ||
		identity.Validate() != nil || identity.Executor != workexecution.Controller {
		return nil, errs.New(errs.KindValidationFailed, "connector check journal ownership is required")
	}
	return &Journal{repository: repository, owner: owner, attemptID: attemptID, identity: identity}, nil
}

func (journal *Journal) Start(ctx context.Context, now time.Time) error {
	return journal.mutate(ctx, true, func(record *Record, assignment workexecution.Attempt) error {
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

// Stop records failure and retains exact cleanup authority before the generic
// work layer releases executor capacity. Unknown remote mutations stay protected.
func (journal *Journal) Stop(ctx context.Context, failure s3connector.ConnectionCheckFailure, at time.Time) error {
	return journal.mutate(ctx, false, func(record *Record, _ workexecution.Attempt) error {
		return record.Stop(failure, at)
	})
}

// ResumeCleanup is deliberately not a retry of the permission check. Even if
// no mutation is pending, an interrupted execution cannot claim a passing result.
func (journal *Journal) ResumeCleanup(ctx context.Context) error {
	return journal.mutate(ctx, false, func(record *Record, _ workexecution.Attempt) error {
		if record.Phase != Checking && record.Phase != Cleaning && record.Phase != CleanupRequired {
			return errs.New(errs.KindStateConflict, "connector check has no interrupted execution to clean up")
		}
		record.Phase = Cleaning
		record.Outcome, record.Failure, record.FinishedAt = "", s3connector.CheckFailureNone, nil
		return nil
	})
}

func (journal *Journal) WriteConnectionCheckCleanup(
	ctx context.Context,
	cleanup s3connector.ConnectionCheckCleanup,
) error {
	return journal.mutate(ctx, true, func(record *Record, _ workexecution.Attempt) error {
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
	return journal.mutate(ctx, false, func(record *Record, _ workexecution.Attempt) error {
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

// RejectAssigned settles only an assignment with no start receipt. Its CAS
// races the start transaction, so exactly one can retain authority to proceed.
func (journal *Journal) RejectAssigned(ctx context.Context, failure s3connector.ConnectionCheckFailure, at time.Time) error {
	return journal.mutateState(ctx, false, true, func(record *Record, attempt workexecution.Attempt) error {
		if attempt.StartedAt != nil || attempt.Direction != workexecution.Forward || record.Phase != Ready {
			return errs.New(errs.KindStateConflict, "connector check already started; reconcile its effects")
		}
		return record.Stop(failure, at)
	})
}

// FinishUnstarted records cancellation or a local setup failure without
// fabricating a StartedAt or allowing any remote mutation.
func (journal *Journal) FinishUnstarted(
	ctx context.Context,
	failure s3connector.ConnectionCheckFailure,
	now time.Time,
) error {
	return journal.mutate(ctx, false, func(record *Record, _ workexecution.Attempt) error {
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
	return journal.mutate(ctx, false, func(record *Record, _ workexecution.Attempt) error {
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

func (journal *Journal) mutate(ctx context.Context, forward bool,
	change func(*Record, workexecution.Attempt) error,
) error {
	return journal.mutateState(ctx, forward, false, change)
}

func (journal *Journal) mutateState(ctx context.Context, forward, allowAssigned bool,
	change func(*Record, workexecution.Attempt) error,
) error {
	keys := []string{Key(journal.owner.TaskID), workqueue.ItemKey(journal.owner.WorkItemID),
		workqueue.AttemptKey(journal.attemptID), OperationKey(journal.owner.OperationID),
		workqueue.TaskKey(journal.owner.TaskID), workqueue.ExecutorKey(journal.identity)}
	read, err := journal.repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil || read.Values[3] == nil || read.Values[4] == nil || read.Values[5] == nil {
		return errs.New(errs.KindStateConflict, "connector check no longer has its Controller claim")
	}
	defer keyvalue.ClearValues(read.Values)
	record, err := Decode(read.Values[0].Value)
	if err != nil {
		return err
	}
	defer record.Clear()
	assignment, err := workqueue.DecodeAttempt(read.Values[2].Value)
	if err != nil || record.Owner != journal.owner || assignment.TaskID != journal.owner.TaskID ||
		assignment.ID != journal.attemptID || assignment.Executor != journal.identity ||
		assignment.WorkItemID != journal.owner.WorkItemID || assignment.StartedAt == nil && !allowAssigned || assignment.FinishedAt != nil ||
		string(read.Values[3].Value) != journal.owner.TaskID {
		return errs.New(errs.KindStateConflict, "connector check Controller ownership changed")
	}
	digest, err := PlanHash(record)
	if err != nil || assignment.Input.ID != record.Owner.PlanID || assignment.Input.SHA256 != digest {
		return errs.New(errs.KindStateConflict, "connector check attempt changed its captured input")
	}
	item, err := workqueue.DecodeItem(read.Values[1].Value)
	if err != nil || item.ID != journal.owner.WorkItemID || item.TaskID != journal.owner.TaskID ||
		item.ActiveAttemptID != assignment.ID ||
		(item.Phase != workexecution.Running && !(allowAssigned && item.Phase == workexecution.Assigned && assignment.StartedAt == nil && record.Phase == Ready)) ||
		item.Input == nil || *item.Input != assignment.Input {
		return errs.New(errs.KindStateConflict, "connector check execution ownership changed")
	}
	task, err := workqueue.DecodeTask(read.Values[4].Value)
	if err != nil || task.ID != journal.owner.TaskID {
		return errs.New(errs.KindStateConflict, "connector check Task ownership changed")
	}
	executor, err := workqueue.DecodeExecutor(read.Values[5].Value)
	if err != nil || executor.Identity != journal.identity {
		return errs.New(errs.KindStateConflict, "connector check executor was replaced")
	}
	if forward && record.Phase != Cleaning && (task.Stop != nil || item.ForwardRevoked ||
		assignment.Direction != workexecution.Forward || !time.Now().UTC().Before(assignment.Deadline)) {
		return errs.New(errs.KindStateConflict, "connector check no longer permits new probe effects")
	}
	conditions := make([]keyvalue.Condition, len(keys))
	for index, key := range keys {
		conditions[index] = keyvalue.Condition{Key: key, ModRevision: read.Values[index].ModRevision}
	}
	pinConditions, err := journal.repository.secretPinConditions(ctx, record, read.ReadRevision)
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
		// These at-most-two memberships were published with this operation, not
		// through a separate staging root. Release only the exact pins compared
		// above, together with proven cleanup and credential erasure.
		for _, condition := range pinConditions {
			mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: condition.Key})
		}
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

func (repository *Repository) secretPinConditions(
	ctx context.Context,
	record Record,
	revision int64,
) ([]keyvalue.Condition, error) {
	var conditions []keyvalue.Condition
	for _, source := range record.SecretSources {
		key := tasksecretpinrecord.Key(source.Pin.SecretID, source.Pin.OperationID)
		read, err := repository.store.GetMany(
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
