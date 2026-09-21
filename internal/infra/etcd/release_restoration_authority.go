package etcd

import (
	"bytes"
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"slices"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *TaskRepository) createReleaseRecoveryRecord(
	ctx context.Context,
	record taskassignments.ReleaseRecoveryRecord,
) (etcdstore.Versioned[taskassignments.ReleaseRecoveryRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[taskassignments.ReleaseRecoveryRecord]{}, err
	}
	value, err := taskassignments.EncodeReleaseRecoveryRecord(record)
	if err != nil {
		return etcdstore.Versioned[taskassignments.ReleaseRecoveryRecord]{}, err
	}
	defer clear(value)
	key := taskassignments.ReleaseRecoveryKey(record.TaskID)
	result, err := repository.store.Transact(
		ctx,
		[]etcdstore.Condition{{Key: key}},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: value}},
	)
	if err != nil {
		return etcdstore.Versioned[taskassignments.ReleaseRecoveryRecord]{}, err
	}
	if result.Succeeded {
		return etcdstore.Versioned[taskassignments.ReleaseRecoveryRecord]{
			Record:       record,
			Revision:     result.Revision,
			ReadRevision: result.Revision,
		}, nil
	}
	read, err := repository.store.Get(ctx, key)
	if err != nil || read.Entry == nil {
		return etcdstore.Versioned[taskassignments.ReleaseRecoveryRecord]{}, errs.New(
			errs.KindStateConflict,
			"release recovery record creation conflicted",
		)
	}
	stored, decodeErr := taskassignments.DecodeReleaseRecoveryRecord(read.Entry.Value)
	storedValue, encodeErr := taskassignments.EncodeReleaseRecoveryRecord(stored)
	if decodeErr != nil || encodeErr != nil || !bytes.Equal(value, storedValue) {
		clear(storedValue)
		return etcdstore.Versioned[taskassignments.ReleaseRecoveryRecord]{}, errs.New(
			errs.KindStateConflict,
			"release recovery record creation conflicted",
		)
	}
	clear(storedValue)
	return etcdstore.Versioned[taskassignments.ReleaseRecoveryRecord]{
		Record:       stored,
		Revision:     read.Entry.ModRevision,
		ReadRevision: read.ReadRevision,
	}, nil
}

func (repository *TaskRepository) releaseRecoveryDirectiveAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	procedure *agentpb.CandidateReleaseProcedure,
	revision int64,
) (*taskassignments.ReleaseRecoveryDirective, error) {
	if assignment.ExecutionMode != taskassignments.TaskExecutionModeRecoveryOnly ||
		!recordcodec.ValidSHA256(assignment.ReleaseRecoveryRecordSHA256) {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{taskassignments.ReleaseRecoveryKey(task.ID)}, Revision: revision,
	})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	record, err := taskassignments.DecodeReleaseRecoveryRecord(read.Values[0].Value)
	if err != nil || record.TaskID != task.ID || record.AssignmentID != assignment.AssignmentID ||
		record.OperationID != task.OperationID || record.PlanHash != task.PlanHash ||
		record.RestorationAuthoritySHA256 != assignment.RestorationAuthoritySHA256 ||
		!record.RecoveryDeadline.Equal(assignment.RecoveryDeadline) ||
		record.EvidenceRevision > revision {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	digest, err := taskassignments.ReleaseRecoveryRecordSHA256(record)
	if err != nil || digest != assignment.ReleaseRecoveryRecordSHA256 {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	wantSteps, err := releaseRestorationStepIDs(procedure, assignment.RestorationAuthority.Candidates)
	if err != nil || !slices.Equal(wantSteps, record.RecoveryStepIDs) {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	applicable, err := releaseApplicableCompensationStepIDs(
		procedure,
		assignment.RestorationAuthority.Candidates,
		record.MutationEvidence,
	)
	if err != nil {
		return nil, err
	}
	return &taskassignments.ReleaseRecoveryDirective{
		Phase: record.Phase, StepIDs: slices.Clone(record.RecoveryStepIDs), Cursor: record.Cursor,
		RecordSHA256: digest, ApplicableCompensationStepIDs: applicable,
	}, nil
}
