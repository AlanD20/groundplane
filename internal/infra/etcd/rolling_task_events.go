package etcd

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type rollingTaskEventWrite struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	budget     *rollingrelease.StepBudget
}

// The cursor write shares the event/assignment CAS. A Running acknowledgement
// therefore cannot authorize a replica mutation without retaining its budget
// and uncertain-effect position. Duplicate starts retain their original deadline.
func (repository *TaskRepository) prepareRollingTaskEvent(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	input taskjournal.TaskEventInput,
	receivedAt time.Time,
	revision int64,
	duplicate bool,
) (rollingTaskEventWrite, error) {
	if duplicate && (input.State != taskjournal.TaskEventStateRunning || input.Identity.Ordinal != 1) {
		return rollingTaskEventWrite{}, nil
	}
	if assignment.ExecutionMode != taskassignments.TaskExecutionModeForward ||
		task.Params[releaserender.TaskReleasePublicationParam] == "" {
		return rollingTaskEventWrite{}, nil
	}
	_, procedure, err := repository.candidateReleaseDescriptorAtRevision(ctx, task, revision)
	if err != nil {
		return rollingTaskEventWrite{}, err
	}
	var member *agentpb.CandidateReleaseMember
	for _, candidate := range procedure.GetMembers() {
		if candidate.GetRolling() != nil && slices.Contains(candidate.GetForwardStepIds(), input.Identity.StepID) {
			if member != nil {
				return rollingTaskEventWrite{}, errs.New(
					errs.KindInternal,
					"rolling Task step belongs to multiple Release members",
				)
			}
			member = candidate
		}
	}
	if member == nil {
		return rollingTaskEventWrite{}, nil
	}
	key := taskassignments.RollingReleaseKey(task.ID, member.GetServiceId())
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return rollingTaskEventWrite{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return rollingTaskEventWrite{}, errs.New(errs.KindInternal, "rolling execution read is inconsistent")
	}
	condition := etcdstore.Condition{Key: key}
	var record taskassignments.RollingReleaseRecord
	if value := read.Values[0]; value != nil {
		condition.ModRevision = value.ModRevision
		record, err = taskassignments.DecodeRollingReleaseRecord(value.Value)
	} else {
		if duplicate {
			return rollingTaskEventWrite{}, errs.New(errs.KindStateConflict, "rolling duplicate start has no retained execution record")
		}
		record, err = taskassignments.NewRollingReleaseRecord(task.PlanHash, assignment, member)
	}
	if err != nil {
		return rollingTaskEventWrite{}, err
	}
	if record.TaskPlanSHA256 != task.PlanHash || record.Plan.ServiceID != member.GetServiceId() {
		return rollingTaskEventWrite{}, errs.New(
			errs.KindInternal,
			"rolling cursor belongs to another Task plan or Service",
		)
	}
	if err := taskassignments.ValidateRollingReleaseAssignment(record, assignment); err != nil {
		return rollingTaskEventWrite{}, err
	}
	next, changed := record, false
	if !duplicate {
		next, changed, err = taskassignments.AdvanceRollingEvent(record, assignment, member, input, receivedAt)
	}
	if err != nil {
		return rollingTaskEventWrite{}, err
	}
	budget, err := taskassignments.RollingEventBudget(next, member, input)
	if err != nil {
		return rollingTaskEventWrite{}, err
	}
	write := rollingTaskEventWrite{conditions: []etcdstore.Condition{condition}, budget: budget}
	if !changed {
		return write, nil
	}
	encoded, err := taskassignments.EncodeRollingReleaseRecord(next)
	if err != nil {
		return rollingTaskEventWrite{}, err
	}
	write.mutations = []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: encoded}}
	return write, nil
}
