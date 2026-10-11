package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *TaskRepository) rollingCompletionConditions(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	status taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	revision int64,
) ([]etcdstore.Condition, error) {
	if task.Params[releaserender.TaskReleasePublicationParam] == "" {
		if len(result.RollingEvidence) != 0 {
			return nil, errs.New(errs.KindStateConflict, "rolling completion is not authorized by this execution")
		}
		return nil, nil
	}
	_, procedure, err := repository.candidateReleaseDescriptorAtRevision(ctx, task, revision)
	if err != nil {
		return nil, err
	}
	restoredCount := 0
	for _, evidence := range result.RollingEvidence {
		if !evidence.Compensated {
			if assignment.ExecutionMode != taskassignments.TaskExecutionModeForward {
				return nil, errs.New(errs.KindStateConflict, "recovery cannot acknowledge a new rolling Release")
			}
			continue
		}
		if status == taskjournal.TaskStatusCompleted &&
			assignment.ExecutionMode == taskassignments.TaskExecutionModeForward {
			return nil, errs.New(errs.KindStateConflict, "restored rolling predecessor is not a successful deployment")
		}
		if err := validateRollingRestorationEvidence(assignment, procedure, evidence); err != nil {
			return nil, err
		}
		restoredCount++
	}
	var keys []string
	var members []*agentpb.CandidateReleaseMember
	for _, member := range procedure.GetMembers() {
		if member.GetRolling() != nil {
			evidence, hasEvidence := releaseRollingEvidence(result, member.GetServiceId())
			if hasEvidence && evidence.Compensated {
				continue
			}
			if !hasEvidence {
				if status == taskjournal.TaskStatusCompleted &&
					assignment.ExecutionMode == taskassignments.TaskExecutionModeForward {
					return nil, errs.New(errs.KindStateConflict, "completed rolling Release has no final runtime proof")
				}
				continue
			}
			keys = append(keys, taskassignments.RollingReleaseKey(task.ID, member.GetServiceId()))
			members = append(members, member)
		}
	}
	if len(members)+restoredCount != len(result.RollingEvidence) {
		return nil, errs.New(
			errs.KindStateConflict,
			"rolling completion names a Service outside the captured procedure",
		)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	keys = append(keys, releases.ReleasePublicationKey(task.Params[releaserender.TaskReleasePublicationParam]))
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, errs.New(errs.KindInternal, "rolling completion read is inconsistent")
	}
	markerValue := read.Values[len(members)]
	if markerValue == nil {
		return nil, releases.CorruptReleaseRecord()
	}
	marker, err := releases.DecodeReleaseRecord[releases.ReleasePublicationMarker](
		markerValue.Value,
		"release-publication",
	)
	if err != nil {
		return nil, err
	}
	conditions := make([]etcdstore.Condition, 0, len(keys))
	for index, value := range read.Values[:len(members)] {
		if value == nil {
			return nil, errs.New(errs.KindStateConflict, "rolling Release has no completed execution record")
		}
		record, err := taskassignments.DecodeRollingReleaseRecord(value.Value)
		if err != nil {
			return nil, err
		}
		expected, err := taskassignments.NewRollingReleaseRecord(task.PlanHash, assignment, members[index])
		if err != nil {
			return nil, err
		}
		if taskassignments.ValidateRollingReleaseAssignment(record, assignment) != nil ||
			record.TaskPlanSHA256 != task.PlanHash ||
			record.ProcedureSHA256 != expected.ProcedureSHA256 ||
			!record.CompletionProven ||
			record.Failed {
			return nil, errs.New(
				errs.KindStateConflict,
				"rolling Release has not proved completion of every replica change",
			)
		}
		evidence, _ := releaseRollingEvidence(result, members[index].GetServiceId())
		runtime, err := releasePreparedRuntime(marker, evidence.ServiceID)
		if err != nil {
			return nil, err
		}
		if err := executionplan.ValidateRollingCompletion(runtime, evidence); err != nil {
			return nil, err
		}
		conditions = append(conditions, etcdstore.Condition{Key: keys[index], ModRevision: value.ModRevision})
	}
	conditions = append(conditions, etcdstore.Condition{Key: markerValue.Key, ModRevision: markerValue.ModRevision})
	return conditions, nil
}

// A restored rolling set may precede any candidate strategy. Its proof binds to
// the assignment's exact historical bytes, not the candidate's forward cursor.
// Recovery-only completion still requires the separate durable recovery cursor.
func validateRollingRestorationEvidence(assignment taskassignments.TaskAssignmentRecord,
	procedure *agentpb.CandidateReleaseProcedure, evidence rollingrelease.Completion,
) error {
	authority := assignment.RestorationAuthority
	if authority == nil {
		return errs.New(errs.KindStateConflict, "rolling restoration has no captured authority")
	}
	for index, candidate := range authority.Candidates {
		if candidate.ServiceID != evidence.ServiceID {
			continue
		}
		if !evidence.Compensated || candidate.Target != taskassignments.ReleaseRestorationServingPredecessor ||
			index >= len(authority.NativePredecessors) || index >= len(procedure.GetMembers()) {
			break
		}
		member, witness := procedure.GetMembers()[index], authority.NativePredecessors[index]
		if member.GetServiceId() != candidate.ServiceID || member.GetCandidateReleaseId() != candidate.ReleaseID ||
			member.GetServingPredecessor() == nil || witness.ServiceID != candidate.ServiceID || evidence.ReleaseID == candidate.ReleaseID {
			break
		}
		serving, err := executionplan.OpenNativeServingRuntime(authority.EnvironmentID, candidate.ServiceID,
			witness.CurrentArtifact, witness.RetainedPriorArtifact)
		if err == nil && serving.Rolling != nil && evidence == executionplan.RollingCompletion(serving.Rolling) {
			return nil
		}
		break
	}
	return errs.New(errs.KindStateConflict, "rolling restoration does not match the complete captured predecessor")
}
