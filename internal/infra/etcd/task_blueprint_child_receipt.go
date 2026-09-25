package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// prepareBlueprintChildReceipt joins the verified child outcome to its unit
// claim in the same terminal transaction. Only a single-Service candidate
// Release has completed-effect proof here; other unit kinds need their own
// proof boundary before they can be published as private children.
func (repository *TaskRepository) prepareBlueprintChildReceipt(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	status taskjournal.TaskStatus,
	verifiedResult bool,
	terminalAt time.Time,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	if !taskjournal.IsBlueprintChild(task.Params) {
		return nil, nil, nil
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return nil, nil, err
	}
	snapshot, err := ledger.Load(ctx, task.Owner.EnvironmentID)
	if err != nil {
		return nil, nil, err
	}
	var execution *blueprintunits.ExecutionRecord
	for index := range snapshot.Executions {
		candidate := &snapshot.Executions[index].Record
		if candidate.PlanID == task.PlanID {
			execution = candidate
			break
		}
	}
	if execution == nil || execution.TaskID != task.ID ||
		execution.ParentTaskID != task.Params[taskjournal.TaskBlueprintParentParam] ||
		execution.Epoch != int64(assignment.ExecutionEpoch) ||
		execution.State != blueprintunits.Running && execution.State != blueprintunits.Draining {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint child execution receipt authority changed")
	}
	verifiedServiceRelease := verifiedResult && !execution.Unit.Removal &&
		execution.Unit.Target.Kind == ids.KindService &&
		task.Params[releaserender.TaskReleasePublicationParam] != ""
	verifiedNetworkEffect := verifiedResult && blueprintNetworkChildTaskMatches(task, execution.Unit)
	verifiedEffect := verifiedServiceRelease || verifiedNetworkEffect
	if status != taskjournal.TaskStatusCompleted &&
		(status != taskjournal.TaskStatusFailed && status != taskjournal.TaskStatusAborted ||
			!verifiedEffect) {
		if execution.State == blueprintunits.Draining {
			return nil, nil, nil
		}
		next := *execution
		next.State = blueprintunits.Draining
		plan, err := blueprintunits.PrepareMutation(snapshot, nil, []blueprintunits.ExecutionChange{{
			PlanID: task.PlanID, Next: &next,
		}})
		if err != nil {
			return nil, nil, err
		}
		return plan.Conditions(), plan.Mutations(), nil
	}
	if !verifiedEffect {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint child completion lacks unit effect proof")
	}
	manifestKey := ""
	manifestRevision := int64(0)
	if verifiedServiceRelease {
		publicationID := task.Params[releaserender.TaskReleasePublicationParam]
		manifestKey = releases.ReleaseManifestStagingKey(publicationID)
		manifestRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{manifestKey}, Revision: snapshot.ReadRevision,
		})
		if err != nil {
			return nil, nil, err
		}
		if manifestRead == nil || len(manifestRead.Values) != 1 || manifestRead.Values[0] == nil {
			return nil, nil, errs.New(errs.KindStateConflict, "Blueprint child Release manifest is missing")
		}
		defer etcdstore.ClearValues(manifestRead.Values)
		manifest, err := releases.DecodeReleaseRecord[releases.ReleaseStagedManifest](
			manifestRead.Values[0].Value, "release-staged-manifest",
		)
		if err != nil || len(manifest.Members) != 1 || manifest.OperationID != task.OperationID ||
			manifest.Members[0].ServiceID != execution.Unit.Target.ID {
			return nil, nil, errs.New(errs.KindStateConflict, "Blueprint child Release does not match its unit")
		}
		manifestRevision = manifestRead.Values[0].ModRevision
	}
	if assignment.ExecutionEpoch == 0 || recordcodec.ValidateID(ids.KindAssignment, assignment.AssignmentID) != nil {
		return nil, nil, errs.New(errs.KindInternal, "Blueprint child assignment identity is invalid")
	}
	if status != taskjournal.TaskStatusCompleted {
		plan, err := blueprintunits.PrepareMutation(snapshot, nil, []blueprintunits.ExecutionChange{{
			PlanID: task.PlanID,
		}})
		if err != nil {
			return nil, nil, err
		}
		conditions, mutations := plan.Conditions(), plan.Mutations()
		failedParent := status == taskjournal.TaskStatusFailed ||
			status == taskjournal.TaskStatusAborted && snapshot.HeadTaskID == execution.ParentTaskID
		if failedParent {
			failureConditions, failureMutations, err := repository.prepareBlueprintParentFailure(
				ctx,
				execution.ParentTaskID,
				task,
				terminalAt,
				snapshot.ReadRevision,
			)
			if err != nil {
				etcdstore.ZeroMutationBytes(mutations)
				return nil, nil, err
			}
			conditions = append(conditions, failureConditions...)
			mutations = append(mutations, failureMutations...)
		}
		return conditions, mutations, nil
	}
	applied := blueprintunits.AppliedRecord{
		EnvironmentID: task.Owner.EnvironmentID,
		Target:        execution.Unit.Target, State: blueprintunits.Applied,
		Fingerprint:  execution.Unit.Fingerprint,
		ParentTaskID: execution.ParentTaskID, SourceTaskID: task.ID, SourcePlanID: task.PlanID,
		SourceAssignment: assignment.AssignmentID, ExecutionEpoch: assignment.ExecutionEpoch,
	}
	plan, err := blueprintunits.PrepareMutation(snapshot,
		[]blueprintunits.AppliedChange{{Target: applied.Target, Next: &applied}},
		[]blueprintunits.ExecutionChange{{PlanID: task.PlanID}},
	)
	if err != nil {
		return nil, nil, err
	}
	conditions := plan.Conditions()
	if manifestKey != "" {
		conditions = append(conditions, etcdstore.Condition{Key: manifestKey, ModRevision: manifestRevision})
	}
	return conditions, plan.Mutations(), nil
}
