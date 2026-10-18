package networkwork

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type CascadeRejectionReason string

const (
	CascadeSourceChanged CascadeRejectionReason = "captured_consumer_changed"
	CascadeDeadline      CascadeRejectionReason = "publication_deadline_exceeded"
	CascadeStopped       CascadeRejectionReason = "publication_stopped"
)

// CascadeRejection is immutable failure authority, not a copy of a storage
// error or a command log. It cannot contain Secret values or arbitrary output.
type CascadeRejection struct {
	TaskID     string                 `json:"task_id"`
	ZoneID     string                 `json:"zone_id"`
	WorkItemID string                 `json:"work_item_id,omitempty"`
	Reason     CascadeRejectionReason `json:"reason"`
	Outcome    workexecution.Outcome  `json:"outcome"`
	At         time.Time              `json:"at"`
}

func CascadeRejectionKey(taskID string) string { return CascadeGraphPrefix(taskID) + "rejection" }

func (repository *InputRepository) RejectStoppedCascade(ctx context.Context, task workqueue.TaskSnapshot,
	authority workqueue.Change, at time.Time,
) error {
	rejection := CascadeRejection{TaskID: task.Task.ID, ZoneID: task.Task.TargetID,
		Reason: CascadeDeadline, Outcome: workexecution.Failed, At: at}
	if task.Task.Stop != nil {
		rejection.Reason, rejection.Outcome = CascadeStopped, task.Task.Stop.Outcome
	}
	change, err := PrepareCascadeRejection(task, rejection, authority.Conditions)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	result, err := repository.transactions.Commit(ctx, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return conflict("Backing Zone publication changed before stop was recorded")
	}
	return nil
}

func (rejection CascadeRejection) validate(task workexecution.Task) error {
	if task.Action != workexecution.ActionRemove || task.Scope.TenantID != "" ||
		ids.Validate(ids.KindNetwork, task.TargetID) != nil || rejection.TaskID != task.ID ||
		rejection.ZoneID != task.TargetID || rejection.At.IsZero() || rejection.At.Location() != time.UTC ||
		rejection.At.Before(task.CreatedAt) || rejection.At.After(task.UpdatedAt) ||
		rejection.WorkItemID != "" && ids.Validate(ids.KindWorkItem, rejection.WorkItemID) != nil {
		return conflict("Backing Zone publication rejection has inconsistent ownership")
	}
	switch rejection.Reason {
	case CascadeSourceChanged:
		if rejection.WorkItemID == "" || rejection.Outcome != workexecution.Failed {
			return conflict("changed consumer rejection requires its captured work identity")
		}
	case CascadeDeadline:
		if rejection.WorkItemID != "" || rejection.Outcome != workexecution.Failed || rejection.At.Before(task.Deadline) {
			return conflict("publication deadline rejection predates the accepted deadline")
		}
	case CascadeStopped:
		if rejection.WorkItemID != "" || task.Stop == nil || rejection.Outcome != task.Stop.Outcome ||
			rejection.At.Before(task.Stop.RequestedAt) {
			return conflict("publication stop rejection differs from its accepted stop")
		}
	default:
		return conflict("Backing Zone publication rejection reason is invalid")
	}
	return nil
}

func cascadeRejectionReference(task workexecution.Task, raw []byte) workexecution.Input {
	sum := sha256.Sum256(raw)
	return workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, "cascade-publication-rejection"),
		SHA256: hex.EncodeToString(sum[:])}
}

// PrepareCascadeRejection preserves the first failure before bounded cleanup.
// The caller joins the captured graph and the exact contradictory source reads;
// a transient transport error is not evidence that the captured input changed.
func PrepareCascadeRejection(task workqueue.TaskSnapshot, rejection CascadeRejection,
	guards []keyvalue.Condition,
) (workqueue.Change, error) {
	if task.Revision <= 0 || task.Task.PublicationComplete || task.Task.FinishedAt != nil || len(guards) == 0 ||
		rejection.At.Before(task.Task.UpdatedAt) {
		return workqueue.Change{}, conflict("publication rejection requires an accepted closed graph")
	}
	next := workexecution.CloneTask(task.Task)
	next.UpdatedAt = rejection.At
	if err := rejection.validate(next); err != nil {
		return workqueue.Change{}, err
	}
	raw, err := recordcodec.Encode("backing-zone-publication-rejection", rejection)
	if err != nil {
		return workqueue.Change{}, err
	}
	evidence := cascadeRejectionReference(next, raw)
	if next.FirstFailure == nil && rejection.Outcome == workexecution.Failed {
		next.FirstFailure = &evidence
	}
	next.Outcome = rejection.Outcome
	taskBytes, err := workqueue.EncodeTask(next)
	if err != nil {
		clear(raw)
		return workqueue.Change{}, err
	}
	conditions := append([]keyvalue.Condition(nil), guards...)
	conditions = append(conditions, keyvalue.Condition{Key: workqueue.TaskKey(next.ID), ModRevision: task.Revision},
		keyvalue.Condition{Key: CascadeRejectionKey(next.ID)})
	return workqueue.Change{Conditions: conditions, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: workqueue.TaskKey(next.ID), Value: taskBytes},
		{Type: keyvalue.MutationPut, Key: CascadeRejectionKey(next.ID), Value: raw},
	}}, nil
}

func (repository *InputRepository) LoadCascadeRejection(ctx context.Context, task workqueue.TaskSnapshot,
	revision int64,
) (CascadeRejection, workexecution.Input, workqueue.Change, bool, error) {
	if task.Revision <= 0 || revision < task.Revision {
		return CascadeRejection{}, workexecution.Input{}, workqueue.Change{}, false, conflict("rejection read requires its Task snapshot")
	}
	key := CascadeRejectionKey(task.Task.ID)
	read, err := repository.read(ctx, []string{key}, revision)
	if err != nil {
		return CascadeRejection{}, workexecution.Input{}, workqueue.Change{}, false, err
	}
	defer keyvalue.ClearValues(read.Values)
	change := workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[0])}}}
	if read.Values[0] == nil {
		return CascadeRejection{}, workexecution.Input{}, change, false, nil
	}
	saved, err := recordcodec.Decode[CascadeRejection](read.Values[0].Value, "backing-zone-publication-rejection")
	if err == nil {
		err = saved.validate(task.Task)
	}
	if err != nil {
		return CascadeRejection{}, workexecution.Input{}, workqueue.Change{}, false, err
	}
	return saved, cascadeRejectionReference(task.Task, read.Values[0].Value), change, true, nil
}
