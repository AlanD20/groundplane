package blueprintunits

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func DefinitionWorkResultKey(workID string) string {
	return recordPrefix + "definition-results/" + workID
}

// PrepareDefinitionWorkSettlement joins acknowledgement and work completion.
// The caller joins the captured Script set/body or Route definition comparisons,
// any removal proof and the scheduler's dependency/conflict fence in this commit.
func PrepareDefinitionWorkSettlement(snapshot Snapshot, view workqueue.View, unit Unit,
	sourceRevision int64, at time.Time,
) (workqueue.Change, error) {
	return prepareDefinitionWorkSettlement(snapshot, view, unit, sourceRevision, nil, at)
}

func PrepareDNSWorkSettlement(snapshot Snapshot, view workqueue.View, unit Unit,
	sourceRevision int64, proof DNSReceipt, at time.Time,
) (workqueue.Change, error) {
	if unit.Target.Kind != ids.KindDNSRecord {
		return workqueue.Change{}, invalidRecord()
	}
	return prepareDefinitionWorkSettlement(snapshot, view, unit, sourceRevision, &proof, at)
}

func prepareDefinitionWorkSettlement(snapshot Snapshot, view workqueue.View, unit Unit,
	sourceRevision int64, dns *DNSReceipt, at time.Time,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if view.ReadRevision != snapshot.ReadRevision || view.Task.Revision <= 0 || view.Item.Revision <= 0 ||
		task.Validate() != nil || item.Validate() != nil || task.Action != workexecution.ActionApply ||
		task.ID != snapshot.HeadTaskID || task.Scope.EnvironmentID != snapshot.EnvironmentID ||
		!task.PublicationComplete || task.Stop != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		!at.Before(task.Deadline) || !at.Before(item.Deadline) || item.TaskID != task.ID ||
		item.Operation != workexecution.ResourceDefinition || item.Placement.Executor != workexecution.Controller ||
		item.TargetID != unit.Target.ID || item.Input == nil || item.ForwardRevoked ||
		item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.Phase != workexecution.Ready ||
		sourceRevision <= 0 || sourceRevision > view.ReadRevision {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Resource definition cannot settle outside its current ready Apply work",
		)
	}
	expected, err := DefinitionWorkInput(task, unit)
	if err != nil || expected != *item.Input {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Resource definition work input differs from the accepted Blueprint",
		)
	}
	operationID := ids.DeriveAt(ids.KindOperation, task.CreatedAt, item.ID, "definition-settlement")
	var receipt MutationPlan
	switch unit.Target.Kind {
	case ids.KindScript:
		receipt, err = prepareControllerScriptReceipt(snapshot, unit, operationID, sourceRevision)
	case ids.KindRoute:
		receipt, err = prepareControllerRouteReceipt(snapshot, unit, operationID, sourceRevision)
	case ids.KindDNSRecord:
		if dns == nil {
			return workqueue.Change{}, invalidRecord()
		}
		receipt, err = PrepareControllerDNSSettlement(snapshot, unit, operationID, sourceRevision, *dns)
	default:
		return workqueue.Change{}, invalidRecord()
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	value, err := recordcodec.Encode("blueprint-definition-work-result", struct {
		TaskID         string              `json:"task_id"`
		WorkID         string              `json:"work_id"`
		EnvironmentID  string              `json:"environment_id"`
		Target         ResourceKey         `json:"target"`
		Removed        bool                `json:"removed"`
		SourceRevision int64               `json:"source_revision"`
		Input          workexecution.Input `json:"input"`
		SettledAt      time.Time           `json:"settled_at"`
	}{task.ID, item.ID, snapshot.EnvironmentID, unit.Target, unit.Removal, sourceRevision, expected, at})
	if err != nil {
		receipt.Clear()
		return workqueue.Change{}, err
	}
	digest := sha256.Sum256(value)
	evidence := workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, task.CreatedAt, item.ID, "definition-result"),
		SHA256: hex.EncodeToString(digest[:]),
	}
	settled, err := workqueue.PrepareUnstartedSettlement(view.Task, view.Item, workexecution.Completion{
		Outcome: workexecution.Succeeded, Evidence: evidence, EffectsSettled: true,
	}, at)
	if err != nil {
		receipt.Clear()
		clear(value)
		return workqueue.Change{}, err
	}
	key := DefinitionWorkResultKey(item.ID)
	settled.Conditions = append(settled.Conditions, receipt.Conditions()...)
	settled.Conditions = append(settled.Conditions, keyvalue.Condition{Key: key})
	settled.Mutations = append(settled.Mutations, receipt.Mutations()...)
	settled.Mutations = append(settled.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	return settled, nil
}
