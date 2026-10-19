package releasegroups

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RestorationPreparation is derivable from the accepted Group root. It binds
// restoration to the earlier member's captured preparation, without copying its
// artifacts or manufacturing the later failure before that failure exists.
type RestorationPreparation struct {
	PlanID            string              `json:"plan_id"`
	TaskID            string              `json:"task_id"`
	GroupID           string              `json:"group_id"`
	GroupOperationID  string              `json:"group_operation_id"`
	Ordinal           uint32              `json:"ordinal"`
	ServiceID         string              `json:"service_id"`
	SourceWorkID      string              `json:"source_work_id"`
	WorkItemID        string              `json:"work_item_id"`
	SourcePreparation workexecution.Input `json:"source_preparation"`
}

func (preparation RestorationPreparation) Reference() (workexecution.Input, error) {
	if ids.Validate(ids.KindPlan, preparation.PlanID) != nil || ids.Validate(ids.KindTask, preparation.TaskID) != nil ||
		ids.Validate(
			ids.KindReleaseGroup,
			preparation.GroupID,
		) != nil || ids.Validate(ids.KindOperation, preparation.GroupOperationID) != nil ||
		ids.Validate(
			ids.KindService,
			preparation.ServiceID,
		) != nil || ids.Validate(ids.KindWorkItem, preparation.SourceWorkID) != nil ||
		ids.Validate(
			ids.KindWorkItem,
			preparation.WorkItemID,
		) != nil || preparation.SourceWorkID == preparation.WorkItemID ||
		preparation.Ordinal == 0 || preparation.Ordinal > 32 || preparation.SourcePreparation.Validate() != nil {
		return workexecution.Input{}, errs.New(errs.KindValidationFailed, "Group restoration preparation is invalid")
	}
	value, err := recordcodec.Encode("release_group_restoration_preparation", preparation)
	if err != nil {
		return workexecution.Input{}, err
	}
	digest := sha256.Sum256(value)
	clear(value)
	return workexecution.Input{ID: preparation.PlanID, SHA256: hex.EncodeToString(digest[:])}, nil
}

func (capture ExecutionCapture) RestorationPreparation(index int) (RestorationPreparation, error) {
	if capture.Policy.Validate() != nil || index < 0 || index >= len(capture.Policy.Members) ||
		index >= len(capture.Members) {
		return RestorationPreparation{}, errs.New(errs.KindValidationFailed, "Group restoration has no captured member")
	}
	member := capture.Policy.Members[index]
	preparation := RestorationPreparation{
		PlanID: ids.DeriveAt(
			ids.KindPlan,
			capture.CreatedAt,
			capture.Policy.TaskID,
			"group-restoration/"+member.RestoreID,
		),
		TaskID: capture.Policy.TaskID, GroupID: capture.Policy.Manifest.ReleaseGroupID,
		GroupOperationID: capture.Policy.Manifest.OperationID, Ordinal: member.Member.Ordinal, ServiceID: member.Member.ServiceID,
		SourceWorkID: member.ForwardID, WorkItemID: member.RestoreID, SourcePreparation: capture.Members[index].Forward,
	}
	if _, err := preparation.Reference(); err != nil {
		return RestorationPreparation{}, err
	}
	return preparation, nil
}
