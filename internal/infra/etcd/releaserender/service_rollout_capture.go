package releaserender

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ServiceRolloutCapture freezes one Service release, independently of whether
// it was requested by Deploy, Rollback or Blueprint Apply. It grants no effect
// or file-restoration authority; those join the native work publication.
type ServiceRolloutCapture struct {
	Action             workexecution.Action    `json:"action"`
	TaskID             string                  `json:"task_id"`
	WorkItemID         string                  `json:"work_item_id"`
	OperationID        string                  `json:"operation_id"`
	PublicationID      string                  `json:"publication_id"`
	SourceReadRevision int64                   `json:"source_read_revision"`
	Scope              workexecution.Scope     `json:"scope"`
	Placement          workexecution.Placement `json:"placement"`
	CreatedAt          time.Time               `json:"created_at"`
	Deadline           time.Time               `json:"deadline"`
	Render             ReleaseRenderInput      `json:"render"`
	CandidateArtifact  []byte                  `json:"candidate_artifact"`
	Group              *GroupMemberBinding     `json:"group,omitempty"`
}

// OpenServiceRolloutCapture returns owned typed input, not a decoded
// legacy Task or generic execution plan. Strategy validation belongs to Release.
func OpenServiceRolloutCapture(capture ServiceRolloutCapture) (ReleaseRenderInput, error) {
	if ids.Validate(ids.KindTask, capture.TaskID) != nil ||
		ids.Validate(ids.KindWorkItem, capture.WorkItemID) != nil ||
		ids.Validate(ids.KindOperation, capture.OperationID) != nil ||
		releases.ValidatePublicationID(capture.PublicationID) != nil ||
		capture.SourceReadRevision <= 0 ||
		capture.Scope.Validate() != nil || capture.Scope.EnvironmentID == "" ||
		capture.Placement.Validate() != nil || capture.Placement.Executor != workexecution.Agent ||
		capture.CreatedAt.IsZero() || !capture.Deadline.After(capture.CreatedAt) ||
		capture.Deadline.Sub(capture.CreatedAt) > 24*time.Hour ||
		capture.Render.EnvironmentID != capture.Scope.EnvironmentID ||
		capture.Render.ProjectID != capture.Scope.ProjectID || capture.Render.TenantID != capture.Scope.TenantID ||
		capture.Render.Projection.EnvironmentID != capture.Scope.EnvironmentID ||
		!IsReleaseAction(capture.Action) ||
		capture.Action == workexecution.ActionApply && capture.Render.Projection.RevisionID != capture.TaskID {
		return ReleaseRenderInput{}, errs.New(
			errs.KindValidationFailed,
			"Service rollout capture identity, action or deadline is invalid",
		)
	}
	if err := ValidateReleaseRenderInput(capture.Render); err != nil {
		return ReleaseRenderInput{}, err
	}
	if capture.Group != nil && (capture.Group.Validate() != nil || capture.Action == workexecution.ActionApply) {
		return ReleaseRenderInput{}, errs.New(
			errs.KindValidationFailed,
			"Service rollout Group binding is invalid for this action",
		)
	}
	operation, err := ReleaseOperationKind(capture.Action)
	if err != nil {
		return ReleaseRenderInput{}, err
	}
	phases, err := OrderedReleaseHooks(operation, capture.Render.ServiceID, capture.Render.Hooks)
	if err != nil {
		return ReleaseRenderInput{}, err
	}
	phases.Clear()
	if _, err := OpenReleaseCandidateArtifact(capture); err != nil {
		return ReleaseRenderInput{}, err
	}
	return CloneReleaseRenderInput(capture.Render), nil
}

func EncodeServiceRolloutCapture(capture ServiceRolloutCapture) ([]byte, error) {
	if _, err := OpenServiceRolloutCapture(capture); err != nil {
		return nil, err
	}
	value, err := recordcodec.Encode("service-rollout", capture)
	if err != nil {
		return nil, err
	}
	if len(value) > keyvalue.MaximumBytes {
		clear(value)
		return nil, errs.New(errs.KindReleasePlanTooLarge, "Service rollout capture exceeds the storage request budget")
	}
	return value, nil
}

func DecodeServiceRolloutCapture(value []byte) (ServiceRolloutCapture, error) {
	if len(value) == 0 || len(value) > keyvalue.MaximumBytes {
		return ServiceRolloutCapture{}, errs.New(errs.KindValidationFailed, "Service rollout capture size is invalid")
	}
	capture, err := recordcodec.Decode[ServiceRolloutCapture](value, "service-rollout")
	if err != nil {
		return ServiceRolloutCapture{}, err
	}
	if _, err := OpenServiceRolloutCapture(capture); err != nil {
		return ServiceRolloutCapture{}, err
	}
	return capture, nil
}

// ServiceRolloutInput binds the full frozen input to the native work item.
// The final operation input may additionally bind file restoration evidence.
func ServiceRolloutInput(capture ServiceRolloutCapture) (workexecution.Input, error) {
	value, err := EncodeServiceRolloutCapture(capture)
	if err != nil {
		return workexecution.Input{}, err
	}
	digest := sha256.Sum256(value)
	clear(value)
	return workexecution.Input{ID: capture.Render.PlanID, SHA256: hex.EncodeToString(digest[:])}, nil
}
