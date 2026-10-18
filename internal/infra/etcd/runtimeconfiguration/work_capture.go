package runtimeconfiguration

import (
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// WorkCapture binds file sources and their acknowledged predecessor to one
// host-bound operation. It is part of that operation's immutable input, never
// reconstructed from Task diagnostics. Secret retention is bound separately by
// the operation owner before publication.
type WorkCapture struct {
	TaskID        string                  `json:"task_id"`
	WorkItemID    string                  `json:"work_item_id"`
	Placement     workexecution.Placement `json:"placement"`
	ReadRevision  int64                   `json:"read_revision"`
	Generation    uint64                  `json:"generation"`
	Current       Reference               `json:"current"`
	Prior         *Reference              `json:"prior"`
	PriorRevision int64                   `json:"prior_revision"`
	Destinations  []string                `json:"destinations"`
}

func HeadKey(environmentID string) string {
	return "/v1/runtime/environment-configurations/" + environmentID
}

func (capture WorkCapture) Validate() error {
	if ids.Validate(ids.KindTask, capture.TaskID) != nil ||
		ids.Validate(ids.KindWorkItem, capture.WorkItemID) != nil ||
		capture.Placement.Validate() != nil || capture.Placement.Executor != workexecution.Agent ||
		capture.ReadRevision <= 0 || capture.Generation == 0 || ValidateReference(capture.Current) != nil ||
		capture.Current.Generation > capture.Generation || capture.PriorRevision < 0 ||
		capture.PriorRevision > capture.ReadRevision || (capture.Prior == nil) != (capture.PriorRevision == 0) ||
		len(capture.Destinations) > MaximumFiles {
		return errs.New(errs.KindValidationFailed, "work configuration capture is invalid")
	}
	if capture.Prior != nil && (ValidateReference(*capture.Prior) != nil ||
		capture.Prior.EnvironmentID != capture.Current.EnvironmentID ||
		capture.Prior.Generation > capture.Current.Generation ||
		capture.Prior.ID == capture.Current.ID && *capture.Prior != capture.Current) {
		return errs.New(errs.KindValidationFailed, "work configuration predecessor is invalid")
	}
	for index, destination := range capture.Destinations {
		if destination == "" || len(destination) > 4096 || strings.ContainsRune(destination, 0) ||
			index > 0 && capture.Destinations[index-1] >= destination {
			return errs.New(errs.KindValidationFailed, "configuration destinations must be uniquely sorted")
		}
	}
	return nil
}

func (capture WorkCapture) Matches(task workexecution.Task, item workexecution.Item) error {
	if err := capture.Validate(); err != nil {
		return err
	}
	if task.Validate() != nil || item.Validate() != nil || item.TaskID != task.ID ||
		capture.TaskID != task.ID || capture.WorkItemID != item.ID || capture.Placement != item.Placement ||
		capture.Current.EnvironmentID != task.Scope.EnvironmentID {
		return errs.New(errs.KindStateConflict, "configuration capture belongs to another work item or host")
	}
	global, local := false, false
	for _, claim := range item.Claims {
		if claim.ScopeID == task.Scope.EnvironmentID {
			global = global || claim.HostID == ""
			local = local || claim.HostID == item.Placement.HostID
		}
	}
	if !global || !local {
		return errs.New(errs.KindValidationFailed, "configuration work must claim its Environment and host files")
	}
	return nil
}
