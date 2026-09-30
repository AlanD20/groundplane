package taskjournal

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type ListFilter struct {
	Status       TaskStatus
	Type         TaskType
	ResourceKind string
}

func (scope ListFilter) Validate() error {
	if scope.Status != "" {
		switch scope.Status {
		case TaskStatusPending, TaskStatusRunning, TaskStatusCompleted,
			TaskStatusFailed, TaskStatusTimedOut, TaskStatusAborted:
		default:
			return errs.New(errs.KindValidationFailed, "Task status filter is invalid")
		}
	}
	if scope.Type != "" && !ValidTaskType(scope.Type) {
		return errs.New(errs.KindValidationFailed, "Task type filter is invalid")
	}
	if scope.ResourceKind != "" {
		switch scope.ResourceKind {
		case "component",
			"service",
			"zone",
			"volume",
			"route",
			"attach",
			"runner",
			"agent",
			"controller",
			"etcd",
			"environment",
			"project",
			"tenant",
			"image",
			"entry",
			"secret",
			"backup",
			"backing_zone",
			"connector",
			"script",
			"release_group",
			"hierarchy_deletion":
		default:
			return errs.New(errs.KindValidationFailed, "Task resource filter is invalid")
		}
	}
	return nil
}

func (scope ListFilter) Matches(status TaskStatus, kind TaskType, resource string) bool {
	return (scope.Status == "" || status == scope.Status) &&
		(scope.Type == "" || kind == scope.Type) &&
		(scope.ResourceKind == "" || resource == scope.ResourceKind)
}

// Cursor authority binds the complete filter, not just the hierarchy owner.
func (scope ListFilter) Collection() string {
	if scope.Status == "" && scope.Type == "" && scope.ResourceKind == "" {
		return "tasks"
	}
	digest := sha256.Sum256(
		[]byte(strings.Join([]string{string(scope.Status), string(scope.Type), scope.ResourceKind}, "\x00")),
	)
	return "tasks:" + hex.EncodeToString(digest[:])
}
