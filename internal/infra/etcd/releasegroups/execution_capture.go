package releasegroups

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	release "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const ExecutionCapturePrefix = "/v1/execution/release-groups/"

func ExecutionCaptureKey(taskID string) string { return ExecutionCapturePrefix + taskID }

// MemberPreparations are immutable operation-owned inputs. Forward and Restore
// have different identities even when restoration selects the original image.
// Neither reference is permission to reconstruct newer desired configuration.
type MemberPreparations struct {
	Forward workexecution.Input `json:"forward"`
	Restore workexecution.Input `json:"restore"`
}

// PreflightHook is a required Script before any member can change runtime. Its
// own native work owns execution/cleanup; no member silently reruns the hook.
type PreflightHook struct {
	WorkItemID string              `json:"work_item_id"`
	ScriptID   string              `json:"script_id"`
	ServiceID  string              `json:"service_id"`
	Ordinal    uint32              `json:"ordinal"`
	Input      workexecution.Input `json:"input"`
}

// ExecutionCapture is the single accepted native Group plan. Actual Service
// artifacts and secrets remain with their typed operation inputs, not duplicated
// in the Group's policy or progress. Graph includes captured bridge prerequisites.
type ExecutionCapture struct {
	PlanID             string                    `json:"plan_id"`
	Policy             release.GroupWorkManifest `json:"policy"`
	Scope              workexecution.Scope       `json:"scope"`
	Action             workexecution.Action      `json:"action"`
	Placement          workexecution.Placement   `json:"placement"`
	GroupName          string                    `json:"group_name"`
	GroupRevision      int64                     `json:"group_revision"`
	SourceReadRevision int64                     `json:"source_read_revision"`
	CreatedAt          time.Time                 `json:"created_at"`
	Deadline           time.Time                 `json:"deadline"`
	Members            []MemberPreparations      `json:"members"`
	Preflight          []PreflightHook           `json:"preflight,omitempty"`
	Graph              []workexecution.Item      `json:"graph"`
}

func (capture ExecutionCapture) Validate() error {
	if capture.Policy.Validate() != nil || ids.Validate(ids.KindPlan, capture.PlanID) != nil ||
		capture.Scope.Validate() != nil || capture.Scope.EnvironmentID != capture.Policy.Manifest.EnvironmentID ||
		capture.Action != workexecution.ActionDeploy && capture.Action != workexecution.ActionRollback ||
		capture.Placement.Validate() != nil || capture.Placement.Executor != workexecution.Agent ||
		capture.GroupName == "" || len(capture.GroupName) > 256 ||
		capture.GroupRevision <= 0 || capture.SourceReadRevision < capture.GroupRevision ||
		!recordcodec.IsCanonicalUTC(capture.CreatedAt) || !recordcodec.IsCanonicalUTC(capture.Deadline) ||
		!capture.Deadline.After(capture.CreatedAt) || capture.Deadline.Sub(capture.CreatedAt) > 24*time.Hour ||
		capture.Deadline.Sub(capture.CreatedAt) != time.Duration(capture.Policy.Manifest.ConfiguredTimeoutSeconds)*time.Second ||
		len(capture.Members) != len(capture.Policy.Members) || len(capture.Preflight) > 16 || len(capture.Graph) > int(workexecution.MaximumTaskItems) {
		return errs.New(errs.KindValidationFailed, "Release Group execution capture is invalid")
	}
	byID := make(map[string]workexecution.Item, len(capture.Graph))
	for _, item := range capture.Graph {
		if item.Validate() != nil || item.TaskID != capture.Policy.TaskID || byID[item.ID].ID != "" ||
			item.Placement != capture.Placement || !item.CreatedAt.Equal(capture.CreatedAt) ||
			!item.UpdatedAt.Equal(capture.CreatedAt) || !item.Deadline.Equal(capture.Deadline) ||
			item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.Result != nil || item.FirstFailure != nil ||
			item.Outcome != "" || item.Cleanup != workexecution.CleanupNone || item.ForwardRevoked {
			return errs.New(errs.KindValidationFailed, "Release Group graph contains different or executed work")
		}
		byID[item.ID] = item
	}
	for index, member := range capture.Policy.Members {
		if capture.Members[index].Forward.Validate() != nil || capture.Members[index].Restore.Validate() != nil ||
			capture.Members[index].Forward.ID == capture.Members[index].Restore.ID {
			return errs.New(errs.KindValidationFailed, "Release Group member preparation is incomplete")
		}
		preparation, err := capture.RestorationPreparation(index)
		if err != nil {
			return err
		}
		reference, err := preparation.Reference()
		if err != nil || reference != capture.Members[index].Restore {
			return errs.New(errs.KindValidationFailed, "Release Group restoration differs from its accepted source preparation")
		}
		for ordinal, id := range []string{member.ForwardID, member.RestoreID} {
			item, found := byID[id]
			operation := workexecution.Release
			if ordinal == 1 {
				operation = workexecution.ReleaseRestoration
			}
			if !found || item.TargetID != member.Member.ServiceID || item.Operation != operation ||
				item.Input != nil || item.Phase != workexecution.Waiting || item.Wait == nil || item.Wait.Reason != workexecution.WaitInput ||
				!slices.Contains(item.Claims, workexecution.Claim{ScopeID: member.Member.ServiceID}) ||
				!slices.Contains(item.Claims, workexecution.Claim{ScopeID: capture.Scope.EnvironmentID}) ||
				!slices.Contains(item.Claims, workexecution.Claim{HostID: capture.Placement.HostID, ScopeID: capture.Scope.EnvironmentID}) {
				return errs.New(errs.KindValidationFailed, "Release Group member work is not captured waiting Service execution")
			}
			delete(byID, id)
		}
	}
	previous := ""
	for _, hook := range capture.Preflight {
		item, found := byID[hook.WorkItemID]
		if !found || ids.Validate(ids.KindScript, hook.ScriptID) != nil || hook.Input.Validate() != nil ||
			hook.Ordinal == 0 || int(hook.Ordinal) > len(capture.Policy.Members) ||
			capture.Policy.Members[hook.Ordinal-1].Member.ServiceID != hook.ServiceID ||
			item.Operation != workexecution.Script || item.TargetID != hook.ScriptID ||
			item.Input == nil || *item.Input != hook.Input || item.Phase != workexecution.Ready ||
			!slices.Contains(item.Claims, workexecution.Claim{ScopeID: capture.Scope.EnvironmentID}) ||
			!slices.Contains(item.Claims, workexecution.Claim{HostID: capture.Placement.HostID, ScopeID: hook.ServiceID}) ||
			previous != "" && !slices.Contains(item.Dependencies, previous) {
			return errs.New(errs.KindValidationFailed, "Release Group preflight differs from its captured Script work")
		}
		delete(byID, hook.WorkItemID)
		previous = hook.WorkItemID
	}
	if previous != "" {
		for _, member := range capture.Policy.Members {
			item := capture.memberWork(member.ForwardID)
			if !slices.Contains(item.Dependencies, previous) {
				return errs.New(errs.KindValidationFailed, "Release Group member can start before required preflight cleanup")
			}
		}
	}
	for _, item := range byID {
		if item.Operation != workexecution.Network || item.Input == nil || item.Phase != workexecution.Ready {
			return errs.New(errs.KindValidationFailed, "Release Group graph contains an undeclared member or prerequisite")
		}
	}
	return nil
}

func (capture ExecutionCapture) memberWork(id string) workexecution.Item {
	for _, item := range capture.Graph {
		if item.ID == id {
			return item
		}
	}
	return workexecution.Item{}
}

func (capture ExecutionCapture) encoded() ([]byte, workexecution.Input, error) {
	if err := capture.Validate(); err != nil {
		return nil, workexecution.Input{}, err
	}
	value, err := recordcodec.Encode("release_group_execution", capture)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	if len(value) > keyvalue.MaximumBytes {
		clear(value)
		return nil, workexecution.Input{}, errs.New(errs.KindReleasePlanTooLarge, "Release Group captured graph exceeds the storage budget")
	}
	digest := sha256.Sum256(value)
	return value, workexecution.Input{ID: capture.PlanID, SHA256: hex.EncodeToString(digest[:])}, nil
}

func (capture ExecutionCapture) Task() (workexecution.Task, error) {
	value, reference, err := capture.encoded()
	clear(value)
	if err != nil {
		return workexecution.Task{}, err
	}
	task := workexecution.Task{ID: capture.Policy.TaskID, Actor: workexecution.OperatorActor,
		Scope: capture.Scope, Action: capture.Action, TargetID: capture.Policy.Manifest.ReleaseGroupID,
		TargetName: capture.GroupName, Intent: reference, CreatedAt: capture.CreatedAt, UpdatedAt: capture.CreatedAt,
		Deadline: capture.Deadline, ExpectedItems: uint32(len(capture.Graph))}
	return task, workexecution.ValidateGraph(task, capture.Graph)
}

// PrepareExecutionCapture joins native Task acceptance. The producer also joins
// complete immutable member preparations and source/host/hierarchy comparisons.
// No generic TaskRecord, network child Task or mutable GroupProgress is stored.
func PrepareExecutionCapture(task workexecution.Task, capture ExecutionCapture) (workqueue.Change, error) {
	expected, err := capture.Task()
	if err != nil {
		return workqueue.Change{}, err
	}
	if task.ID != expected.ID || task.Actor != expected.Actor || task.Scope != expected.Scope ||
		task.Action != expected.Action || task.TargetID != expected.TargetID || task.TargetName != expected.TargetName ||
		task.Intent != expected.Intent || task.ExpectedItems != expected.ExpectedItems || task.PublicationComplete ||
		task.Progress.Total() != 0 || task.Stop != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		!task.CreatedAt.Equal(expected.CreatedAt) || !task.UpdatedAt.Equal(expected.UpdatedAt) || !task.Deadline.Equal(expected.Deadline) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Release Group acceptance differs from its complete capture")
	}
	value, _, err := capture.encoded()
	if err != nil {
		return workqueue.Change{}, err
	}
	key := ExecutionCaptureKey(task.ID)
	reservation, err := workqueue.PrepareTaskReservation(task, workexecution.Claim{ScopeID: task.Scope.EnvironmentID})
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	reservation.Conditions = append(reservation.Conditions, keyvalue.Condition{Key: key},
		keyvalue.Condition{Key: ReleaseGroupRecordKey(task.TargetID), ModRevision: capture.GroupRevision})
	reservation.Mutations = append(reservation.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	return reservation, nil
}
