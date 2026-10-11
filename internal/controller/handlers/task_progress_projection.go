package handlers

import (
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// Trimmed history cannot reset a completed step to pending. Retained events
// supersede the compact checkpoint; Controller-native steps follow their Task.
func taskProgressStatuses(
	record etcd.TaskRecord,
	snapshot etcd.TaskEventSnapshot,
	status apiTypes.TaskStatus,
) (map[string]apiTypes.TaskStatus, error) {
	values := make(map[string]apiTypes.TaskStatus, len(record.Steps))
	identities := make(map[string]taskjournal.TaskEventIdentity, len(record.Steps))
	initial := apiTypes.TaskPending
	if record.Executor == taskjournal.TaskExecutorController {
		initial = status
	}
	for _, step := range record.Steps {
		values[step.ID] = initial
	}
	set := func(identity taskjournal.TaskEventIdentity, state taskjournal.TaskEventState) error {
		if identity.ControllerOperation() && record.Executor == taskjournal.TaskExecutorController {
			return nil
		}
		id := identity.StepID
		mapped, err := taskEventAPIStatus(state)
		if err != nil {
			return err
		}
		if _, exists := values[id]; !exists {
			return errs.New(errs.KindInternal, "Task event references an unknown step")
		}
		prior := identities[id]
		if identity.Attempt < prior.Attempt || identity.Attempt == prior.Attempt && identity.Ordinal <= prior.Ordinal {
			return nil
		}
		identities[id] = identity
		values[id] = mapped
		return nil
	}
	for _, checkpoint := range record.EventCheckpoints {
		if err := set(checkpoint.Identity, checkpoint.State); err != nil {
			return nil, err
		}
	}
	for _, event := range snapshot.Events {
		if err := set(event.Identity, event.State); err != nil {
			return nil, err
		}
	}
	return values, nil
}

type taskReadinessObservation struct {
	value      *taskprogress.Readiness
	receivedAt *time.Time
}

func taskStepFailures(
	record etcd.TaskRecord,
	snapshot etcd.TaskEventSnapshot,
) (map[string]*apiTypes.TaskFailure, error) {
	values := make(map[string]*apiTypes.TaskFailure)
	identities := make(map[string]taskjournal.TaskEventIdentity)
	set := func(identity taskjournal.TaskEventIdentity, state taskjournal.TaskEventState, failure *taskprogress.Failure) {
		prior := identities[identity.StepID]
		if identity.Attempt < prior.Attempt || identity.Attempt == prior.Attempt && identity.Ordinal <= prior.Ordinal {
			return
		}
		identities[identity.StepID] = identity
		values[identity.StepID] = taskStepFailure(string(state), identity.StepID, failure)
	}
	for _, checkpoint := range record.EventCheckpoints {
		set(checkpoint.Identity, checkpoint.State, checkpoint.Failure)
	}
	for _, event := range snapshot.Events {
		payload, err := taskprogress.DecodePayload(event.Payload)
		if err != nil {
			return nil, err
		}
		set(event.Identity, event.State, payload.Failure)
	}
	return values, nil
}

func taskHealthWaitObservations(
	record etcd.TaskRecord,
	snapshot etcd.TaskEventSnapshot,
) (map[string]taskReadinessObservation, error) {
	values := make(map[string]taskReadinessObservation)
	identities := make(map[string]taskjournal.TaskEventIdentity)
	for _, checkpoint := range record.EventCheckpoints {
		values[checkpoint.Identity.StepID] = taskReadinessObservation{
			checkpoint.HealthWait,
			checkpoint.HealthWaitReceivedAt,
		}
		identity := checkpoint.Identity
		identity.Ordinal = checkpoint.HealthWaitOrdinal
		identities[identity.StepID] = identity
	}
	for _, event := range snapshot.Events {
		payload, err := taskprogress.DecodePayload(event.Payload)
		if err != nil {
			return nil, err
		}
		prior := identities[event.Identity.StepID]
		if event.Identity.Attempt < prior.Attempt {
			continue
		}
		if event.Identity.Attempt > prior.Attempt {
			delete(values, event.Identity.StepID)
			prior = event.Identity
			prior.Ordinal = 0
		}
		if payload.HealthWait != nil && event.Identity.Ordinal > prior.Ordinal {
			values[event.Identity.StepID] = taskReadinessObservation{payload.HealthWait, &event.ReceivedAt}
			prior.Ordinal = event.Identity.Ordinal
		}
		identities[event.Identity.StepID] = prior
	}
	return values, nil
}
