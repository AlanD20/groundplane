package rollingrelease

import (
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type Phase string

const (
	StartCandidate       Phase = "start_candidate"
	WaitCandidateHealthy Phase = "wait_candidate_healthy"
	SwitchTraffic        Phase = "switch_traffic"
	DrainPredecessor     Phase = "drain_predecessor"
	RemovePredecessor    Phase = "remove_predecessor"
	RemoveRetained       Phase = "remove_retained"
)

func (change Change) Phases() []Phase {
	phases := make([]Phase, 0, 5)
	if change.Candidate != nil {
		phases = append(phases, StartCandidate, WaitCandidateHealthy)
	}
	phases = append(phases, SwitchTraffic)
	if change.Retire != nil {
		phases = append(phases, DrainPredecessor, RemovePredecessor)
	}
	if change.Retained != nil {
		phases = append(phases, RemoveRetained)
	}
	return phases
}

// Progress is a bounded cursor, not permission to repeat an uncertain action.
// The owner must persist Begin before execution and Confirm only with verified
// step evidence in the same transaction that acknowledges that evidence.
type Progress struct {
	Change           uint32    `json:"change"`
	Phase            Phase     `json:"phase,omitempty"`
	InFlight         bool      `json:"in_flight"`
	ReplicaStartedAt time.Time `json:"replica_started_at"`
	ReplicaDeadline  time.Time `json:"replica_deadline"`
	DrainUntil       time.Time `json:"drain_until"`
}

func NewProgress(plan Plan) (Progress, error) {
	changes, err := plan.Changes()
	if err != nil {
		return Progress{}, err
	}
	return Progress{Change: 1, Phase: changes[0].Phases()[0]}, nil
}

func (progress Progress) Validate(plan Plan, taskDeadline time.Time) error {
	changes, err := plan.Changes()
	if err != nil {
		return err
	}
	if taskDeadline.IsZero() || progress.Change == 0 || progress.Change > uint32(len(changes)+1) {
		return errs.New(errs.KindValidationFailed, "rolling progress identity or Task deadline is invalid")
	}
	if progress.Change == uint32(len(changes)+1) {
		if progress.Phase != "" || progress.InFlight || !progress.ReplicaStartedAt.IsZero() ||
			!progress.ReplicaDeadline.IsZero() ||
			!progress.DrainUntil.IsZero() {
			return errs.New(errs.KindValidationFailed, "completed rolling progress has an unfinished replica")
		}
		return nil
	}
	phases := changes[progress.Change-1].Phases()
	index := phaseIndex(phases, progress.Phase)
	if index < 0 {
		return errs.New(errs.KindValidationFailed, "rolling progress phase is invalid")
	}
	if progress.ReplicaStartedAt.IsZero() {
		if index != 0 || progress.InFlight || !progress.ReplicaDeadline.IsZero() || !progress.DrainUntil.IsZero() {
			return errs.New(errs.KindValidationFailed, "rolling progress has no replica time budget")
		}
		return nil
	}
	if !progress.ReplicaDeadline.After(progress.ReplicaStartedAt) || progress.ReplicaDeadline.After(taskDeadline) ||
		progress.ReplicaDeadline.Sub(
			progress.ReplicaStartedAt,
		) > time.Duration(
			plan.ReplicaTimeoutSeconds,
		)*time.Second {
		return errs.New(errs.KindValidationFailed, "rolling replica deadline exceeds its captured budget")
	}
	draining := progress.Phase == DrainPredecessor || progress.Phase == RemovePredecessor
	if draining {
		if progress.DrainUntil.Before(progress.ReplicaStartedAt) ||
			progress.DrainUntil.After(progress.ReplicaDeadline) {
			return errs.New(errs.KindValidationFailed, "rolling drain deadline is invalid")
		}
	} else if !progress.DrainUntil.IsZero() {
		return errs.New(errs.KindValidationFailed, "rolling drain began before the traffic switch")
	}
	return nil
}

// Begin does not reset the budget when progressing from start to health/switch
// or after reconnect. An in-flight phase requires observation/recovery, not a
// fresh execution granted by a new deadline.
func (progress Progress) Begin(plan Plan, taskDeadline, now time.Time) (Progress, error) {
	if err := progress.Validate(plan, taskDeadline); err != nil {
		return Progress{}, err
	}
	if progress.Phase == "" || progress.InFlight {
		return Progress{}, errs.New(
			errs.KindStateConflict,
			"rolling phase is complete or requires observation before resuming",
		)
	}
	if now.IsZero() || !now.Before(taskDeadline) {
		return Progress{}, errs.New(errs.KindStateConflict, "rolling Task deadline expired")
	}
	if progress.ReplicaStartedAt.IsZero() {
		progress.ReplicaStartedAt = now.UTC()
		progress.ReplicaDeadline = now.Add(time.Duration(plan.ReplicaTimeoutSeconds) * time.Second).UTC()
		if progress.ReplicaDeadline.After(taskDeadline) {
			progress.ReplicaDeadline = taskDeadline.UTC()
		}
	}
	if now.Before(progress.ReplicaStartedAt) || !now.Before(progress.ReplicaDeadline) {
		return Progress{}, errs.New(
			errs.KindStateConflict,
			"rolling replica deadline expired or its clock moved backwards",
		)
	}
	progress.InFlight = true
	return progress, nil
}

// Confirm advances a verified stage only. A persisted in-flight marker is not
// completion evidence; callers must prove the captured operation's postcondition.
func (progress Progress) Confirm(plan Plan, taskDeadline time.Time, phase Phase, now time.Time) (Progress, error) {
	if err := progress.Validate(plan, taskDeadline); err != nil {
		return Progress{}, err
	}
	if !progress.InFlight || progress.Phase != phase {
		return Progress{}, errs.New(errs.KindStateConflict, "rolling completion does not match the in-flight phase")
	}
	if now.Before(progress.ReplicaStartedAt) || !now.Before(progress.ReplicaDeadline) {
		return Progress{}, errs.New(errs.KindStateConflict, "rolling completion is outside the replica time budget")
	}
	if phase == DrainPredecessor && now.Before(progress.DrainUntil) {
		return Progress{}, errs.New(errs.KindStateConflict, "rolling predecessor drain has not finished")
	}
	changes, err := plan.Changes()
	if err != nil {
		return Progress{}, err
	}
	change := changes[progress.Change-1]
	phases := change.Phases()
	index := phaseIndex(phases, phase)
	progress.InFlight = false
	if index+1 < len(phases) {
		progress.Phase = phases[index+1]
		if progress.Phase == RemoveRetained {
			progress.DrainUntil = time.Time{}
		}
		if phase == SwitchTraffic && change.Retire != nil {
			// Drain for at most 30 seconds, leaving at least half the remaining
			// replica budget for retirement. Waiting to the exact deadline would
			// make every short configured timeout fail before removal could start.
			drain := min(time.Duration(DrainSeconds)*time.Second, progress.ReplicaDeadline.Sub(now)/2)
			progress.DrainUntil = now.Add(drain).UTC()
		}
		return progress, nil
	}
	progress = Progress{Change: progress.Change + 1}
	if progress.Change <= uint32(len(changes)) {
		progress.Phase = changes[progress.Change-1].Phases()[0]
	}
	return progress, nil
}

func phaseIndex(phases []Phase, phase Phase) int {
	for index, candidate := range phases {
		if candidate == phase {
			return index
		}
	}
	return -1
}
