package controllerupgrade

import (
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Phase string

const (
	PhasePrepared    Phase = "prepared"
	PhaseActivating  Phase = "activating"
	PhaseTrial       Phase = "trial"
	PhaseStarting    Phase = "starting"
	PhaseHealthy     Phase = "healthy"
	PhaseRollingBack Phase = "rolling_back"
	PhaseRolledBack  Phase = "rolled_back"
	PhaseRecovered   Phase = "recovered"
	PhaseCancelled   Phase = "cancelled"
)

func (phase Phase) CanAdvance(next Phase) bool {
	switch phase {
	case PhasePrepared:
		return next == PhaseActivating || next == PhaseCancelled || next == PhaseRollingBack
	case PhaseActivating:
		return next == PhaseTrial || next == PhaseRollingBack
	case PhaseTrial:
		return next == PhaseStarting || next == PhaseRollingBack
	case PhaseStarting:
		return next == PhaseHealthy || next == PhaseRollingBack
	case PhaseRollingBack:
		return next == PhaseRolledBack
	case PhaseRolledBack:
		return next == PhaseRecovered
	default:
		return false
	}
}

// Settled includes complete Agent recovery, not merely restoring the executable.
func (phase Phase) Settled() bool {
	return phase == PhaseHealthy || phase == PhaseRecovered || phase == PhaseCancelled
}

func (phase Phase) Valid() bool {
	switch phase {
	case PhasePrepared, PhaseActivating, PhaseTrial, PhaseStarting, PhaseHealthy,
		PhaseRollingBack, PhaseRolledBack, PhaseRecovered, PhaseCancelled:
		return true
	default:
		return false
	}
}

type AgentPredecessor struct {
	ID         string `json:"id"`
	Image      string `json:"image"`
	Generation uint64 `json:"generation"`
}

// Journal pins all recovery inputs. Its single phase is host handoff evidence;
// the referenced durable Task remains the operator-visible operation authority.
type Journal struct {
	Schema             int               `json:"schema"`
	TaskID             string            `json:"task_id"`
	Release            Digest            `json:"release"`
	Manifest           Manifest          `json:"manifest"`
	PreviousController Digest            `json:"previous_controller"`
	Agent              *AgentPredecessor `json:"agent"`
	StartedAt          time.Time         `json:"started_at"`
	Deadline           time.Time         `json:"deadline"`
	Phase              Phase             `json:"phase"`
	TrialBootID        string            `json:"trial_boot_id"`
}

func (journal Journal) Validate() error {
	_, startOffset := journal.StartedAt.Zone()
	_, deadlineOffset := journal.Deadline.Zone()
	if journal.Schema != 1 || ids.Validate(ids.KindTask, journal.TaskID) != nil ||
		!journal.Release.Valid() || !journal.PreviousController.Valid() ||
		!journal.Phase.Valid() || journal.StartedAt.IsZero() || startOffset != 0 || deadlineOffset != 0 ||
		!journal.Deadline.After(journal.StartedAt) ||
		journal.Deadline.Sub(journal.StartedAt) > TaskTimeoutSeconds*time.Second {
		return errs.New(errs.KindValidationFailed, "controller recovery journal is invalid")
	}
	if err := journal.Manifest.Validate(); err != nil {
		return err
	}
	if journal.TrialBootID != "" && !ValidBootID(journal.TrialBootID) ||
		(journal.Phase == PhaseTrial || journal.Phase == PhaseStarting || journal.Phase == PhaseHealthy) &&
			!ValidBootID(journal.TrialBootID) {
		return errs.New(errs.KindValidationFailed, "controller recovery boot identity is invalid")
	}
	if journal.Agent != nil && (ids.Validate(ids.KindAgent, journal.Agent.ID) != nil ||
		!imageref.IsDigestPinned(journal.Agent.Image) || len(journal.Agent.Image) > 1024 ||
		journal.Agent.Generation == 0 || journal.Agent.Generation > math.MaxUint64-4) {
		return errs.New(errs.KindValidationFailed, "controller recovery Agent identity is invalid")
	}
	return nil
}

func (journal Journal) SameOperation(other Journal) bool {
	if journal.Schema != other.Schema || journal.TaskID != other.TaskID || journal.Release != other.Release ||
		journal.Manifest != other.Manifest || journal.PreviousController != other.PreviousController ||
		!journal.StartedAt.Equal(other.StartedAt) ||
		!journal.Deadline.Equal(other.Deadline) {
		return false
	}
	if journal.Agent == nil || other.Agent == nil {
		return journal.Agent == nil && other.Agent == nil
	}
	return *journal.Agent == *other.Agent
}

func ValidBootID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for index, character := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if character != '-' {
				return false
			}
			continue
		}
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}
