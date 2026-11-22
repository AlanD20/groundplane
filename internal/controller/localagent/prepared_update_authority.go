package localagent

import (
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PreparedUpdateAuthority is operation-scoped evidence reconstructed from the
// current native Controller journal. Its fields remain private so a lifecycle
// caller cannot replace durable journal authority with a Task-only assertion.
type PreparedUpdateAuthority struct {
	taskID  string
	request UpdateRequest
	goal    UpdateGoal
	phase   upgrade.Phase
	process upgrade.Digest
}

// AuthorizePreparedUpdate binds the current Controller process and recovery
// phase to the exact Agent pair captured before native activation.
func AuthorizePreparedUpdate(
	journal upgrade.Journal,
	process upgrade.Digest,
	request UpdateRequest,
	goal UpdateGoal,
) (PreparedUpdateAuthority, error) {
	if err := journal.Validate(); err != nil {
		return PreparedUpdateAuthority{}, err
	}
	if err := validateUpdateRequest(request); err != nil {
		return PreparedUpdateAuthority{}, err
	}
	if journal.Agent == nil || journal.Manifest.ChannelSchema != agentprotocol.SchemaVersion ||
		journal.Agent.ID != request.AgentID || journal.Agent.Image != request.PreviousImage ||
		journal.Agent.Generation != request.StartingGeneration ||
		journal.Manifest.AgentImage != request.DesiredImage {
		return PreparedUpdateAuthority{}, preparedUpdateConflict()
	}

	switch goal {
	case UpdateFinish:
		if (journal.Phase != upgrade.PhaseStarting && journal.Phase != upgrade.PhaseHealthy) ||
			process != journal.Manifest.ControllerSHA256 {
			return PreparedUpdateAuthority{}, preparedUpdateConflict()
		}
	case UpdateRestore:
		if (journal.Phase != upgrade.PhaseRolledBack && journal.Phase != upgrade.PhaseRecovered) ||
			process != journal.PreviousController {
			return PreparedUpdateAuthority{}, preparedUpdateConflict()
		}
	default:
		return PreparedUpdateAuthority{}, preparedUpdateConflict()
	}

	return PreparedUpdateAuthority{
		taskID: journal.TaskID, request: request, goal: goal, phase: journal.Phase, process: process,
	}, nil
}

// Validate rejects zero, stale, or cross-operation authority before lifecycle
// recovery observes or mutates the installed Agent generation.
func (authority PreparedUpdateAuthority) Validate(request UpdateRequest, goal UpdateGoal) error {
	if err := validateUpdateRequest(request); err != nil {
		return err
	}
	if authority.taskID == "" || !authority.process.Valid() || authority.request != request || authority.goal != goal {
		return preparedUpdateConflict()
	}
	switch goal {
	case UpdateFinish:
		if authority.phase != upgrade.PhaseStarting && authority.phase != upgrade.PhaseHealthy {
			return preparedUpdateConflict()
		}
	case UpdateRestore:
		if authority.phase != upgrade.PhaseRolledBack && authority.phase != upgrade.PhaseRecovered {
			return preparedUpdateConflict()
		}
	default:
		return preparedUpdateConflict()
	}
	return nil
}

// AllowsCandidateRollback is false after the predecessor has already recorded
// complete Agent recovery. Recovered authority may settle only existing state.
func (authority PreparedUpdateAuthority) AllowsCandidateRollback() bool {
	return authority.goal == UpdateRestore && authority.phase == upgrade.PhaseRolledBack
}

func preparedUpdateConflict() error {
	return errs.New(errs.KindStateConflict, "native Agent update authority changed")
}
