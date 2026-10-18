package agentworkjournal

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// RemovalRecoveryRecord retains metadata only. CaptureSHA256 binds retry
// ancestry without storing Compose files or secret configuration sources.
type RemovalRecoveryRecord struct {
	Stages        []lifecycleexecution.Stage  `json:"stages"`
	Source        lifecycleexecution.Recovery `json:"source"`
	CaptureSHA256 string                      `json:"capture_sha256"`
	Progress      removalexecution.Progress   `json:"progress"`
	Acknowledged  bool                        `json:"acknowledged"`
	Completion    *workexecution.Attempt      `json:"completion,omitempty"`
	Failure       *LifecycleFailureDelivery   `json:"failure,omitempty"`
}

func (record attemptRecord) validateRemovalRecovery() error {
	r := record.RemovalRecovery
	if r == nil || record.LifecycleRecovery != nil || record.Lifecycle != nil ||
		record.AttachProvisionRecovery != nil || record.AttachProvisionFailure != nil || record.AttachProvision != nil ||
		record.EntryRecovery != nil || record.EntryConfiguration != nil || record.ComponentRecovery != nil || record.Component != nil ||
		record.NetworkRecovery != nil || record.Network != nil || record.VolumeRecovery != nil || record.Volume != nil ||
		record.AttachRecovery != nil || record.AttachFailure != nil || record.AttachRemoval != nil || record.EnvironmentFailure != nil ||
		record.EnvironmentRemoval != nil || record.ScriptFailure != nil || record.ScriptRecovery != nil || len(record.Script) != 0 ||
		record.Release != nil || record.Effect != nil || record.Completion != nil || record.Failure != nil || record.Recovery != nil || record.ScriptCompletion != nil {
		return invalid()
	}
	digest, err := hex.DecodeString(r.CaptureSHA256)
	if err != nil || len(digest) != 32 || hex.EncodeToString(digest) != r.CaptureSHA256 ||
		r.Source.ValidateForStages(record.Attempt, r.Stages) != nil || record.Attempt.FinishedAt != nil ||
		r.Progress.ValidateEnvelope(record.Attempt) != nil || r.Progress.Sequence != record.Sequence ||
		!r.Progress.UpdatedAt.Equal(record.UpdatedAt) || r.Progress.Sequence == 0 && (r.Failure == nil || r.Acknowledged) {
		return invalid()
	}
	if f := r.Failure; f != nil {
		if !f.Reason.Valid() || r.Completion != nil || r.Progress.Phase == removalexecution.Complete {
			return invalid()
		}
		if terminal := f.Receipt; terminal != nil {
			if terminal.Validate() != nil || !workexecution.SameAssignment(record.Attempt, *terminal) ||
				terminal.StartedAt == nil || !terminal.StartedAt.Equal(*record.Attempt.StartedAt) ||
				terminal.FinishedAt == nil || terminal.FinishedAt.Before(record.UpdatedAt) || terminal.Result == nil ||
				terminal.Outcome != workexecution.Failed || len(f.Evidence) == 0 || len(f.Evidence) > removalexecution.MaximumProgressBytes {
				return invalid()
			}
			failure, err := jcs.Decode[removalexecution.Failure](f.Evidence)
			digest := sha256.Sum256(f.Evidence)
			if err != nil || failure.ValidateEnvelope(*terminal) != nil || failure.Reason != f.Reason ||
				failure.OriginalFailure != r.Source.Evidence || hex.EncodeToString(digest[:]) != terminal.Result.SHA256 {
				return invalid()
			}
		} else if len(f.Evidence) != 0 {
			return invalid()
		}
	}
	if terminal := r.Completion; terminal != nil {
		checkpoint, err := r.Progress.CheckpointEnvelope(record.Attempt, r.Source.Forward.ID, r.Source.Evidence)
		if err != nil || !r.Acknowledged || r.Progress.Phase != removalexecution.Complete || r.Progress.InFlight ||
			terminal.Validate() != nil || !workexecution.SameAssignment(record.Attempt, *terminal) ||
			terminal.StartedAt == nil || !terminal.StartedAt.Equal(*record.Attempt.StartedAt) || terminal.FinishedAt == nil ||
			terminal.FinishedAt.Before(record.UpdatedAt) || terminal.Outcome != workexecution.Succeeded ||
			terminal.Result == nil || *terminal.Result != checkpoint.Evidence {
			return invalid()
		}
	}
	return nil
}
