package serviceruntimerecord

import (
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// AcknowledgeCompletedRelease creates the resource-owned runtime from loaded
// native completion proof. The caller must commit it together with the proof's
// comparisons, serving state and work completion; this function writes nothing.
func AcknowledgeCompletedRelease(
	input releaseexecution.Input,
	proof releasework.CompletionProof,
	configuration runtimeconfiguration.Reference,
	at time.Time,
) (Record, error) {
	if proof.Progress.Record == nil || proof.Progress.Record.Progress.Phase != releaseexecution.Complete ||
		proof.Progress.Record.Progress.InFlight {
		return Record{}, errs.New(errs.KindStateConflict, "Release runtime requires completed native execution")
	}
	return acknowledgeServingRelease(input, proof, configuration, at)
}

// AcknowledgePreservedRelease records an actually served candidate after failed
// leave_active cleanup. It consumes the original serving proof and checkpoint;
// it does not manufacture a completed forward cursor or successful Release.
func AcknowledgePreservedRelease(
	input releaseexecution.RecoveryInput,
	preserved releasework.PreservedServing,
	configuration runtimeconfiguration.Reference,
	at time.Time,
) (Record, error) {
	if input.Validate() != nil || !input.PreservesServing() || preserved.Proof == nil ||
		!workexecution.SameAssignment(preserved.Input.Attempt, input.Forward) ||
		preserved.Input.Attempt.StartedAt == nil || input.Forward.StartedAt == nil ||
		!preserved.Input.Attempt.StartedAt.Equal(*input.Forward.StartedAt) {
		return Record{}, errs.New(
			errs.KindStateConflict,
			"preserved runtime requires its original failed attempt and serving proof",
		)
	}
	return acknowledgeServingRelease(preserved.Input, *preserved.Proof, configuration, at)
}

func acknowledgeServingRelease(
	input releaseexecution.Input,
	proof releasework.CompletionProof,
	configuration runtimeconfiguration.Reference,
	at time.Time,
) (Record, error) {
	attempt := input.Attempt
	progress := proof.Progress.Record
	if attempt.Validate() != nil || attempt.Direction != workexecution.Forward || attempt.StartedAt == nil ||
		attempt.FinishedAt != nil || input.Release == nil || progress == nil || progress.Serving == nil ||
		progress.TaskID != attempt.TaskID || progress.WorkItemID != attempt.WorkItemID || progress.Input != attempt.Input ||
		progress.Progress.Validate(input) != nil ||
		proof.Runtime.ServiceID != input.Release.ServiceId || proof.Runtime.ReleaseID != input.Release.ReleaseId ||
		at.IsZero() || at.Location() != time.UTC || at.Before(progress.Progress.UpdatedAt) {
		return Record{}, errs.New(errs.KindStateConflict, "Release runtime requires completed native execution")
	}
	selected := input.Release.Configuration
	if runtimeconfiguration.ValidateReference(configuration) != nil || selected == nil ||
		configuration.ID != selected.ConfigurationId || configuration.EnvironmentID != selected.EnvironmentId ||
		configuration.Generation != selected.Generation || configuration.SHA256 != hex.EncodeToString(selected.Sha256) {
		return Record{}, errs.New(
			errs.KindStateConflict,
			"Release runtime configuration differs from captured execution",
		)
	}
	boundary := progress.Serving.Boundary
	if boundary.ValidateEnvelope(attempt) != nil || boundary.Ack == nil ||
		boundary.Progress.Action != releaseexecution.VerifyServing ||
		releaseexecution.ValidateHelperResult(input, boundary.Progress, proof.Serving) != nil ||
		proof.Evidence.AttemptID != attempt.ID ||
		proof.Evidence.Sequence != boundary.Sequence ||
		proof.Evidence.StageID != boundary.StageID ||
		proof.Evidence.Input != attempt.Input {
		return Record{}, errs.New(errs.KindStateConflict, "Release runtime has no matching final serving proof")
	}
	report := releaseexecution.CheckpointReport{
		Sequence: progress.CheckpointSequence,
		StageID:  progress.StageID,
		Progress: progress.Progress,
	}
	if err := report.ValidateEnvelope(attempt); err != nil {
		return Record{}, err
	}
	checkpoint, err := report.Checkpoint(attempt)
	if err != nil {
		return Record{}, err
	}
	record := Record{EnvironmentID: input.Release.EnvironmentId, Runtime: proof.Runtime, Configuration: &configuration,
		Source: Acknowledgement{TaskID: attempt.TaskID, WorkItemID: attempt.WorkItemID, AttemptID: attempt.ID,
			Executor: attempt.Executor, Input: attempt.Input, Checkpoint: checkpoint,
			RenderGeneration: input.Release.RenderGeneration, AcknowledgedAt: at}}
	return record, Validate(record)
}
