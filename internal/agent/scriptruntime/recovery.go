package scriptruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RecoverStandalone is cleanup only. Its caller holds the recovery execution
// lease and supplies the Controller's acknowledged Start, never a forward grant.
func (runtime *Runtime) RecoverStandalone(
	ctx context.Context,
	input scriptexecution.RecoveryInput,
	ownership *agentownership.Lease,
) (scriptexecution.RecoveryProof, error) {
	if runtime == nil || runtime.engine == nil || ctx == nil || input.Validate() != nil ||
		input.Attempt.StartedAt == nil {
		return scriptexecution.RecoveryProof{}, errs.New(
			errs.KindStateConflict,
			"Script cleanup requires a started captured recovery attempt",
		)
	}
	if err := ownership.CheckRecovery(input.Forward, input.Attempt); err != nil {
		return scriptexecution.RecoveryProof{}, err
	}
	bounded, cancel := context.WithDeadline(ctx, input.Attempt.Deadline)
	defer cancel()
	if err := bounded.Err(); err != nil {
		return scriptexecution.RecoveryProof{}, err
	}
	request := scriptexecution.QuiescenceRequest{TaskID: input.Forward.TaskID, WorkItemID: input.Forward.WorkItemID,
		OperationID: input.OperationID, ExecutionID: input.ExecutionID, Projection: input.Projection}
	quiescence, err := runtime.engine.Quiesce(bounded, request)
	if err != nil {
		return scriptexecution.RecoveryProof{}, err
	}
	if err := ownership.CheckRecovery(input.Forward, input.Attempt); err != nil {
		return scriptexecution.RecoveryProof{}, err
	}
	if err := bounded.Err(); err != nil {
		return scriptexecution.RecoveryProof{}, err
	}
	var body *scriptexecution.BodyEvidence
	if receipt := input.Artifacts.BodyPrepared; receipt != nil {
		evidence := bodyEvidenceFromCheckpoint(receipt)
		body = &evidence
	}
	var container *scriptexecution.ContainerEvidence
	if receipt := input.Artifacts.ContainerCreated; receipt != nil {
		evidence := containerEvidenceFromCheckpoint(receipt)
		container = &evidence
	}
	cleanup, err := runtime.engine.Cleanup(scriptexecution.CleanupRequest{QuiescenceRequest: request,
		BodyMetadata: input.Artifacts.BodyMetadata}, body, container)
	if err != nil {
		return scriptexecution.RecoveryProof{}, err
	}
	if err := ownership.CheckRecovery(input.Forward, input.Attempt); err != nil {
		return scriptexecution.RecoveryProof{}, err
	}
	if !quiescence.Absent || !cleanup.ContainerAbsent || !cleanup.BodyAbsent || !cleanup.ExecutionDirectoryAbsent {
		return scriptexecution.RecoveryProof{}, errs.New(
			errs.KindStateConflict,
			"Script recovery has not proven complete cleanup",
		)
	}
	proof := scriptexecution.RecoveryProof{AttemptID: input.Attempt.ID, ForwardAttemptID: input.Forward.ID,
		ExecutionID: input.ExecutionID, Input: input.Attempt.Input,
		Quiescence: quiescence, Cleanup: cleanup, ObservedAt: time.Now().UTC()}
	return proof, proof.Validate(input)
}
