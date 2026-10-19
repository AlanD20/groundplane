package composehelper

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func ExecuteRecovery(
	ctx context.Context,
	taskRunner runner.Runner,
	request *agentpb.ReleaseRecoveryHelperRequest,
) (*agentpb.ReleaseRecoveryHelperResponse, error) {
	input, report, err := OpenRecoveryRequest(request)
	if err != nil {
		return nil, err
	}
	proof := releaseexecution.RecoveryRuntimeProof{AttemptID: input.Input.Attempt.ID, StageID: report.StageID,
		Phase: report.Progress.Phase, Inspection: request.InspectOnly}
	if input.PreservesServing() && report.Progress.Phase != releaseexecution.InspectFailedRuntime {
		err = inspectPreservedRelease(ctx, taskRunner, input)
		if err != nil {
			return nil, err
		}
		proof.ObservedAt = time.Now().UTC()
		return releaseexecution.EncodeRecoveryRuntimeProof(input, report, proof)
	}
	switch report.Progress.Phase {
	case releaseexecution.InspectFailedRuntime:
		inventory, inspectErr := InspectRecoveryRuntime(ctx, taskRunner, input, report)
		err = inspectErr
		proof.Inventory = &inventory
	case releaseexecution.RestoreServingRuntime:
		err = RestoreRecoveryServing(ctx, taskRunner, input, report, request.InspectOnly)
	case releaseexecution.RemoveFailedCandidate:
		err = RemoveRecoveryCandidate(ctx, taskRunner, input, report, request.InspectOnly)
	case releaseexecution.VerifyRestoration:
		err = VerifyRecoveryRuntime(ctx, taskRunner, input, report)
	default:
		return nil, invalidReleaseHelper()
	}
	if err != nil {
		return nil, err
	}
	proof.ObservedAt = time.Now().UTC()
	return releaseexecution.EncodeRecoveryRuntimeProof(input, report, proof)
}
