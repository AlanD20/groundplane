package composehelpercontainer

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// QuiesceRemovalRecovery must run under exclusive Agent work/process ownership.
// It stops only the captured failed removal helper and this work/input's stable
// cleanup helper. Docker absence is checked after non-force removal. Inspection
// of accepted Docker workload effects remains the next separate recovery phase.
func (executor *Executor) QuiesceRemovalRecovery(
	ctx context.Context,
	request *agentpb.ServiceRemovalRecoveryHelperRequest,
) (*agentpb.ServiceRemovalRecoveryHelperResponse, error) {
	if executor == nil || executor.engine == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "Service removal recovery helper is not configured")
	}
	input, err := executionplan.OpenRemovalRecoveryHelperRequest(request)
	if err != nil {
		return nil, err
	}
	if input.Progress.Phase != removalexecution.QuiesceExecutors {
		return nil, errs.New(
			errs.KindValidationFailed,
			"Stopping removal executors requires its acknowledged quiescence step",
		)
	}
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	forward := input.Capture.Source.Forward
	forwardLabels := releaseHelperLabels(forward)
	forwardLabels["io.groundplane.role"] = "lifecycle-executor"
	if err := executor.stopNamedHelper(bounded, "gp-lifecycle-executor-"+forward.ID, forwardLabels); err != nil {
		return nil, err
	}
	recoveryLabels := releaseHelperLabels(input.Attempt)
	recoveryLabels["io.groundplane.role"] = "service-removal-recovery"
	if err := executor.stopNamedHelper(bounded, "gp-removal-recovery-"+input.Attempt.WorkItemID, recoveryLabels); err != nil {
		return nil, err
	}
	at := time.Now().UTC()
	raw, err := json.Marshal(struct {
		Attempt string    `json:"attempt"`
		Forward string    `json:"forward"`
		Work    string    `json:"work"`
		Stage   string    `json:"stage"`
		At      time.Time `json:"at"`
	}{input.Attempt.ID, forward.ID, input.Attempt.WorkItemID, input.Checkpoint.StageID, at})
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(raw)
	response := &agentpb.ServiceRemovalRecoveryHelperResponse{StageId: input.Checkpoint.StageID,
		Evidence:   &agentpb.WorkInputReference{InputId: input.Attempt.Input.ID, Sha256: digest[:]},
		ObservedAt: timestamppb.New(at), Satisfied: true, Inspection: input.InspectOnly}
	if _, err := executionplan.ReadRemovalRecoveryHelperReceipt(input, response); err != nil {
		return nil, err
	}
	return response, nil
}
