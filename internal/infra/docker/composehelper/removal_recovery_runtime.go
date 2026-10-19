package composehelper

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ExecuteRemovalRuntime restores only captured members, workloads before their
// proxy. Quiescence and configuration are separate acknowledged recovery phases.
func ExecuteRemovalRuntime(
	ctx context.Context,
	taskRunner runner.Runner,
	root string,
	request *agentpb.ServiceRemovalRecoveryHelperRequest,
) (*agentpb.ServiceRemovalRecoveryHelperResponse, error) {
	if ctx == nil || taskRunner == nil {
		return nil, invalidReleaseHelper()
	}
	input, err := executionplan.OpenRemovalRecoveryHelperRequest(request)
	if err != nil {
		return nil, err
	}
	phase := input.Progress.Phase
	if phase != removalexecution.InspectRuntime && phase != removalexecution.RestoreRuntime &&
		phase != removalexecution.VerifyRestoration {
		return nil, errs.New(
			errs.KindValidationFailed,
			"Service removal runtime helper received a non-runtime recovery phase",
		)
	}
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	if !input.InspectOnly {
		cancel()
		bounded, cancel = context.WithDeadline(ctx, input.Attempt.Deadline)
	}
	defer cancel()
	members := input.Procedure.Members
	observed := make([][]string, len(members))
	// Inspect all names before any write: foreign ownership never gets replaced.
	for index, member := range members {
		scope, err := environmentpath.Parse(root, member.Artifact.AuthorizedVolumeDir)
		if err != nil || scope.EnvironmentID != input.Procedure.EnvironmentId {
			return nil, invalidReleaseHelper()
		}
		observed[index], err = inspectCapturedContainers(
			bounded,
			taskRunner,
			member.Artifact,
			member.Artifact.Services[0],
		)
		if err != nil {
			return nil, err
		}
	}
	if phase == removalexecution.RestoreRuntime && !input.InspectOnly {
		for _, proxyPass := range []bool{false, true} {
			for _, member := range members {
				service := member.Artifact.Services[0]
				if (service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY) != proxyPass {
					continue
				}
				if err := ensureRecoveryService(bounded, taskRunner, member.Artifact, service, false); err != nil {
					return nil, err
				}
			}
		}
	}
	satisfied := true
	states := make([]*agentpb.ServiceLifecycleHelperResponse, 0, len(members))
	if phase != removalexecution.InspectRuntime {
		for _, member := range members {
			service := member.Artifact.Services[0]
			state, err := inspectLifecycleMember(bounded, taskRunner, executionplan.LifecycleHelperInput{
				Attempt: input.Attempt, Procedure: input.Procedure, InspectOnly: true,
				Stage: lifecycleexecution.Stage{
					ID:       member.Ownership.StepId,
					Member:   service.ComposeName,
					Expected: lifecycleexecution.Running,
				},
			})
			if err != nil {
				return nil, err
			}
			satisfied = satisfied && state.Satisfied
			if service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY && state.Satisfied {
				ids, err := inspectCapturedContainers(bounded, taskRunner, member.Artifact, service)
				if err != nil {
					return nil, err
				}
				if len(ids) != 1 {
					satisfied = false
				} else {
					proven, err := restorationProxyProven(bounded, taskRunner, ids[0], service, true)
					if err != nil {
						return nil, err
					}
					satisfied = satisfied && proven
				}
			}
			states = append(states, state)
		}
	}
	at := time.Now().UTC()
	response := &agentpb.ServiceRemovalRecoveryHelperResponse{StageId: input.Checkpoint.StageID,
		ObservedAt: timestamppb.New(at), Inspection: input.InspectOnly, Satisfied: satisfied}
	if satisfied {
		raw, err := json.Marshal(struct {
			Attempt    string                                    `json:"attempt"`
			Stage      string                                    `json:"stage"`
			Containers [][]string                                `json:"containers"`
			States     []*agentpb.ServiceLifecycleHelperResponse `json:"states"`
			At         time.Time                                 `json:"at"`
		}{input.Attempt.ID, input.Checkpoint.StageID, observed, states, at})
		if err != nil {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
		digest := sha256.Sum256(raw)
		response.Evidence = &agentpb.WorkInputReference{InputId: input.Attempt.Input.ID, Sha256: digest[:]}
	}
	if _, err := executionplan.ReadRemovalRecoveryHelperReceipt(input, response); err != nil {
		return nil, err
	}
	return response, nil
}
