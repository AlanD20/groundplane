package composehelper

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"math"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ExecuteNetworkWork is called under the helper's exclusive host lease. A
// mutation requires the Controller's acknowledged in-flight boundary. Restart
// recovery calls with InspectOnly and never silently repeats a create/remove.
func ExecuteNetworkWork(ctx context.Context, taskRunner runner.Runner, request *agentpb.NetworkWorkHelperRequest) (*agentpb.NetworkWorkHelperResponse, error) {
	input, err := executionplan.OpenNetworkHelperRequest(request)
	if err != nil {
		return nil, err
	}
	if ctx == nil || taskRunner == nil {
		return nil, errs.New(errs.KindInternal, "Network helper is not configured")
	}
	bounded, cancel := context.WithDeadline(ctx, input.Attempt.Deadline)
	if input.InspectOnly {
		cancel()
		bounded, cancel = context.WithTimeout(ctx, 30*time.Second)
	}
	defer cancel()
	observation, dockerID, err := observeNetworkWork(bounded, taskRunner, input)
	if err != nil {
		return nil, err
	}
	if !input.InspectOnly && !observation.Satisfies(input.Progress.Stage) {
		switch input.Progress.Stage {
		case networkexecution.EnsureNetwork:
			remaining := math.Ceil(time.Until(input.Attempt.Deadline).Seconds())
			if remaining <= 0 || remaining > math.MaxUint32 {
				return nil, errs.New(errs.KindStateConflict, "Network execution deadline has elapsed")
			}
			_, err = executeManagedNetworkEnsure(bounded, taskRunner, uint32(remaining), input.Procedure.Candidate,
				&agentpb.ManagedNetworkEnsure{ArtifactId: input.Procedure.Candidate.ArtifactId, NetworkId: input.Procedure.NetworkId})
		case networkexecution.RemoveNetwork:
			// Docker refuses a bridge with attached endpoints. Never force-detach
			// arbitrary containers or resolve the name again after ownership checks.
			_, err = runNetworkWork(bounded, taskRunner, "network", "rm", dockerID)
		default:
			return nil, errs.New(errs.KindValidationFailed, "Network work stage is unsupported")
		}
		if err != nil {
			return nil, err
		}
		observation, _, err = observeNetworkWork(bounded, taskRunner, input)
		if err != nil {
			return nil, err
		}
	}
	digest, err := hex.DecodeString(observation.Input.SHA256)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	response := &agentpb.NetworkWorkHelperResponse{NetworkId: observation.NetworkID,
		Input: &agentpb.WorkInputReference{InputId: observation.Input.ID, Sha256: digest},
		Ready: observation.Ready, Present: observation.Present, Absent: observation.Absent, ObservedAt: timestamppb.New(observation.At), Inspection: input.InspectOnly}
	if _, err := executionplan.ReadNetworkHelperObservation(input, response); err != nil {
		return nil, err
	}
	return response, nil
}

func observeNetworkWork(ctx context.Context, taskRunner runner.Runner, input executionplan.NetworkHelperInput) (networkexecution.Observation, string, error) {
	procedure := input.Procedure
	observation := networkexecution.Observation{NetworkID: procedure.NetworkId, Input: input.Attempt.Input}
	listed, err := runNetworkWork(ctx, taskRunner, "network", "ls", "--filter", "name=^"+procedure.DockerName+"$",
		"--format", "{{.ID}} {{.Name}}", "--no-trunc")
	if err != nil {
		return observation, "", err
	}
	identity := strings.Fields(string(listed.Stdout))
	if len(identity) == 0 {
		observation.Absent, observation.At = true, time.Now().UTC()
		return observation, "", nil
	}
	if len(identity) != 2 || identity[1] != procedure.DockerName || !validOwnedNetworkID(identity[0]) {
		return observation, "", errs.New(errs.KindStateConflict, "Network name resolves to an ambiguous bridge")
	}
	inspected, err := runNetworkWork(ctx, taskRunner, "network", "inspect", "--format", "{{json .}}", identity[0])
	if err != nil {
		return observation, "", err
	}
	var actual inspectedOwnedNetwork
	if json.Unmarshal(inspected.Stdout, &actual) != nil || actual.ID != identity[0] || actual.Name != procedure.DockerName ||
		actual.Labels["com.groundplane.managed"] != "true" || actual.Labels["com.groundplane.kind"] != "network" ||
		actual.Labels["com.groundplane.environment-id"] != procedure.EnvironmentId {
		return observation, "", errs.New(errs.KindStateConflict, "Network ownership differs from captured input")
	}
	if procedure.Action == agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_ENSURE {
		expected, err := selectedEnsuredNetwork(procedure.Candidate,
			&agentpb.ManagedNetworkEnsure{ArtifactId: procedure.Candidate.ArtifactId, NetworkId: procedure.NetworkId})
		if err != nil {
			return observation, "", err
		}
		if !ownedNetworkMatches(actual, expected) {
			return observation, "", errs.New(errs.KindStateConflict, "Existing Zone bridge differs from its captured configuration")
		}
		observation.Ready = true
	}
	observation.Present, observation.At = true, time.Now().UTC()
	return observation, actual.ID, nil
}

func runNetworkWork(ctx context.Context, taskRunner runner.Runner, args ...string) (runner.Result, error) {
	result, err := taskRunner.Run(ctx, runner.RunCmdOpts{Name: DockerExecutable, Args: args, Dir: WorkDirectory,
		Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true, CaptureLimitBytes: maximumComponentConfig})
	if ctx.Err() != nil {
		return runner.Result{}, ctx.Err()
	}
	if err != nil {
		return runner.Result{}, errs.Wrap(errs.KindInternal, err)
	}
	if result.ExitCode != 0 {
		return runner.Result{}, errs.New(errs.KindStateConflict, "Docker could not complete the captured Network operation; attached endpoints must be removed through their owning resources")
	}
	return result, nil
}
