package composehelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TASK-31/74: the actual native request reader must reject concatenated or
// trailing instructions, not just decode the first permitted frame.
func TestNativeNetworkRequestRejectsTrailingBytes(t *testing.T) {
	request := nativeNetworkRemovalRequest(t)
	framed, err := MarshalNetworkWorkRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := ReadNetworkWorkRequest(t.Context(), bytes.NewReader(framed))
	if err != nil || !proto.Equal(decoded, request) {
		t.Fatalf("native request roundtrip: %v", err)
	}
	for _, extra := range [][]byte{{0}, framed} {
		trailing := append(append([]byte(nil), framed...), extra...)
		if _, err := ReadNetworkWorkRequest(t.Context(), bytes.NewReader(trailing)); !errors.Is(
			err,
			errs.New(errs.KindValidationFailed, ""),
		) {
			t.Fatalf("accepted trailing helper instructions: %v", err)
		}
	}
}

// NET-04 / TASK-74: removal disconnects captured singleton/proxy/slot endpoints,
// never deploys a workload, and lost-ack inspection observes absence without
// replaying effects. The Runner models Docker state, not a real network namespace.
func TestNativeNetworkRemovalDisconnectsCapturedRuntimeAndInspectsLostAck(t *testing.T) {
	for _, names := range [][]string{nil, {"singleton"}, {"proxy", "blue", "green"}} {
		t.Run(strings.Join(names, "+"), func(t *testing.T) {
			request := nativeNetworkRemovalRequest(t)
			host := newNativeRemovalNetworkHost(t, request, names)
			response, err := ExecuteNetworkWork(t.Context(), host.runner, request)
			if err != nil || response == nil || !response.Absent || response.Present || host.present ||
				!slices.Equal(host.disconnected, host.containers) || host.removals != 1 {
				t.Fatalf("captured bridge cleanup = %+v, %v; disconnected %v", response, err, host.disconnected)
			}
			request.InspectOnly = true
			response, err = ExecuteNetworkWork(t.Context(), host.runner, request)
			if err != nil || response == nil || !response.Absent || !response.Inspection ||
				host.removals != 1 || len(host.disconnected) != len(names) {
				t.Fatalf("lost-ack inspection repeated effects or lost absence: %+v, %v", response, err)
			}
		})
	}
}

// NET-04 / TASK-74: validate every endpoint before disconnecting any, even when
// the foreign endpoint sorts after a valid consumer. A stale grant reaches no I/O.
func TestNativeNetworkRemovalRejectsForeignOwnershipBeforeEffects(t *testing.T) {
	for _, changed := range []string{"network", "endpoint-environment", "endpoint-service", "acknowledgement"} {
		t.Run(changed, func(t *testing.T) {
			request := nativeNetworkRemovalRequest(t)
			host := newNativeRemovalNetworkHost(t, request, []string{"blue", "green"})
			switch changed {
			case "network":
				host.environmentID = ids.New(ids.KindEnvironment)
			case "endpoint-environment":
				host.foreignEnvironment = true
			case "endpoint-service":
				host.foreignService = true
			case "acknowledgement":
				request.Acknowledgement.Sequence++
			}
			response, err := ExecuteNetworkWork(t.Context(), host.runner, request)
			if err == nil || response != nil || !host.present || host.removals != 0 || len(host.disconnected) != 0 {
				t.Fatalf("changed ownership mutated host: %+v, %v", response, err)
			}
			if changed == "acknowledgement" && len(host.runner.RecordedCalls()) != 0 {
				t.Fatal("unacknowledged effect reached Docker")
			}
		})
	}
}

func nativeNetworkRemovalRequest(t *testing.T) *agentpb.NetworkWorkHelperRequest {
	t.Helper()
	digest := sha256.Sum256([]byte("captured-zone-removal"))
	networkID := ids.New(ids.KindNetwork)
	input := &agentpb.NetworkWorkInput{EnvironmentId: ids.New(ids.KindEnvironment), NetworkId: networkID,
		DockerName: "gp_net_" + networkID, IntentSha256: digest[:], RemovalSubnet: "10.70.0.0/24",
		Action:               agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_REMOVE,
		DisconnectServiceIds: []string{ids.New(ids.KindService)}}
	_, raw, reference, err := executionplan.CaptureNetworkWorkInput(ids.New(ids.KindPlan), input)
	clear(raw)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Add(-time.Second)
	attempt := workexecution.Attempt{ID: ids.New(ids.KindAttempt), TaskID: ids.New(ids.KindTask),
		WorkItemID: ids.New(ids.KindWorkItem), Number: 1, Direction: workexecution.Forward,
		Input: reference, AssignedAt: at, StartedAt: &at, Deadline: at.Add(time.Minute),
		Executor: workexecution.Identity{Executor: workexecution.Agent, HostID: ids.New(ids.KindHost),
			AgentID: ids.New(ids.KindAgent), Generation: 1}}
	progress, err := networkexecution.NewProgress(attempt, input.Action, networkID, at)
	if err != nil {
		t.Fatal(err)
	}
	progress, err = progress.Begin(attempt, input.Action, networkID, at)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := progress.Checkpoint(attempt, input.Action, networkID)
	if err != nil {
		t.Fatal(err)
	}
	report, err := networkexecution.EncodeProgress(attempt, input.Action, networkID, progress)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := hex.DecodeString(checkpoint.Evidence.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	return &agentpb.NetworkWorkHelperRequest{
		Assignment: &agentpb.NetworkWorkAssignment{Attempt: wire, Input: input},
		Start:      &agentpb.WorkStartAck{Attempt: proto.CloneOf(wire), StartedAt: timestamppb.New(at)},
		Report:     &agentpb.NetworkWorkCheckpointRequest{Attempt: proto.CloneOf(wire), Progress: report},
		Acknowledgement: &agentpb.NetworkWorkCheckpointAck{Attempt: proto.CloneOf(wire), Sequence: checkpoint.Sequence,
			StageId: checkpoint.StageID, RecordedAt: timestamppb.New(checkpoint.RecordedAt),
			Evidence: &agentpb.WorkInputReference{InputId: reference.ID, Sha256: evidence}},
	}
}

type nativeRemovalNetworkHost struct {
	runner                                      *runner.FakeRunner
	environmentID, name, serviceID              string
	containers, disconnected                    []string
	present, foreignEnvironment, foreignService bool
	removals                                    int
}

func newNativeRemovalNetworkHost(
	t *testing.T,
	request *agentpb.NetworkWorkHelperRequest,
	names []string,
) *nativeRemovalNetworkHost {
	t.Helper()
	input := request.Assignment.Input
	host := &nativeRemovalNetworkHost{runner: runner.NewFake(), environmentID: input.EnvironmentId,
		name: input.DockerName, serviceID: input.DisconnectServiceIds[0], present: true}
	for index := range names {
		host.containers = append(host.containers, strings.Repeat(string(rune('a'+index)), 64))
	}
	host.runner.RunFunc = func(_ context.Context, command runner.RunCmdOpts) (runner.Result, error) {
		args := command.Args
		if command.Name != DockerExecutable || len(args) < 2 {
			t.Fatalf("unexpected command: %+v", command)
		}
		networkID := strings.Repeat("d", 64)
		var body []byte
		switch {
		case args[0] == "network" && args[1] == "ls":
			if host.present {
				body = []byte(networkID + " " + host.name)
			}
		case args[0] == "network" && args[1] == "inspect" && args[3] == "{{json .}}":
			actual := inspectedOwnedNetwork{ID: networkID, Name: host.name, Labels: map[string]string{
				"com.groundplane.managed": "true", "com.groundplane.kind": "network",
				"com.groundplane.environment-id": host.environmentID}}
			var err error
			body, err = json.Marshal(actual)
			if err != nil {
				t.Fatal(err)
			}
		case args[0] == "network" && args[1] == "inspect":
			body = []byte(strings.Join(host.containers, " "))
		case args[0] == "container" && args[1] == "inspect":
			environmentID, serviceID := input.EnvironmentId, host.serviceID
			if args[len(args)-1] == host.containers[len(host.containers)-1] {
				if host.foreignEnvironment {
					environmentID = ids.New(ids.KindEnvironment)
				}
				if host.foreignService {
					serviceID = ids.New(ids.KindService)
				}
			}
			body = []byte(`{"Id":"` + args[len(args)-1] + `","Config":{"Labels":{` +
				`"com.groundplane.managed":"true","com.groundplane.environment-id":"` + environmentID +
				`","com.groundplane.service-id":"` + serviceID + `"}}}`)
		case args[0] == "network" && args[1] == "disconnect":
			if args[2] != networkID {
				t.Fatal("disconnect resolved a mutable name instead of inspected Docker ID")
			}
			host.disconnected = append(host.disconnected, args[3])
		case args[0] == "network" && args[1] == "rm":
			if args[2] != networkID {
				t.Fatal("removal resolved a mutable name instead of inspected Docker ID")
			}
			host.present = false
			host.removals++
		default:
			t.Fatalf("Zone removal attempted a workload mutation: %v", args)
		}
		return runner.Result{Stdout: body}, nil
	}
	return host
}
