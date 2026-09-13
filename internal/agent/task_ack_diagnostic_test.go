package agent

import (
	"bytes"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: Component configuration and activation failures are closed Task
// failures, not unknown protocol values. The Agent must preserve their exact
// enum numbers through TaskAck construction and protobuf serialization.
func TestSendTaskAckPreservesComponentFailureDiagnostics(t *testing.T) {
	t.Parallel()

	for _, diagnostic := range []agentpb.ComposeHelperDiagnostic{
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_CONFIG_REJECTED,
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
	} {
		diagnostic := diagnostic
		t.Run(diagnostic.String(), func(t *testing.T) {
			t.Parallel()
			stream := newFakeStream()
			client := &Client{}
			result := TaskResult{
				AssignmentID:   workerTestAssignmentID,
				TaskID:         workerTestTaskID,
				PlanHash:       PlanHash{1},
				Terminal:       TaskTerminalFailed,
				ExitCode:       17,
				ExecutionEpoch: 3,
				Compose: &agentpb.ComposeTaskResult{
					FailedStepId: "step-component",
					Diagnostic:   diagnostic,
				},
			}
			if err := client.sendTaskAck(stream, result); err != nil {
				t.Fatalf("sendTaskAck() error = %v", err)
			}
			acknowledgements := stream.taskAcknowledgements()
			if len(acknowledgements) != 1 {
				t.Fatalf("TaskAck count = %d, want 1", len(acknowledgements))
			}
			wire, err := proto.Marshal(acknowledgements[0])
			if err != nil {
				t.Fatalf("Marshal(TaskAck) error = %v", err)
			}
			decoded := &agentpb.TaskAck{}
			if err := proto.Unmarshal(wire, decoded); err != nil {
				t.Fatalf("Unmarshal(TaskAck) error = %v", err)
			}
			if decoded.GetComposeResult().GetDiagnostic() != diagnostic ||
				!bytes.Equal(decoded.GetPlanHash(), result.PlanHash[:]) {
				t.Fatalf("decoded TaskAck = %#v", decoded)
			}
		})
	}
}
