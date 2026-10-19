package agent

import (
	"context"
	"sync"

	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type orderedReleaseRuntime struct {
	mu         sync.Mutex
	events     []string
	responses  map[string]*agentpb.ComposeHelperResponse
	scriptExit map[string]int32
	scriptErr  map[string]error
}

func (runtime *orderedReleaseRuntime) Execute(
	_ context.Context,
	request *agentpb.ComposeHelperRequest,
) (*agentpb.ComposeHelperResponse, error) {
	runtime.record("compose:" + request.StepId)
	return runtime.responses[request.StepId], nil
}

func (runtime *orderedReleaseRuntime) ExecuteScript(
	_ context.Context,
	_ testtaskassignment.Assignment,
	step *agentpb.ExecutionStep,
	_ func(context.Context, *agentpb.ScriptCheckpointRequest) error,
) (int32, error) {
	runtime.record("script:" + step.StepId)
	return runtime.scriptExit[step.StepId], runtime.scriptErr[step.StepId]
}

func (runtime *orderedReleaseRuntime) CompleteScriptWithoutStart(
	_ context.Context,
	_ testtaskassignment.Assignment,
	step *agentpb.ExecutionStep,
	reason agentpb.ScriptOutcomeReason,
	_ func(context.Context, *agentpb.ScriptCheckpointRequest) error,
) error {
	runtime.record("script-without-start:" + step.StepId + ":" + reason.String())
	return nil
}

func (runtime *orderedReleaseRuntime) record(event string) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.events = append(runtime.events, event)
}

func releaseHookStep(stepID string, policy agentpb.ExecutionStepPolicy) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{
		StepId: stepID, TimeoutSeconds: 5, Policy: policy,
		Payload: &agentpb.ExecutionStep_RunScript{RunScript: &agentpb.RunScript{
			ScriptExecutionId: stepID, ServiceId: "api", ReleaseId: "release-api",
		}},
	}
}
