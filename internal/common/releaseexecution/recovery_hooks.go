package releaseexecution

import (
	"sort"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (input RecoveryInput) failureHooks() []*agentpb.ReleaseWorkHook {
	if input.Group != nil || input.Forward.Outcome != workexecution.Failed || input.Input.Release == nil {
		return nil
	}
	var hooks []*agentpb.ReleaseWorkHook
	for _, hook := range input.Input.Release.Hooks {
		if hook != nil && hook.Phase == "on-failure" {
			hooks = append(hooks, hook)
		}
	}
	sort.SliceStable(hooks, func(i, j int) bool { return hooks[i].Order < hooks[j].Order })
	return hooks
}

// RecoveryActionCount bounds retained proofs independently of diagnostic history.
func (input RecoveryInput) RecoveryActionCount() int { return 6 + len(input.failureHooks()) }

func (input RecoveryInput) HookServingReleaseID() string {
	if input.PreservesServing() {
		return input.Input.Release.ReleaseId
	}
	if input.Input.Release.Predecessor.Absent {
		return ""
	}
	return input.Input.Release.Predecessor.ReleaseId
}

func (progress RecoveryProgress) ActiveHook(input RecoveryInput) (*agentpb.ReleaseWorkHook, error) {
	if progress.Validate(input) != nil || !progress.InFlight || progress.Phase != FailureHooks {
		return nil, invalidProgress()
	}
	return proto.CloneOf(input.failureHooks()[progress.Hook]), nil
}

// FailureHookProof references the Script owner's acknowledged terminal outcome
// and cleanup, never its output. Durable publication rechecks that owner record.
type FailureHookProof struct {
	ExecutionID        string               `json:"execution_id"`
	Checkpoint         *workexecution.Input `json:"checkpoint,omitempty"`
	InterruptedCleanup *workexecution.Input `json:"interrupted_cleanup,omitempty"`
}

func (proof FailureHookProof) Validate(input RecoveryInput, boundary RecoveryReport) error {
	hook, err := boundary.Progress.ActiveHook(input)
	if err != nil || boundary.Ack == nil || proof.ExecutionID != hook.ExecutionId ||
		(proof.Checkpoint == nil) == (proof.InterruptedCleanup == nil) {
		return invalidProgress()
	}
	if proof.Checkpoint != nil && proof.Checkpoint.Validate() != nil ||
		proof.InterruptedCleanup != nil && proof.InterruptedCleanup.Validate() != nil {
		return invalidProgress()
	}
	return nil
}
