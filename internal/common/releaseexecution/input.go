// Package releaseexecution owns the typed Service Release execution state.
package releaseexecution

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Input owns decoded delivery bytes. It is not a Start receipt and cannot
// authorize a retry of an interrupted mutation.
type Input struct {
	Attempt   workexecution.Attempt
	Release   *agentpb.ReleaseWorkInput
	Artifacts executionplan.ReleaseWorkArtifacts
}

// WithStart binds the Controller's exact persisted receipt. Reconcile=true
// requires loading saved progress and inspecting effects, not NewProgress.
func (input Input) WithStart(ack *agentpb.WorkStartAck) (Input, bool, error) {
	started, reconcile, err := workexecution.DecodeAgentStartAck(ack, input.Attempt)
	if err != nil {
		return Input{}, false, err
	}
	input.Attempt.StartedAt = &started
	return input, reconcile, nil
}
