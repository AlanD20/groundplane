package agent

import (
	"bytes"
	"crypto/sha256"
	componentaction "github.com/AlanD20/groundplane/internal/agent/componentaction"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func managedConfigPublicationProven(step *agentpb.ExecutionStep, result *componentaction.ComponentActionResult) bool {
	if step == nil || result == nil || result.ManagedConfig == nil {
		return false
	}
	action := step.GetComponentApply()
	return action != nil &&
		managedConfigFileStateMatches(result.ManagedConfig.Live, action.GetArtifactDigest()) &&
		managedConfigFileStateMatches(result.ManagedConfig.Previous, action.GetExpectedPreviousArtifactDigest())
}

func managedConfigCommitProven(step *agentpb.ExecutionStep, state componentaction.ManagedConfigTransactionState) bool {
	if step == nil || step.GetComponentApply() == nil {
		return false
	}
	action := step.GetComponentApply()
	return managedConfigFileStateMatches(state.Live, action.GetArtifactDigest()) &&
		managedConfigFileStateMatches(state.Previous, action.GetExpectedPreviousArtifactDigest())
}

func managedConfigRollbackProven(step *agentpb.ExecutionStep, state componentaction.ManagedConfigTransactionState) bool {
	if step == nil || step.GetComponentApply() == nil {
		return false
	}
	expected := step.GetComponentApply().GetExpectedPreviousArtifactDigest()
	return managedConfigFileStateMatches(state.Live, expected) &&
		managedConfigFileStateMatches(state.Previous, expected)
}

func managedConfigFileStateMatches(state componentaction.ManagedConfigFileState, expected []byte) bool {
	if len(expected) == 0 {
		return !state.Present
	}
	return len(expected) == sha256.Size && state.Present && bytes.Equal(state.SHA256[:], expected)
}
