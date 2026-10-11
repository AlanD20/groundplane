package executionplan

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// An unchanged predecessor file grants only observation authority. Bind it to
// the exact Environment and retained snapshot and forbid every writer to its
// path, rather than fabricating an executable restoration step.
func validateUnchangedComponentConfiguration(plan *agentpb.ExecutionPlan,
	procedure *agentpb.ComponentRecoveryProcedure, file *agentpb.ComponentUnchangedConfigurationFile,
) error {
	artifacts := make(map[string]*agentpb.ComposeArtifact, len(plan.GetArtifacts()))
	for _, artifact := range plan.GetArtifacts() {
		artifacts[artifact.GetArtifactId()] = artifact
	}
	environmentID, err := componentRecoveryEnvironment(plan, artifacts)
	if err != nil || file.GetEnvironmentId() != environmentID ||
		ids.Validate(ids.KindConfig, file.GetMaterializationId()) != nil ||
		entrymaterialization.ValidateDesiredDestination(file.GetDestination()) != nil ||
		ids.Validate(ids.KindConfig, file.GetPriorSnapshotId()) != nil ||
		!validComponentDigest(file.GetPriorSnapshotSha256()) ||
		file.GetPriorSnapshotGeneration() == 0 || file.GetPriorSnapshotGeneration() > plan.GetRenderGeneration() {
		return invalidComponentRecovery("unchanged configuration scope or snapshot is invalid")
	}
	configuration := procedure.GetConfigurationRestoration()
	if configuration != nil && !ConfigurationSnapshotMatches(configuration,
		file.GetPriorSnapshotId(), file.GetPriorSnapshotSha256()) {
		return invalidComponentRecovery("unchanged configuration differs from the restoration snapshot")
	}
	for _, step := range plan.GetSteps() {
		materialization := step.GetMaterializeFile()
		if materialization != nil && (materialization.GetMaterializationId() == file.GetMaterializationId() ||
			materialization.GetEnvironmentId() == environmentID && materialization.GetDestination() == file.GetDestination()) {
			return invalidComponentRecovery("unchanged configuration is also an executable file source")
		}
	}
	digest := sha256.Sum256(procedure.GetRetainedConfiguration().GetSource())
	if !bytes.Equal(digest[:], file.GetSha256()) {
		return invalidComponentRecovery("unchanged configuration digest differs from its captured bytes")
	}
	return nil
}
