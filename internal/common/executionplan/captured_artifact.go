package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ValidateCapturedArtifact checks the shared Compose artifact contract against
// its captured label context. The local context below is not an executable plan,
// persisted input or an effect grant; operation owners supply all authorization.
func ValidateCapturedArtifact(planID string, generation uint64, artifact *agentpb.ComposeArtifact) error {
	if ids.Validate(ids.KindPlan, planID) != nil || generation == 0 || artifact == nil ||
		proto.Size(artifact) > MaximumPlanBytes || RejectUnknown(artifact) != nil {
		return errs.New(errs.KindValidationFailed, "captured Compose artifact context is invalid")
	}
	return validateArtifact(&agentpb.ExecutionPlan{PlanId: planID, RenderGeneration: generation}, artifact)
}

// ValidateCapturedMaterializations checks the complete bounded ordered file
// manifest without carrying or authorizing its private content.
func ValidateCapturedMaterializations(
	generation uint64,
	artifact *agentpb.ComposeArtifact,
	files []*agentpb.MaterializeFile,
) error {
	if artifact == nil || generation == 0 || len(files) > 512 {
		return errs.New(errs.KindValidationFailed, "captured file manifest is missing or oversized")
	}
	previous := ""
	artifacts := map[string]*agentpb.ComposeArtifact{artifact.ArtifactId: artifact}
	for _, file := range files {
		if file == nil || file.Destination <= previous || RejectUnknown(file) != nil {
			return errs.New(errs.KindValidationFailed, "captured file manifest is not uniquely ordered")
		}
		if err := validateMaterializeFile(generation, file, artifacts); err != nil {
			return err
		}
		previous = file.Destination
	}
	return nil
}
