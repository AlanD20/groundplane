package componentplanning

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Plain Component files have no Service identity. Their captured source, not an
// invented Service reference, grants ownership within the one-Component recipe.
func (record ChildExecution) validateMaterializations(plan *agentpb.ExecutionPlan) error {
	files := make(map[string]taskmaterialization.Record, len(record.Materializations))
	previous := ""
	for _, file := range record.Materializations {
		if file.StepID <= previous || file.EnvironmentID != record.EnvironmentID ||
			taskmaterialization.ValidateRecord(file, plan.GetRenderGeneration()) != nil {
			return errs.New(errs.KindStateConflict, "Blueprint Component file metadata is invalid")
		}
		previous = file.StepID
		switch file.Source.Kind {
		case taskmaterialization.SourceComponentFile:
			source := file.Source.ComponentFile
			if source.ComponentID != record.Component.Desired.ID || source.RevisionID != record.ParentTaskID ||
				source.Path != file.Destination || source.RouteTaskID != "" {
				return errs.New(errs.KindStateConflict, "Blueprint Component file belongs to another input")
			}
		case taskmaterialization.SourceGeneratedEnvironment:
			if file.ServiceID == "" {
				return errs.New(errs.KindStateConflict, "Blueprint Component environment file has no Service")
			}
		default:
			return errs.New(errs.KindStateConflict, "Blueprint Component file has an unrelated source")
		}
		files[file.StepID] = file
	}
	for _, step := range plan.GetSteps() {
		wire := step.GetMaterializeFile()
		if wire == nil || step.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD {
			continue
		}
		file, found := files[step.GetStepId()]
		if !found || file.MaterializationID != wire.GetMaterializationId() || file.EnvironmentID != wire.GetEnvironmentId() ||
			file.Destination != wire.GetDestination() || file.ServiceID != wire.GetServiceId() || file.ServiceName != wire.GetServiceName() ||
			file.UID != wire.GetUid() || file.GID != wire.GetGid() || file.Mode != wire.GetMode() ||
			file.Length != wire.GetLength() || file.SHA256 != hex.EncodeToString(wire.GetSha256()) ||
			!componentOutputMatches(file.OutputKind, wire.GetOutputKind()) {
			return errs.New(errs.KindStateConflict, "Blueprint Component file step differs from its captured source")
		}
		delete(files, step.GetStepId())
	}
	if len(files) != 0 {
		return errs.New(errs.KindStateConflict, "Blueprint Component source has no file step")
	}
	return nil
}

func componentOutputMatches(output taskmaterialization.OutputKind, wire agentpb.MaterializationOutputKind) bool {
	return output == taskmaterialization.OutputPlainFile && wire == agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE ||
		output == taskmaterialization.OutputGeneratedEnvironment && wire == agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_GENERATED_ENV
}
