package materialization

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/docker/materializerrunner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (runtime *Runtime) StopComponentFiles(ctx context.Context, input componentexecution.HelperInput, files []entrymaterialization.Header) error {
	if runtime == nil || runtime.helper == nil || ctx == nil || input.Recovery == nil ||
		componentexecution.ValidateRecoveryConfigurationFiles(input, files) != nil {
		return errs.New(errs.KindStateConflict, "Component file cleanup does not match captured recovery")
	}
	if len(files) == 0 {
		return nil
	}
	at, err := ids.Timestamp(ids.KindWorkItem, input.Recovery.Forward.WorkItemID)
	if err != nil {
		return err
	}
	forwardStage := ids.DeriveAt(ids.KindStep, at, input.Recovery.Forward.WorkItemID, "component/"+string(componentexecution.WriteConfiguration))
	for _, file := range files {
		forward := materializerrunner.Ownership{TaskID: input.Recovery.Forward.TaskID, StageID: forwardStage,
			EnvironmentID: input.Procedure.EnvironmentId, Destination: file.Destination(), Generation: input.Procedure.Configuration.Generation}
		if err := runtime.helper.StopCaptured(ctx, forward); err != nil {
			return err
		}
		if err := runtime.helper.StopCaptured(ctx, materializerrunner.OwnershipFromHeader(file)); err != nil {
			return err
		}
	}
	return nil
}
