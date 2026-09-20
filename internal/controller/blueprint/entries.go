package blueprint

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/entrygeneration"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
)

func (service *Service) prepareBlueprintEntryValues(
	ctx context.Context,
	generationService *entrygeneration.EntryGenerationService,
	projectID string,
	environmentID string,
	reconciliation taskplanning.BlueprintEntryReconciliation,
	createdAt time.Time,
) error {
	for _, candidate := range reconciliation.Values {
		found, err := service.repository.BlueprintEntryValueGenerationExists(ctx, candidate.Record)
		if err != nil {
			return err
		}
		if !found {
			generation, err := generationService.Generate(
				ctx, projectID, environmentID, candidate.Desired,
				candidate.Record.CurrentValueGenerationID, createdAt,
			)
			if err != nil {
				return err
			}
			createErr := service.repository.CreateBlueprintEntryValueGeneration(ctx, generation)
			entrygeneration.ClearEntryValueGeneration(&generation)
			if createErr != nil {
				return createErr
			}
		}
	}
	for _, record := range reconciliation.Current {
		if record.BlueprintKey == "" {
			continue
		}
		if err := service.repository.BindBlueprintEntryEnvironment(ctx, environmentID, record.Entry.ID); err != nil {
			return err
		}
	}
	return nil
}
