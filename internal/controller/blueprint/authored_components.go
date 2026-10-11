package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) authoredComponentIdentities(
	ctx context.Context, environmentID string, specs map[string]core.ComponentSpec,
	previous []projectionrecord.OwnedIdentity,
) ([]composeidentity.Resource, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	records, err := service.listBlueprintComponents(ctx, environmentID)
	if err != nil {
		return nil, err
	}
	current := make([]core.Component, len(records))
	for index, record := range records {
		if record.Record.Desired.OwnerID != environmentID {
			return nil, errs.New(errs.KindStateConflict, "Blueprint Component belongs to another Environment")
		}
		current[index], err = componentrecord.ProjectRecord(record.Record)
		if err != nil {
			return nil, err
		}
	}
	selected, err := taskplanning.SelectBlueprintComponentSingletons(specs, current)
	if err != nil {
		return nil, err
	}
	byCapability := make(map[core.ComponentCapability]string, len(selected))
	for _, component := range selected {
		capability, _ := component.Kind.EnvironmentCapability()
		byCapability[capability] = component.ID
	}
	result := make([]composeidentity.Resource, 0, len(specs))
	for _, capability := range sortedAuthoredMapKeys(specs) {
		id := byCapability[core.ComponentCapability(capability)]
		for _, prior := range previous {
			if prior.Name == capability && prior.ID != id {
				return nil, errs.New(
					errs.KindStateConflict,
					"Blueprint Component identity differs from its Environment singleton",
				)
			}
		}
		result = append(result, composeidentity.Resource{ID: id, Name: capability})
	}
	return result, nil
}
