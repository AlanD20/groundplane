package blueprintrelease

import (
	"github.com/AlanD20/groundplane/internal/core"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareServiceChanges validates desired replacement while retaining runtime intent.
func PrepareServiceChanges(
	environmentID string,
	desired []core.Service,
	current []etcdstore.Versioned[servicerecord.ServiceRecord],
) ([]blueprints.EnvironmentBlueprintServiceChange, error) {
	currentByID := make(map[string]etcdstore.Versioned[servicerecord.ServiceRecord], len(current))
	for _, service := range current {
		if service.Record.EnvironmentID != environmentID || service.Record.Desired.ID == "" {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Service state is inconsistent")
		}
		if _, duplicate := currentByID[service.Record.Desired.ID]; duplicate {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Service state repeats an id")
		}
		currentByID[service.Record.Desired.ID] = service
	}
	changes := make([]blueprints.EnvironmentBlueprintServiceChange, 0, len(desired))
	for _, next := range desired {
		if existing, found := currentByID[next.ID]; found {
			replacement, err := servicerecord.ReplaceServiceDesired(existing.Record, next)
			if err != nil {
				return nil, err
			}
			currentCopy := existing
			changes = append(changes, blueprints.EnvironmentBlueprintServiceChange{
				Current: &currentCopy,
				Record:  replacement,
			})
			delete(currentByID, next.ID)
			continue
		}
		record, err := servicerecord.NewServiceRecord(environmentID, next, "")
		if err != nil {
			return nil, err
		}
		changes = append(changes, blueprints.EnvironmentBlueprintServiceChange{Record: record})
	}
	if len(currentByID) != 0 {
		return nil, errs.New(
			errs.KindResourceInUse,
			"Blueprint omits an existing Service; remove it explicitly before apply",
		)
	}
	return changes, nil
}
