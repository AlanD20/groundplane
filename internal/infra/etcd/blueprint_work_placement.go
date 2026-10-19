package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Compare captured placements at admission, not when an Agent reconnects. A
// host's stable identity survives process replacement; an Apply cannot silently
// move its work to a different host between planning and publication.
func (repository *EnvironmentBlueprintRepository) blueprintWorkPlacementConditions(ctx context.Context,
	graph blueprintunits.CapturedGraph,
) ([]keyvalue.Condition, error) {
	var selected string
	for _, item := range graph.Items {
		if item.Placement.HostID == "" {
			continue
		}
		if selected != "" && selected != item.Placement.HostID {
			return nil, errs.New(
				errs.KindValidationFailed,
				"Blueprint targets multiple hosts; this installation supports one managed host",
			)
		}
		selected = item.Placement.HostID
	}
	if selected == "" {
		return nil, nil
	}
	host, err := hostbindings.ReadLocal(ctx, repository.store, 0)
	if err != nil {
		return nil, err
	}
	if host.Binding.HostID != selected {
		return nil, errs.New(errs.KindStateConflict, "Blueprint target host changed before publication")
	}
	return host.Conditions, nil
}
