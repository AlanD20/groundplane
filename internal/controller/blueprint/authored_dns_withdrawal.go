package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredDNSReferenceRepository interface {
	ReadEnvironmentBlueprintDNSBindings(context.Context, string, int64) ([]dnsrecords.EnvironmentBinding, error)
}

// Removal depends on the DNS unit that withdraws its exact acknowledged target,
// including a record whose resolver Task completed before its Apply receipt.
func (service *Service) orderAuthoredDNSWithdrawal(ctx context.Context,
	units, omissions []blueprintunits.Unit, snapshot blueprintunits.Snapshot,
) error {
	targets := make(map[string]blueprintunits.ResourceKey)
	for _, candidates := range [][]blueprintunits.Unit{units, omissions} {
		for _, candidate := range candidates {
			if candidate.Target.Kind == ids.KindDNSRecord {
				targets[candidate.Target.ID] = candidate.Target
			}
		}
	}
	if len(targets) == 0 {
		return nil
	}
	reader, ok := service.repository.(authoredDNSReferenceRepository)
	if !ok {
		return errs.New(errs.KindInternal, "Blueprint DNS reference repository is not configured")
	}
	bindings, err := reader.ReadEnvironmentBlueprintDNSBindings(ctx, snapshot.EnvironmentID, snapshot.ReadRevision)
	if err != nil {
		return err
	}
	for _, binding := range bindings {
		if binding.Address == "" {
			continue
		}
		dependency, found := targets[binding.Record.ID]
		if !found {
			return errs.New(errs.KindStateConflict, "Blueprint has no withdrawal unit for an acknowledged DNS record")
		}
		for index := range omissions {
			target := omissions[index].Target
			if target.Kind == ids.KindService && target.ID == binding.ServiceID ||
				target.Kind == ids.KindNetwork && target.ID == binding.Record.ZoneID {
				omissions[index].After = append(omissions[index].After, dependency)
				omissions[index].After = canonicalAuthoredUnitKeys(omissions[index].After)
			}
		}
	}
	return nil
}
