package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredAttachBindingRepository interface {
	SettleBlueprintAttachBinding(context.Context, etcd.TaskRecord, blueprintunits.Unit) error
}

func (service *Service) TrySettleAttachBinding(
	ctx context.Context, parent etcd.TaskRecord, unit blueprintunits.Unit, snapshot blueprintunits.Snapshot,
) (bool, error) {
	if validateAuthoredRuntimeProjectionParent(parent, snapshot) != nil || unit.Removal {
		return false, errs.New(errs.KindStateConflict, "Attach binding Apply authority changed")
	}
	reader, ok := service.repository.(authoredAttachGenerationReader)
	if !ok {
		return false, errs.New(errs.KindInternal, "Attach binding reader is unavailable")
	}
	generation, found, err := reader.GetBlueprintAttachInputGeneration(ctx, parent.ID, unit.Target.ID)
	if err != nil || !found {
		return false, err
	}
	defer attachinputs.Clear(&generation.Record)
	if !generation.Record.SettlesInController() {
		return false, nil
	}
	settler, ok := service.repository.(authoredAttachBindingRepository)
	if !ok {
		return true, errs.New(errs.KindInternal, "Attach binding settlement repository is unavailable")
	}
	return true, settler.SettleBlueprintAttachBinding(ctx, parent, unit)
}
