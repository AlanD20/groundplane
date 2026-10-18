package serviceremoval

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type InputRecords interface {
	PrepareBlueprintServiceRemoval(context.Context, string, string) error
}

type InputPreparation struct{ records InputRecords }

func NewInputPreparation(records InputRecords) (*InputPreparation, error) {
	if records == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Service removal requires captured input storage")
	}
	return &InputPreparation{records: records}, nil
}

func (owner *InputPreparation) Prepare(ctx context.Context, taskID, workID string) error {
	return owner.records.PrepareBlueprintServiceRemoval(ctx, taskID, workID)
}
