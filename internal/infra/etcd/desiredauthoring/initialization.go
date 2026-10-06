package desiredauthoring

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func Initialize(ctx context.Context, store Store, environmentID string,
	locator idempotencyrecord.IdempotencyLocator, intent idempotencyrecord.ProtectedIntentRecord, now time.Time,
) error {
	head, found, err := blueprints.ReadCurrentDesiredInput(ctx, store, environmentID, 0)
	if err != nil || found {
		return err
	}
	fence, err := environmentfence.LoadOrdinary(ctx, store, environmentID, head.ReadRevision)
	if err != nil {
		return err
	}
	publication, err := Prepare(ctx, store, environmentID,
		idempotencyrecord.IdempotencyMarker{Locator: locator, Intent: intent, CreatedAt: now},
		func(_ *core.BlueprintDesiredInput, _ *projectionrecord.EnvironmentComposeProjection) error {
			return nil
		})
	if err != nil {
		return err
	}
	defer etcdstore.ClearMutationValues(publication.Mutations)
	for _, condition := range publication.Conditions {
		if condition.Key == blueprints.EnvironmentBlueprintHeadKey(environmentID) && condition.ModRevision != 0 {
			return errs.New(errs.KindStateConflict, "Environment desired initialization already completed; retry")
		}
	}
	conditions, mutations, _, err := Bind(publication, fence.TransactionConditions(), nil,
		func(_ int64, _ []*etcdstore.KeyValue) error {
			return errs.New(errs.KindStateConflict, "Environment initialization changed")
		})
	if err != nil {
		return err
	}
	result, err := store.Transact(ctx, conditions, mutations)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Environment desired initialization changed; retry the action")
	}
	return nil
}
