package desiredrevision

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// A terminal native cleanup may run after the bounded staging collector.
// Prove exact accepted ownership and complete draft absence; a promoted
// Task or another live locator can never be treated as retired staging.
func (repository *Repository) verifyRetiredAcceptedBlueprintDraft(
	ctx context.Context,
	claim blueprints.EnvironmentBlueprintStageClaim,
	descriptorKey, locatorKey string,
	evidence []*etcdstore.KeyValue,
) error {
	conditions, uncaptured, err := uncapturedOwnerConditions(
		evidence[2],
		evidence[3],
		blueprints.EnvironmentBlueprintStageDescriptor{Claim: claim},
	)
	if err != nil {
		return err
	}
	if claim.TaskLocator == nil || !uncaptured || evidence[1] != nil {
		return errs.New(errs.KindStateConflict, "Blueprint staging retirement evidence is unavailable")
	}
	conditions = append(
		conditions,
		etcdstore.Condition{Key: descriptorKey},
		etcdstore.Condition{Key: locatorKey},
		etcdstore.Condition{
			Key:    blueprints.EnvironmentBlueprintRevisionPrefixFinal(claim.EnvironmentID, claim.RevisionID),
			Prefix: true,
		},
	)
	result, err := repository.store.Transact(ctx, conditions, nil)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Blueprint staging retirement evidence changed")
	}
	return nil
}

// Retirement completes before the immutable accepted Task/revision namespace
// may be reused. Every page retains the exact Task and descriptor CAS guards.
func (repository *Repository) retireAcceptedBlueprintDraft(
	ctx context.Context,
	descriptor blueprints.EnvironmentBlueprintStageDescriptor,
) error {
	// The normalized revision has a bounded chunk count; this ceiling also
	// prevents a contended collector from spinning without making progress.
	for page := 0; page < 128; page++ {
		result, err := repository.store.Get(
			ctx,
			blueprints.EnvironmentBlueprintDescriptorKeyByID(descriptor.Claim.DescriptorID),
		)
		if err != nil {
			return err
		}
		if result == nil {
			return blueprints.CorruptEnvironmentBlueprintStage()
		}
		if result.Entry == nil {
			return nil
		}
		current, err := blueprints.DecodeEnvironmentBlueprintStageDescriptor(result.Entry.Value)
		clear(result.Entry.Value)
		if err != nil {
			return err
		}
		if current.State != blueprints.EnvironmentBlueprintStageAbandoned ||
			!blueprints.SameEnvironmentBlueprintStageClaim(current.Claim, descriptor.Claim) {
			return errs.New(errs.KindStateConflict, "Blueprint draft retirement owner changed")
		}
		retired, err := repository.cleanupAbandonedEnvironmentBlueprintDescriptor(ctx, current, time.Now().UTC())
		if err != nil {
			return err
		}
		if retired {
			return nil
		}
	}
	return errs.New(errs.KindStateConflict, "Blueprint draft retirement did not settle within its bounded pages")
}
