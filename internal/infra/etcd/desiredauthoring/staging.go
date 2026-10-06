package desiredauthoring

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Stage immutable values before the bounded resource transaction. Metadata
// descriptors own private staging without claiming an operator replay locator.
// Publication compares the descriptor and immutable roots; cleanup must first
// change that descriptor, so it cannot expose an incomplete revision. No resource
// or desired head changes here.
func stage(ctx context.Context, store Store, publication Publication) (Publication, error) {
	defer keyvalue.ClearMutationValues(publication.Mutations)
	if len(publication.Mutations) < 2 {
		return Publication{}, errs.New(errs.KindInternal, "desired publication is incomplete")
	}
	descriptorMutation := publication.Mutations[0]
	descriptor, err := blueprints.DecodeEnvironmentBlueprintStageDescriptor(descriptorMutation.Value)
	if err != nil {
		return Publication{}, err
	}
	descriptor.State = blueprints.EnvironmentBlueprintStageMetadata
	sealed, err := blueprints.EncodeEnvironmentBlueprintStageDescriptor(descriptor)
	if err != nil {
		return Publication{}, err
	}
	defer clear(sealed)
	result, err := store.Transact(ctx, []keyvalue.Condition{{Key: descriptorMutation.Key}}, []keyvalue.Mutation{{
		Type: keyvalue.MutationPut, Key: descriptorMutation.Key, Value: sealed}})
	if err != nil {
		return Publication{}, err
	}
	if !result.Succeeded {
		return Publication{}, errs.New(errs.KindStateConflict, "desired staging identity changed")
	}
	descriptorRevision := result.Revision
	final := Publication{Conditions: append([]keyvalue.Condition(nil), publication.Conditions[:3]...)}
	final.Conditions = append(final.Conditions, keyvalue.Condition{Key: descriptorMutation.Key, ModRevision: descriptorRevision})
	for _, mutation := range publication.Mutations[1 : len(publication.Mutations)-1] {
		result, err = store.Transact(ctx, []keyvalue.Condition{{Key: descriptorMutation.Key, ModRevision: descriptorRevision}, {Key: mutation.Key}}, []keyvalue.Mutation{mutation})
		if err != nil {
			return Publication{}, err
		}
		if !result.Succeeded {
			return Publication{}, errs.New(errs.KindStateConflict, "desired staged content changed")
		}
		if !strings.Contains(mutation.Key, "/chunks/") {
			final.Conditions = append(final.Conditions, keyvalue.Condition{Key: mutation.Key, ModRevision: result.Revision})
		}
	}
	head := publication.Mutations[len(publication.Mutations)-1]
	head.Value = append([]byte(nil), head.Value...)
	final.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: descriptorMutation.Key}, head}
	return final, nil
}
