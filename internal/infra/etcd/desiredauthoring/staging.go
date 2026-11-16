package desiredauthoring

import (
	"bytes"
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
	descriptorRevision := result.Revision
	if !result.Succeeded {
		descriptorRevision, err = readIdenticalStagedValue(ctx, store, descriptorMutation.Key, sealed)
		if err != nil {
			return Publication{}, err
		}
	}
	final := Publication{Conditions: append([]keyvalue.Condition(nil), publication.Conditions[:3]...)}
	final.Conditions = append(
		final.Conditions,
		keyvalue.Condition{Key: descriptorMutation.Key, ModRevision: descriptorRevision},
	)
	for _, mutation := range publication.Mutations[1 : len(publication.Mutations)-1] {
		result, err = store.Transact(
			ctx,
			[]keyvalue.Condition{{Key: descriptorMutation.Key, ModRevision: descriptorRevision}, {Key: mutation.Key}},
			[]keyvalue.Mutation{mutation},
		)
		if err != nil {
			return Publication{}, err
		}
		contentRevision := result.Revision
		if !result.Succeeded {
			contentRevision, err = readIdenticalStagedValue(ctx, store, mutation.Key, mutation.Value)
			if err != nil {
				return Publication{}, err
			}
		}
		if !strings.Contains(mutation.Key, "/chunks/") {
			final.Conditions = append(
				final.Conditions,
				keyvalue.Condition{Key: mutation.Key, ModRevision: contentRevision},
			)
		}
	}
	head := publication.Mutations[len(publication.Mutations)-1]
	head.Value = append([]byte(nil), head.Value...)
	// The staging owner becomes the published revision's retention owner in the
	// same transaction as its head. It does not invent a Task for a metadata edit.
	descriptor.State = blueprints.EnvironmentBlueprintStagePublishedMetadata
	published, err := blueprints.EncodeEnvironmentBlueprintStageDescriptor(descriptor)
	if err != nil {
		clear(head.Value)
		return Publication{}, err
	}
	final.Mutations = []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: descriptorMutation.Key, Value: published},
		head,
	}
	return final, nil
}

// Existing immutable content is reusable only byte for byte. The final
// descriptor comparison also fences concurrent abandonment or publication.
func readIdenticalStagedValue(ctx context.Context, store Store, key string, expected []byte) (int64, error) {
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}})
	if err != nil {
		return 0, err
	}
	if read == nil || len(read.Values) != 1 {
		return 0, errs.New(errs.KindInternal, "desired staged content read is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || !bytes.Equal(read.Values[0].Value, expected) {
		return 0, errs.New(errs.KindStateConflict, "desired staged content changed")
	}
	return read.Values[0].ModRevision, nil
}
