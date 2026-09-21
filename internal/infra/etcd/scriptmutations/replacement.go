package scriptmutations

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) PrepareScriptReplacement(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[servicerecord.ServiceRecord],
	current etcdstore.Versioned[scriptrecord.Record],
	desired core.Script,
) (scriptrecord.Record, []etcdstore.Condition, []etcdstore.Mutation, func(int64, []*etcdstore.KeyValue) error, error) {
	replacement, err := scriptrecord.ReplaceDesired(current.Record, desired)
	if err != nil {
		return scriptrecord.Record{}, nil, nil, nil, err
	}
	if err := ValidateScriptHierarchy(ctx, environment, project, target, replacement); err != nil {
		return scriptrecord.Record{}, nil, nil, nil, err
	}
	if err := ValidateScriptVersion(current); err != nil {
		return scriptrecord.Record{}, nil, nil, nil, err
	}
	active, err := scriptrecord.ReadActiveScriptSet(
		ctx,
		repository.store,
		current.Record.EnvironmentID,
		current.ReadRevision,
	)
	if err != nil {
		return scriptrecord.Record{}, nil, nil, nil, err
	}
	if current.Record.ScriptSetGeneration != active.Record.GenerationID {
		return scriptrecord.Record{}, nil, nil, nil, errs.New(errs.KindStateConflict, "Script-set generation changed")
	}
	replacement.ScriptSetGeneration = active.Record.GenerationID
	indexKeys := []string{
		scriptrecord.ScriptSetOwnerKey(
			current.Record.EnvironmentID,
			active.Record.GenerationID,
			current.Record.Desired.ID,
		),
		scriptrecord.ScriptSetSlugKey(
			current.Record.EnvironmentID,
			active.Record.GenerationID,
			current.Record.Desired.Slug,
		),
	}
	slugChanged := replacement.Desired.Slug != current.Record.Desired.Slug
	if slugChanged {
		indexKeys = append(
			indexKeys,
			scriptrecord.ScriptSetSlugKey(
				current.Record.EnvironmentID,
				active.Record.GenerationID,
				replacement.Desired.Slug,
			),
		)
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     indexKeys,
		Revision: current.ReadRevision,
	})
	if err != nil {
		return scriptrecord.Record{}, nil, nil, nil, err
	}
	if indexes == nil || len(indexes.Values) != len(indexKeys) || indexes.Values[0] == nil ||
		indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != current.Record.Desired.ID ||
		string(indexes.Values[1].Value) != current.Record.Desired.ID {
		return scriptrecord.Record{}, nil, nil, nil, errs.New(
			errs.KindInternal,
			"Script indexes are missing or corrupt",
		)
	}
	if slugChanged && indexes.Values[2] != nil {
		return scriptrecord.Record{}, nil, nil, nil, errs.New(errs.KindNameConflict, "Script slug is already in use")
	}
	value, err := scriptrecord.EncodeRecord(replacement)
	if err != nil {
		return scriptrecord.Record{}, nil, nil, nil, err
	}
	conditions := scriptWriteConditions(
		environment, project, target, current.Record, &current, active,
		indexes.Values[0].ModRevision, indexes.Values[1].ModRevision,
	)
	activeValue, err := scriptrecord.EncodeScriptSetGeneration(active.Record)
	if err != nil {
		clear(value)
		return scriptrecord.Record{}, nil, nil, nil, err
	}
	mutations := []etcdstore.Mutation{
		{
			Type: etcdstore.MutationPut,
			Key: scriptrecord.ScriptSetScriptKey(
				replacement.EnvironmentID,
				active.Record.GenerationID,
				replacement.Desired.ID,
			),
			Value: value,
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   scriptrecord.ScriptSetActiveKey(replacement.EnvironmentID),
			Value: activeValue,
		},
	}
	extras := scriptWriteConflictExtras{}
	if slugChanged {
		extras.newSlug = replacement.Desired.Slug
		conditions = append(
			conditions,
			etcdstore.Condition{
				Key: scriptrecord.ScriptSetSlugKey(
					replacement.EnvironmentID,
					active.Record.GenerationID,
					extras.newSlug,
				),
			},
		)
		mutations = append(
			mutations,
			etcdstore.Mutation{
				Type: etcdstore.MutationDelete,
				Key: scriptrecord.ScriptSetSlugKey(
					current.Record.EnvironmentID,
					active.Record.GenerationID,
					current.Record.Desired.Slug,
				),
			},
			etcdstore.Mutation{
				Type: etcdstore.MutationPut,
				Key: scriptrecord.ScriptSetSlugKey(
					replacement.EnvironmentID,
					active.Record.GenerationID,
					extras.newSlug,
				),
				Value: []byte(replacement.Desired.ID),
			},
		)
	}
	if replacement.ActiveGeneration != current.Record.ActiveGeneration {
		generation, generationErr := scriptrecord.NewScriptBodyGeneration(replacement)
		if generationErr != nil {
			etcdstore.ClearMutationValues(mutations)
			return scriptrecord.Record{}, nil, nil, nil, generationErr
		}
		generationValue, generationErr := scriptrecord.EncodeScriptBodyGeneration(generation)
		if generationErr != nil {
			etcdstore.ClearMutationValues(mutations)
			return scriptrecord.Record{}, nil, nil, nil, generationErr
		}
		extras.bodyGeneration = true
		conditions = append(conditions, etcdstore.Condition{Key: scriptrecord.ScriptSetBodyGenerationKey(
			replacement.EnvironmentID, active.Record.GenerationID, replacement.Desired.ID, replacement.ActiveGeneration,
		)})
		mutations = append(mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: scriptrecord.ScriptSetBodyGenerationKey(replacement.EnvironmentID, active.Record.GenerationID, replacement.Desired.ID, replacement.ActiveGeneration),
			Value: generationValue,
		})
	}
	classify := func(_ int64, values []*etcdstore.KeyValue) error {
		return classifyScriptWriteConflict(
			values,
			environment,
			project,
			target,
			current.Record,
			current.Revision,
			extras,
		)
	}
	return replacement, conditions, mutations, classify, nil
}
