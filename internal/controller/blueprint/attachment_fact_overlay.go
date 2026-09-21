package blueprint

import (
	"context"
	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/entrygeneration"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintAttachFactValue struct {
	secret bool
	value  []byte
}

type blueprintAttachFactOverlay struct {
	fallback entrygeneration.EntryFactResolver
	aliases  map[string]string
	sets     map[string]map[string]map[string]blueprintAttachFactValue
}

func newBlueprintAttachFactOverlay(fallback entrygeneration.EntryFactResolver) *blueprintAttachFactOverlay {
	return &blueprintAttachFactOverlay{
		fallback: fallback, aliases: map[string]string{},
		sets: map[string]map[string]map[string]blueprintAttachFactValue{},
	}
}

func (overlay *blueprintAttachFactOverlay) addOwner(
	name string,
	own adapters.Input,
	grantNames []string,
	grants []attachments.GrantInput,
	adapter adapters.Adapter,
) error {
	overlay.aliases[name] = name
	overlay.sets[name] = map[string]map[string]blueprintAttachFactValue{}
	if err := overlay.addSet(name, "", adapter, own); err != nil {
		return err
	}
	for index, grant := range grants {
		if err := overlay.addSet(name, grantNames[index], adapter, grant.Params); err != nil {
			return err
		}
	}
	return nil
}

func (overlay *blueprintAttachFactOverlay) addAlias(name string, owner string) {
	overlay.aliases[name] = owner
}

func (overlay *blueprintAttachFactOverlay) addSet(
	owner string,
	grant string,
	adapter adapters.Adapter,
	params adapters.Input,
) error {
	facts, err := adapters.BuildFacts(adapter, params)
	if err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	defer adapters.ClearFacts(facts)
	values := make(map[string]blueprintAttachFactValue, len(facts))
	for _, fact := range facts {
		values[fact.Key] = blueprintAttachFactValue{secret: fact.Secret, value: append([]byte(nil), fact.Value...)}
	}
	overlay.sets[owner][grant] = values
	return nil
}

func (overlay *blueprintAttachFactOverlay) ResolveFact(
	ctx context.Context,
	environmentID string,
	reference core.FactRef,
	destinationSecret bool,
	consume secretvalue.PlaintextConsumer,
) error {
	owner, candidate := overlay.aliases[reference.Attach]
	if !candidate {
		return overlay.fallback.ResolveFact(ctx, environmentID, reference, destinationSecret, consume)
	}
	sets, candidateOwner := overlay.sets[owner]
	if !candidateOwner {
		reference.Attach = owner
		return overlay.fallback.ResolveFact(ctx, environmentID, reference, destinationSecret, consume)
	}
	set := sets[reference.Grant]
	fact, found := set[reference.Key]
	if !found {
		return errs.New(errs.KindValidationFailed, "Blueprint Attach fact is not declared")
	}
	if fact.secret && !destinationSecret {
		return errs.New(errs.KindValidationFailed, "Secret Blueprint Attach fact requires a secret Entry destination")
	}
	value := append([]byte(nil), fact.value...)
	defer clear(value)
	return consume(value)
}

func (overlay *blueprintAttachFactOverlay) clear() {
	for _, sets := range overlay.sets {
		for _, facts := range sets {
			for key, fact := range facts {
				clear(fact.value)
				delete(facts, key)
			}
		}
	}
}

func cloneBlueprintAttachFactSets(values []attachrecord.FactSetMetadata) []attachrecord.FactSetMetadata {
	cloned := make([]attachrecord.FactSetMetadata, len(values))
	for index, value := range values {
		cloned[index] = attachrecord.FactSetMetadata{
			GrantAttachID: value.GrantAttachID,
			Facts:         append([]attachrecord.FactDefinition(nil), value.Facts...),
		}
	}
	return cloned
}
