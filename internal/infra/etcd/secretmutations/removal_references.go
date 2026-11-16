package secretmutations

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintsecretrefs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskcredentialrefs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareSecretRemovalAbsence reports an actionable dependency error and keeps every
// absence comparison in the transaction; a read alone cannot authorize deletion.
func PrepareSecretRemovalAbsence(
	ctx context.Context,
	backend persistenceStore,
	secretID string,
) ([]keyvalue.Condition, error) {
	// Completed Tasks release these references immediately on an operator's
	// deletion attempt, so periodic maintenance cadence never blocks the UI.
	queued, err := taskcredentialrefs.NewRepository(backend)
	if err != nil {
		return nil, err
	}
	if err := queued.PurgeFinishedForSecret(ctx, secretID); err != nil {
		return nil, err
	}
	guards, err := PrepareSecretScriptAbsence(ctx, backend, secretID)
	if err != nil {
		return nil, err
	}
	prefix := componentplanning.ComponentSecretReferencePrefix(secretID)
	read, err := backend.Range(ctx, keyvalue.RangeRequest{Prefix: prefix, Limit: 1})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) > 1 || read.More && len(read.Values) == 0 {
		return nil, errs.New(errs.KindInternal, "Secret Component reference evidence is incomplete")
	}
	defer clearReferenceValues(read.Values)
	if len(read.Values) != 0 {
		return nil, errs.New(errs.KindResourceInUse, "Secret is referenced by a Component")
	}
	guards = append(guards, keyvalue.Condition{Key: prefix, Prefix: true})
	prefix = blueprintsecretrefs.SecretPrefix(secretID)
	desired, err := backend.Range(ctx, keyvalue.RangeRequest{Prefix: prefix, Limit: 1, Revision: read.ReadRevision})
	if err != nil {
		return nil, err
	}
	if desired == nil || desired.ReadRevision != read.ReadRevision || len(desired.Values) > 1 ||
		desired.More && len(desired.Values) == 0 {
		return nil, errs.New(errs.KindInternal, "Secret Blueprint reference evidence is incomplete")
	}
	defer clearReferenceValues(desired.Values)
	if len(desired.Values) != 0 {
		return nil, errs.New(
			errs.KindResourceInUse,
			"This credential is selected in an Environment Blueprint; remove or replace that selection before deleting it",
		)
	}
	return append(guards, keyvalue.Condition{Key: prefix, Prefix: true},
		keyvalue.Condition{Key: taskcredentialrefs.SecretPrefix(secretID), Prefix: true}), nil
}

func clearReferenceValues(values []keyvalue.KeyValue) {
	for index := range values {
		clear(values[index].Value)
	}
}
