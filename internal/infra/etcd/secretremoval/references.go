package secretremoval

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secretmutations"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareReferenceAbsence reports an actionable dependency error and keeps every
// absence comparison in the transaction; a read alone cannot authorize deletion.
func PrepareReferenceAbsence(ctx context.Context, backend store, secretID string) ([]keyvalue.Condition, error) {
	guards, err := secretmutations.PrepareSecretScriptAbsence(ctx, backend, secretID)
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
	return append(guards, keyvalue.Condition{Key: prefix, Prefix: true}), nil
}

func clearReferenceValues(values []keyvalue.KeyValue) {
	for index := range values {
		clear(values[index].Value)
	}
}
