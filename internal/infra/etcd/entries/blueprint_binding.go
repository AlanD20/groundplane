package entries

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BindBlueprintEntryEnvironment installs a derived lookup route. The current
// Environment projection remains the authority, so an index written before
// head publication cannot make staged Entry state visible.
func (repository *Repository) BindBlueprintEntryEnvironment(
	ctx context.Context,
	environmentID string,
	entryID string,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil ||
		recordcodec.ValidateID(ids.KindEnvEntry, entryID) != nil {
		return errs.New(errs.KindValidationFailed, "Blueprint Entry lookup identity is invalid")
	}
	key := BlueprintEntryEnvironmentPrefix + entryID
	result, err := repository.store.Transact(
		ctx,
		[]etcdstore.Condition{{Key: key}},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: []byte(environmentID)}},
	)
	if err != nil {
		return err
	}
	if result.Succeeded {
		return nil
	}
	existing, err := repository.store.Get(ctx, key)
	if err != nil {
		return err
	}
	if existing != nil && existing.Entry != nil && string(existing.Entry.Value) == environmentID {
		return nil
	}
	return errs.New(errs.KindStateConflict, "Blueprint Entry lookup identity is already occupied")
}

func (repository *Repository) ResolveBlueprintEntryEnvironment(
	ctx context.Context,
	entryID string,
) (string, bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return "", false, err
	}
	if recordcodec.ValidateID(ids.KindEnvEntry, entryID) != nil {
		return "", false, errs.New(errs.KindValidationFailed, "Blueprint Entry lookup id is invalid")
	}
	result, err := repository.store.Get(ctx, BlueprintEntryEnvironmentPrefix+entryID)
	if err != nil {
		return "", false, err
	}
	if result == nil {
		return "", false, errs.New(errs.KindInternal, "Blueprint Entry lookup read is empty")
	}
	if result.Entry == nil {
		return "", false, nil
	}
	environmentID := string(result.Entry.Value)
	if recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil {
		return "", false, errs.New(errs.KindInternal, "Blueprint Entry lookup is corrupt")
	}
	return environmentID, true, nil
}
