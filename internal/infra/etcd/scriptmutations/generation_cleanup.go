package scriptmutations

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const scriptLocatorCleanupBatchSize int64 = 16

// cleanupEnvironmentDeletionScriptLocators removes locator authority for
// inactive/abandoned generations. The owned Environment tombstone is the
// durable proof that no Blueprint staging transaction can add more locators.
func CleanupEnvironmentDeletionScriptLocators(
	ctx context.Context,
	store mutationStore,
	environmentID string,
	authority etcdstore.Condition,
) error {
	if authority.Key == "" || authority.ModRevision <= 0 || authority.Prefix {
		return errs.New(errs.KindInternal, "Script locator cleanup authority is invalid")
	}
	for {
		conditions, mutations, err := PrepareEnvironmentDeletionScriptLocatorBatch(ctx, store, environmentID, authority)
		if err != nil {
			return err
		}
		if len(mutations) == 0 {
			return nil
		}
		result, err := store.Transact(ctx, conditions, mutations)
		if err != nil {
			return err
		}
		if !result.Succeeded {
			return errs.New(errs.KindStateConflict, "Script locator cleanup raced durable state")
		}
	}
}

// PrepareEnvironmentDeletionScriptLocatorBatch prepares one bounded cleanup
// transaction without executing it. The caller joins its Task authority before
// committing; an empty mutation list means all locators have been removed.
func PrepareEnvironmentDeletionScriptLocatorBatch(
	ctx context.Context,
	store mutationStore,
	environmentID string,
	authority etcdstore.Condition,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	if authority.Key == "" || authority.ModRevision <= 0 || authority.Prefix {
		return nil, nil, errs.New(errs.KindInternal, "Script locator cleanup authority is invalid")
	}
	page, err := store.Range(ctx, etcdstore.RangeRequest{
		Prefix: scriptrecord.ScriptEnvironmentLocatorPrefixFor(environmentID), Limit: scriptLocatorCleanupBatchSize,
	})
	if err != nil {
		return nil, nil, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) > int(scriptLocatorCleanupBatchSize) {
		if page != nil {
			etcdstore.ClearRangeValues(page.Values)
		}
		return nil, nil, errs.New(errs.KindInternal, "Script locator cleanup page is invalid")
	}
	defer etcdstore.ClearRangeValues(page.Values)
	if len(page.Values) == 0 {
		return nil, nil, nil
	}
	active, err := scriptrecord.ReadActiveScriptSet(ctx, store, environmentID, page.ReadRevision)
	if err != nil {
		return nil, nil, err
	}
	globalKeys := make([]string, len(page.Values))
	activeKeys := make([]string, len(page.Values))
	for index, locator := range page.Values {
		id := strings.TrimPrefix(locator.Key, scriptrecord.ScriptEnvironmentLocatorPrefixFor(environmentID))
		if id == "" || strings.Contains(id, "/") || string(locator.Value) != id {
			return nil, nil, errs.New(errs.KindInternal, "Script Environment locator is corrupt")
		}
		globalKeys[index] = scriptrecord.ScriptLocatorKey(id)
		activeKeys[index] = scriptrecord.ScriptSetScriptKey(environmentID, active.Record.GenerationID, id)
	}
	reads, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: append(globalKeys, activeKeys...), Revision: page.ReadRevision,
	})
	if err != nil {
		return nil, nil, err
	}
	if reads == nil || reads.ReadRevision != page.ReadRevision || len(reads.Values) != len(globalKeys)+len(activeKeys) {
		if reads != nil {
			etcdstore.ClearValues(reads.Values)
		}
		return nil, nil, errs.New(errs.KindInternal, "Script locator cleanup evidence is incomplete")
	}
	defer etcdstore.ClearValues(reads.Values)
	conditions := []etcdstore.Condition{
		authority,
		{Key: scriptrecord.ScriptSetActiveKey(environmentID), ModRevision: active.Revision},
	}
	mutations := make([]etcdstore.Mutation, 0, len(page.Values)*2)
	for index, environmentLocator := range page.Values {
		if reads.Values[len(globalKeys)+index] != nil {
			return nil, nil, errs.New(errs.KindStateConflict, "Environment retained durable Scripts")
		}
		global := reads.Values[index]
		if global == nil {
			return nil, nil, errs.New(errs.KindInternal, "Script global locator is missing")
		}
		locator, decodeErr := scriptrecord.DecodeScriptLocator(global.Value)
		if decodeErr != nil || locator.EnvironmentID != environmentID ||
			locator.ScriptID != string(environmentLocator.Value) {
			return nil, nil, errs.New(errs.KindStateConflict, "Script global locator ownership changed")
		}
		conditions = append(conditions,
			etcdstore.Condition{Key: environmentLocator.Key, ModRevision: environmentLocator.ModRevision},
			etcdstore.Condition{Key: global.Key, ModRevision: global.ModRevision},
		)
		mutations = append(mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: environmentLocator.Key},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: global.Key},
		)
	}
	return conditions, mutations, nil
}

func CleanupTaskEnvironmentDeletionScriptLocators(
	ctx context.Context,
	store mutationStore,
	environmentID string,
	taskID string,
) error {
	key := deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), environmentID)
	read, err := store.Get(ctx, key)
	if err != nil {
		return err
	}
	if read == nil || read.Entry == nil {
		return errs.New(errs.KindStateConflict, "Environment deletion tombstone is missing")
	}
	tombstone, err := deletionrecord.DecodeDeletionTombstone(read.Entry.Value)
	if err != nil || tombstone.TargetKind != deletionrecord.DeletionTargetEnvironment ||
		tombstone.TargetID != environmentID ||
		tombstone.TaskID != taskID {
		return errs.New(errs.KindStateConflict, "Environment deletion tombstone ownership changed")
	}
	return CleanupEnvironmentDeletionScriptLocators(
		ctx, store, environmentID, etcdstore.Condition{Key: key, ModRevision: read.Entry.ModRevision},
	)
}
