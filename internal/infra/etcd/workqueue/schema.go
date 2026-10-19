package workqueue

import (
	"context"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const schemaKey = root + "schema"
const retiredJournalSchemaKey = "/v1/meta/task-journal-schema"
const retiredJournalRejection = "retired-by-native-execution"

// These are rejection fences, not readers or migration inputs. In particular,
// an old unfinished execution can never be reinterpreted as new work.
var retiredExecutionRoots = []string{
	"/v1/tasks/",
	"/v1/indexes/tasks/",
	"/v1/runtime/task-queue/",
	"/v1/runtime/assignments/",
	"/v1/runtime/controller-task-claims/",
	"/v1/runtime/blueprint-parent-claims/",
	"/v1/runtime/task-pruning/",
}

// EnsureSchema admits one coordinated storage cutover before publication or
// executor registration. It never changes workload definitions, files or data.
// Existing installations require explicit offline reconciliation or an approved
// disposable reset; this method does neither and has no legacy decoder.
func (repository *Repository) EnsureSchema(ctx context.Context) error {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return err
	}
	state, err := repository.store.Range(ctx, keyvalue.RangeRequest{Prefix: root, Limit: 1})
	if err != nil {
		return err
	}
	if state == nil || state.ReadRevision <= 0 || len(state.Values) > 1 || state.More && len(state.Values) == 0 {
		return corrupt()
	}
	defer keyvalue.ClearRangeValues(state.Values)
	read, err := repository.read(ctx, []string{schemaKey, retiredJournalSchemaKey}, state.ReadRevision)
	if err != nil {
		return err
	}
	defer keyvalue.ClearValues(read.Values)
	wanted := "native-execution/v1;storage-epoch=" + strconv.Itoa(controllerupgrade.StorageEpoch)
	current, rejection := read.Values[0], read.Values[1]
	if current != nil &&
		(string(current.Value) != wanted || rejection == nil || string(rejection.Value) != retiredJournalRejection) {
		return errs.New(
			errs.KindStateConflict,
			"execution storage schema is incompatible; do not replace binaries against this store",
		)
	}
	if current == nil && (len(state.Values) != 0 || rejection != nil) {
		return errs.New(
			errs.KindStateConflict,
			"execution cutover requires an empty execution store; preserve existing data and reconcile the previous installation offline",
		)
	}
	conditions := []keyvalue.Condition{{Key: root, Prefix: true}, {Key: retiredJournalSchemaKey}}
	for _, prefix := range retiredExecutionRoots {
		old, err := repository.store.Range(
			ctx,
			keyvalue.RangeRequest{Prefix: prefix, Revision: state.ReadRevision, Limit: 1},
		)
		if err != nil {
			return err
		}
		if old == nil || old.ReadRevision != state.ReadRevision || len(old.Values) > 1 ||
			old.More && len(old.Values) == 0 {
			return corrupt()
		}
		present := len(old.Values) != 0
		keyvalue.ClearRangeValues(old.Values)
		if present {
			return errs.New(
				errs.KindStateConflict,
				"superseded Task records remain; export history and reconcile outstanding effects before the native execution cutover",
			)
		}
		conditions = append(conditions, keyvalue.Condition{Key: prefix, Prefix: true})
	}
	if current != nil {
		return nil
	}
	result, err := repository.Commit(ctx, Change{Conditions: conditions, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: schemaKey, Value: []byte(wanted)},
		// The previous Controller validates this key before starting. An
		// incompatible value prevents accidental binary-only rollback.
		{Type: keyvalue.MutationPut, Key: retiredJournalSchemaKey, Value: []byte(retiredJournalRejection)},
	}})
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(
			errs.KindStateConflict,
			"execution store changed during schema initialization; no cutover was performed",
		)
	}
	return nil
}
