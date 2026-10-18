package etcd

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Publication makes prepared hooks executable only in the owning native work
// transaction. The caller must join the retained source publication fragment in
// that same transaction; staging snapshots alone never publishes an execution.
func (prepared PreparedBlueprintReleaseHooks) Publication(evidence ReleaseHookWorkPublication) (workqueue.Change, error) {
	if len(evidence.Hooks) == 0 || evidence.Item.Input == nil ||
		prepared.revision <= 0 || prepared.key != blueprintReleaseHookStageKey(evidence.Item.ID) ||
		prepared.stage.TaskID != evidence.Task.ID || prepared.stage.WorkItemID != evidence.Item.ID ||
		prepared.stage.OperationID != evidence.OperationID || prepared.stage.Input != *evidence.Item.Input ||
		prepared.stage.Count != len(evidence.Hooks) || len(prepared.snapshotRevisions) != len(evidence.Hooks) {
		return workqueue.Change{}, errs.New(errs.KindValidationFailed, "Blueprint hooks do not match their prepared work")
	}
	// Match the original capture before adding retention metadata. That metadata
	// is produced only after the inert snapshots have acquired storage revisions.
	unretained := evidence
	unretained.Hooks = append([]ReleaseHookExecutionPublication(nil), evidence.Hooks...)
	for index, hook := range evidence.Hooks {
		if hook.Execution.SourceMembershipCount == 0 || hook.Execution.SourceMembershipSHA256 == "" ||
			hook.SnapshotRevision <= 0 || hook.SnapshotRevision != prepared.snapshotRevisions[hook.Execution.SnapshotID] {
			return workqueue.Change{}, errs.New(errs.KindValidationFailed, "Blueprint hook retained sources or snapshot revision are missing")
		}
		unretained.Hooks[index].Execution.SourceMembershipCount = 0
		unretained.Hooks[index].Execution.SourceMembershipSHA256 = ""
	}
	original, err := prepareBlueprintReleaseHookPublicationFragment(unretained)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clearReleaseHookPublicationFragment(original)
	hash := sha256.New()
	for _, mutation := range original.mutations {
		hash.Write([]byte(mutation.Key))
		hash.Write([]byte{0})
		hash.Write(mutation.Value)
	}
	if hex.EncodeToString(hash.Sum(nil)) != prepared.stage.Digest {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint hook captured inputs changed after preparation")
	}
	fragment, err := prepareBlueprintReleaseHookPublicationFragment(evidence)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clearReleaseHookPublicationFragment(fragment)
	change := workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: prepared.key, ModRevision: prepared.revision}},
		Mutations:  []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: prepared.key}},
	}
	for index, hook := range evidence.Hooks {
		// The fragment has one new execution followed by its immutable snapshot.
		// Compare the staged snapshot instead of recreating or overwriting it.
		execution, snapshot := fragment.mutations[index*2], fragment.mutations[index*2+1]
		change.Conditions = append(change.Conditions,
			keyvalue.Condition{Key: execution.Key},
			keyvalue.Condition{Key: snapshot.Key, ModRevision: hook.SnapshotRevision},
		)
		execution.Value = append([]byte(nil), execution.Value...)
		change.Mutations = append(change.Mutations, execution)
	}
	return change, nil
}
