package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Parent settlement already proves its children can no longer execute. Keep
// that exact proof while draining the parent's captured hook-value references
// through the existing bounded release protocol. Child Tasks have independent
// references, so this cannot release values still required by their recovery.
func (repository *TaskRepository) prepareBlueprintParentSourceRelease(ctx context.Context,
	task TaskRecord, revision int64, guards []keyvalue.Condition,
) (scriptsourcepublication.ScriptSourceReleaseFragment, []keyvalue.Condition, error) {
	key := scriptsourceevidence.ScriptSourceRootKey(task.OperationID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return scriptsourcepublication.ScriptSourceReleaseFragment{}, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return scriptsourcepublication.ScriptSourceReleaseFragment{}, nil, errs.New(
			errs.KindInternal,
			"Apply source hold read is incomplete",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return scriptsourcepublication.ScriptSourceReleaseFragment{}, []keyvalue.Condition{{Key: key}}, nil
	}
	root, err := scriptsourceevidence.DecodeScriptOperationSourceRoot(read.Values[0].Value)
	if err != nil || root.OperationID != task.OperationID {
		return scriptsourcepublication.ScriptSourceReleaseFragment{}, nil, errs.New(
			errs.KindStateConflict,
			"Apply source hold identity changed",
		)
	}
	authority, err := scriptsourcepublication.NewAuthority(repository.store)
	if err != nil {
		return scriptsourcepublication.ScriptSourceReleaseFragment{}, nil, err
	}
	if root.Phase == scriptsourceevidence.ScriptOperationSourceActive {
		fragment, err := authority.PrepareNormalRelease(ctx, task.OperationID, sourceref.RetryDispositionForbidden)
		if err != nil {
			return scriptsourcepublication.ScriptSourceReleaseFragment{}, nil, err
		}
		defer fragment.Clear()
		conditions := append(slices.Clone(guards), fragment.Conditions()...)
		result, err := repository.store.Transact(ctx, conditions, fragment.Mutations())
		keyvalue.ClearValues(result.FailureReads)
		if err != nil {
			return scriptsourcepublication.ScriptSourceReleaseFragment{}, nil, err
		}
		if !result.Succeeded {
			return scriptsourcepublication.ScriptSourceReleaseFragment{}, nil, errs.New(
				errs.KindStateConflict,
				"Apply source release raced its settlement proof",
			)
		}
		return scriptsourcepublication.ScriptSourceReleaseFragment{}, nil, errs.New(
			errs.KindResourceInUse,
			"Apply is releasing captured hook inputs",
		)
	}
	if root.Phase != scriptsourceevidence.ScriptOperationSourceReleasing ||
		root.ReleasePath != scriptsourceevidence.ScriptSourceReleaseNormal || root.RetryDisposition != sourceref.RetryDispositionForbidden {
		return scriptsourcepublication.ScriptSourceReleaseFragment{}, nil, errs.New(
			errs.KindStateConflict,
			"Apply source hold has another release owner",
		)
	}
	processed, drained, err := authority.ReleaseNext(ctx, task.OperationID, guards)
	if err != nil {
		return scriptsourcepublication.ScriptSourceReleaseFragment{}, nil, err
	}
	if processed || !drained {
		return scriptsourcepublication.ScriptSourceReleaseFragment{}, nil, errs.New(
			errs.KindResourceInUse,
			"Apply is releasing captured hook inputs",
		)
	}
	fragment, err := authority.PrepareReleaseFinalization(ctx, task.OperationID)
	return fragment, nil, err
}
