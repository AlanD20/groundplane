package githubconnection

import (
	"context"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/gitselector"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const runnerDefaultRecordKind = "github-repository-runner-default"

type RunnerDefaultKey struct {
	Owner        Scope  `json:"owner"`
	ConnectionID string `json:"connection_id"`
	RepositoryID int64  `json:"repository_id"`
}

func (key RunnerDefaultKey) path() (string, error) {
	if key.Owner.validate() != nil || recordcodec.ValidateID(IDKind, key.ConnectionID) != nil || key.RepositoryID <= 0 {
		return "", errs.New(errs.KindValidationFailed, "invalid GitHub repository Runner default key")
	}
	return "/v1/records/github-runner-defaults/" + string(key.Owner.Kind) + "/" + key.Owner.ID +
		"/" + key.ConnectionID + "/" + strconv.FormatInt(key.RepositoryID, 10), nil
}

type RunnerDefault struct {
	RunnerDefaultKey
	Selector gitselector.Selector `json:"selector"`
}

// ResolveRunnerDefault reads the nearest configured owner at one MVCC
// revision. Compare the nearer absent keys when accepting inherited work.
func (repository *Repository) ResolveRunnerDefault(
	ctx context.Context,
	keys []RunnerDefaultKey,
) (keyvalue.Versioned[RunnerDefault], []keyvalue.Condition, error) {
	if len(keys) == 0 || len(keys) > 3 {
		return keyvalue.Versioned[RunnerDefault]{}, nil, errs.New(
			errs.KindValidationFailed,
			"invalid GitHub repository default owner chain",
		)
	}
	paths := make([]string, 0, len(keys))
	for _, key := range keys {
		path, err := key.path()
		if err != nil {
			return keyvalue.Versioned[RunnerDefault]{}, nil, err
		}
		paths = append(paths, path)
	}
	result, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: paths})
	if err != nil {
		return keyvalue.Versioned[RunnerDefault]{}, nil, err
	}
	if result == nil || result.ReadRevision <= 0 || len(result.Values) != len(paths) {
		return keyvalue.Versioned[RunnerDefault]{}, nil, corruption()
	}
	defer keyvalue.ClearValues(result.Values)
	var conditions []keyvalue.Condition
	for i, path := range paths {
		entry := result.Values[i]
		if entry == nil {
			conditions = append(conditions, keyvalue.Condition{Key: path})
			continue
		}
		record, err := recordcodec.Decode[RunnerDefault](entry.Value, runnerDefaultRecordKind)
		if err != nil || record.RunnerDefaultKey != keys[i] || !record.Selector.Valid() ||
			entry.Key != path || entry.ModRevision <= 0 || entry.ModRevision > result.ReadRevision {
			return keyvalue.Versioned[RunnerDefault]{}, nil, corruption()
		}
		conditions = append(conditions, keyvalue.Condition{Key: path, ModRevision: entry.ModRevision})
		return keyvalue.Versioned[RunnerDefault]{
			Record:       record,
			Revision:     entry.ModRevision,
			ReadRevision: result.ReadRevision,
		}, conditions, nil
	}
	return keyvalue.Versioned[RunnerDefault]{ReadRevision: result.ReadRevision}, conditions, nil
}

func (repository *Repository) ChangeRunnerDefault(
	ctx context.Context,
	key RunnerDefaultKey,
	selector *gitselector.Selector,
	expected int64,
	guards []keyvalue.Condition,
	effects []keyvalue.Mutation,
) (int64, error) {
	path, err := key.path()
	if err != nil {
		return 0, err
	}
	if expected < 0 || (selector == nil && expected == 0) || (selector != nil && !selector.Valid()) {
		return 0, errs.New(errs.KindValidationFailed, "invalid GitHub repository Runner default change")
	}
	guards = append(guards, keyvalue.Condition{Key: path, ModRevision: expected})
	change := keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: path}
	if selector != nil {
		encoded, err := recordcodec.Encode(
			runnerDefaultRecordKind,
			RunnerDefault{RunnerDefaultKey: key, Selector: *selector},
		)
		if err != nil {
			return 0, err
		}
		change.Type, change.Value = keyvalue.MutationPut, encoded
	}
	result, err := repository.store.Transact(ctx, guards, append(effects, change))
	if err != nil {
		return 0, err
	}
	if !result.Succeeded {
		return 0, errs.New(errs.KindStateConflict, "repository Runner default or its authorization changed")
	}
	return result.Revision, nil
}
