package etcd

import (
	"context"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/remotefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/remotehosts"
	"github.com/AlanD20/groundplane/internal/infra/etcd/remoteimages"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumRemoteReleaseConditions = 32

// DirectReleaseRemoteSource binds the remote proof and exact executor to the
// accepted Deploy. The mutable executor record is re-read at each boundary.
type DirectReleaseRemoteSource struct {
	Executor   workexecution.Identity `json:"executor"`
	Conditions []keyvalue.Condition   `json:"conditions"`
}

func CaptureDirectReleaseRemoteSource(executor workexecution.Identity, conditions []keyvalue.Condition,
	readRevision int64,
) (*DirectReleaseRemoteSource, error) {
	if len(conditions) > maximumRemoteReleaseConditions+1 {
		return nil, invalidRemoteReleaseSource()
	}
	captured := make([]keyvalue.Condition, 0, len(conditions))
	executorKey := workqueue.ExecutorKey(executor)
	var executorRevision int64
	for _, condition := range conditions {
		if condition.Key == executorKey && !condition.Prefix {
			if condition.ModRevision <= 0 || condition.ModRevision > readRevision ||
				executorRevision != 0 && condition.ModRevision != executorRevision {
				return nil, invalidRemoteReleaseSource()
			}
			executorRevision = condition.ModRevision
			continue
		}
		captured = append(captured, condition)
	}
	slices.SortFunc(captured, compareRemoteConditions)
	captured = slices.Compact(captured)
	source := &DirectReleaseRemoteSource{Executor: executor, Conditions: captured}
	if source.validate(readRevision) != nil {
		return nil, invalidRemoteReleaseSource()
	}
	return source, nil
}

func compareRemoteConditions(left, right keyvalue.Condition) int {
	return strings.Compare(left.Key, right.Key)
}

func (source *DirectReleaseRemoteSource) validate(readRevision int64) error {
	if source.Executor.Validate() != nil || source.Executor.Executor != workexecution.Agent || readRevision <= 0 ||
		len(source.Conditions) == 0 || len(source.Conditions) > maximumRemoteReleaseConditions {
		return invalidRemoteReleaseSource()
	}
	executorKey := workqueue.ExecutorKey(source.Executor)
	for index, condition := range source.Conditions {
		if condition.Key == "" || condition.ModRevision < 0 || condition.ModRevision > readRevision ||
			condition.Prefix && (condition.ModRevision != 0 || strings.HasPrefix(executorKey, condition.Key)) ||
			condition.Key == executorKey || index > 0 && source.Conditions[index-1].Key >= condition.Key {
			return invalidRemoteReleaseSource()
		}
	}
	return nil
}

func (request DirectReleasePreparation) validateRemoteSource() error {
	source := request.Remote
	target := workexecution.Placement{Executor: workexecution.Agent, HostID: source.Executor.HostID}
	if request.GroupID != "" || len(request.Networks) == 0 || request.Intent.OperationKind != release.OperationDeploy ||
		request.Intent.Workspace.Kind != release.WorkspaceTenant || request.Configuration.Placement != target ||
		request.Intent.Strategy != release.StrategyRecreate || request.Intent.CandidateWorkload.ReplicaCount != 1 ||
		source.validate(request.Source.ReadRevision) != nil {
		return invalidRemoteReleaseSource()
	}
	image, err := remotefetch.Reference(request.Intent.CandidateWorkload.RequestedReference)
	if err != nil {
		return invalidRemoteReleaseSource()
	}
	for _, key := range []string{
		hostbindings.AgentKey(source.Executor.AgentID),
		hostbindings.HostKey(source.Executor.HostID),
		hostbindings.PlatformKey(source.Executor.HostID),
		remotehosts.Key(source.Executor.HostID),
		remoteimages.WitnessKey(source.Executor.HostID, image),
	} {
		index, found := slices.BinarySearchFunc(source.Conditions, key, func(value keyvalue.Condition, key string) int {
			return strings.Compare(value.Key, key)
		})
		if !found || source.Conditions[index].ModRevision <= 0 {
			return invalidRemoteReleaseSource()
		}
	}
	return nil
}

func invalidRemoteReleaseSource() error {
	return errs.New(errs.KindValidationFailed, "remote Release lacks valid captured Host and image authority")
}

// DirectReleaseRemoteConditions compares captured remote evidence at one
// forward boundary. The next native transition must join these comparisons.
func (ledger *ReleaseLedger) DirectReleaseRemoteConditions(
	ctx context.Context,
	source *DirectReleaseRemoteSource,
	revision int64,
) ([]keyvalue.Condition, error) {
	current, conditions, err := ledger.matchRemoteReleaseSource(ctx, source, revision)
	if err != nil {
		return nil, err
	}
	if !current {
		return nil, errs.New(errs.KindStateConflict, "remote Release authority changed after preparation")
	}
	return conditions, nil
}

// matchRemoteReleaseSource reads the same MVCC view as the rest of authorization.
// On supersession it returns current comparisons, so a refusal may only settle
// while the conflicting evidence still exists.
func (ledger *ReleaseLedger) matchRemoteReleaseSource(ctx context.Context, source *DirectReleaseRemoteSource,
	revision int64,
) (bool, []keyvalue.Condition, error) {
	if source == nil {
		return true, nil, nil
	}
	if ledger == nil || ledger.store == nil || ctx == nil || source.validate(revision) != nil {
		return false, nil, invalidRemoteReleaseSource()
	}
	executorKey := workqueue.ExecutorKey(source.Executor)
	keys := []string{executorKey}
	for _, condition := range source.Conditions {
		if !condition.Prefix {
			keys = append(keys, condition.Key)
		}
	}
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return false, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return false, nil, invalidRemoteReleaseSnapshot()
	}
	defer keyvalue.ClearValues(read.Values)
	for index, value := range read.Values {
		if value != nil && (value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > revision) {
			return false, nil, invalidRemoteReleaseSnapshot()
		}
	}
	executorValue := read.Values[0]
	guards := []keyvalue.Condition{{Key: executorKey, ModRevision: keyvalue.RevisionOf(executorValue)}}
	current := true
	exactIndex := 1
	for _, expected := range source.Conditions {
		var actual keyvalue.Condition
		if expected.Prefix {
			actual, err = ledger.readRemoteReleasePrefix(ctx, expected, revision)
			if err != nil {
				return false, nil, err
			}
		} else {
			actual = keyvalue.Condition{Key: expected.Key, ModRevision: keyvalue.RevisionOf(read.Values[exactIndex])}
			exactIndex++
		}
		current = current && expected == actual
		guards = append(guards, actual)
	}
	if executorValue == nil {
		return false, guards, nil
	}
	executor, err := workqueue.DecodeExecutor(executorValue.Value)
	if err != nil {
		return false, nil, err
	}
	if !current || executor.Identity != source.Executor {
		return false, guards, nil
	}
	if !executor.Allows(workexecution.Release) {
		return false, nil, errs.New(errs.KindAgentReadinessChanged, "captured remote Release executor is unavailable")
	}
	return true, guards, nil
}

func (ledger *ReleaseLedger) readRemoteReleasePrefix(ctx context.Context, expected keyvalue.Condition,
	revision int64,
) (keyvalue.Condition, error) {
	read, err := ledger.store.Range(ctx, keyvalue.RangeRequest{Prefix: expected.Key, Limit: 1, Revision: revision})
	if err != nil {
		return keyvalue.Condition{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) > 1 {
		return keyvalue.Condition{}, invalidRemoteReleaseSnapshot()
	}
	if len(read.Values) == 0 {
		if read.More {
			return keyvalue.Condition{}, invalidRemoteReleaseSnapshot()
		}
		return expected, nil
	}
	value := read.Values[0]
	defer clear(value.Value)
	if !strings.HasPrefix(value.Key, expected.Key) || value.ModRevision <= 0 || value.ModRevision > revision {
		return keyvalue.Condition{}, invalidRemoteReleaseSnapshot()
	}
	return keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision}, nil
}

func invalidRemoteReleaseSnapshot() error {
	return errs.New(errs.KindStateConflict, "remote Release authority snapshot is incomplete")
}
