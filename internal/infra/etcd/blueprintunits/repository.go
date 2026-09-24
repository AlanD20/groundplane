package blueprintunits

import (
	"context"
	"math"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const pageSize = 64

type Reader interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
}

type Repository struct{ store Reader }

func NewRepository(backend Reader) (*Repository, error) {
	if backend == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint unit store is required")
	}
	return &Repository{store: backend}, nil
}

// Snapshot is one MVCC view of the latest head and acknowledged/unfinished
// units. A missing epoch is valid only before the first unit mutation.
type Snapshot struct {
	EnvironmentID string
	HeadTaskID    string
	HeadRevision  int64
	Epoch         EpochRecord
	EpochRevision int64
	Applied       []etcdstore.Versioned[AppliedRecord]
	Executions    []etcdstore.Versioned[ExecutionRecord]
	ReadRevision  int64
}

func (repository *Repository) Load(ctx context.Context, environmentID string) (Snapshot, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return Snapshot{}, err
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return Snapshot{}, invalidRecord()
	}
	headKey := blueprints.EnvironmentBlueprintHeadKey(environmentID)
	epochKey := EpochKey(environmentID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{headKey, epochKey}})
	if err != nil {
		return Snapshot{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 2 || read.Values[0] == nil {
		return Snapshot{}, errs.New(errs.KindStateConflict, "Environment Blueprint head is unavailable")
	}
	defer etcdstore.ClearValues(read.Values)
	headID, err := idempotencyrecord.DecodeTaskReference(read.Values[0].Value)
	if err != nil {
		return Snapshot{}, corruptRecord()
	}
	snapshot := Snapshot{
		EnvironmentID: environmentID, HeadTaskID: headID,
		HeadRevision: read.Values[0].ModRevision, ReadRevision: read.ReadRevision,
	}
	if read.Values[1] != nil {
		snapshot.Epoch, err = DecodeEpoch(read.Values[1].Value)
		if err != nil || snapshot.Epoch.EnvironmentID != environmentID {
			return Snapshot{}, corruptRecord()
		}
		snapshot.EpochRevision = read.Values[1].ModRevision
	}
	for _, prefix := range []string{AppliedPrefix(environmentID), ExecutionPrefix(environmentID)} {
		start := ""
		for {
			page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
				Prefix: prefix, StartExclusive: start, Limit: pageSize, Revision: snapshot.ReadRevision,
			})
			if err != nil {
				return Snapshot{}, err
			}
			if page == nil || page.ReadRevision != snapshot.ReadRevision || page.More && len(page.Values) == 0 {
				return Snapshot{}, corruptRecord()
			}
			for _, value := range page.Values {
				if !strings.HasPrefix(value.Key, prefix) || value.ModRevision <= 0 ||
					snapshot.EpochRevision == 0 || value.ModRevision > snapshot.EpochRevision {
					return Snapshot{}, corruptRecord()
				}
				if prefix == AppliedPrefix(environmentID) {
					record, err := DecodeApplied(value.Value)
					if err != nil || record.EnvironmentID != environmentID ||
						value.Key != AppliedKey(environmentID, record.Target) {
						return Snapshot{}, corruptRecord()
					}
					snapshot.Applied = append(snapshot.Applied, etcdstore.Versioned[AppliedRecord]{
						Record: record, Revision: value.ModRevision, ReadRevision: snapshot.ReadRevision,
					})
				} else {
					record, err := DecodeExecution(value.Value)
					if err != nil || record.EnvironmentID != environmentID ||
						value.Key != ExecutionKey(environmentID, record.PlanID) {
						return Snapshot{}, corruptRecord()
					}
					snapshot.Executions = append(snapshot.Executions, etcdstore.Versioned[ExecutionRecord]{
						Record: record, Revision: value.ModRevision, ReadRevision: snapshot.ReadRevision,
					})
				}
			}
			if !page.More {
				break
			}
			start = page.Values[len(page.Values)-1].Key
		}
	}
	return snapshot, nil
}

type AppliedChange struct {
	Target ResourceKey
	Next   *AppliedRecord
}

type ExecutionChange struct {
	PlanID string
	Next   *ExecutionRecord
}

// MutationPlan is appended to the same transaction that publishes or retires
// the affected child Task. Its head and epoch compares fence the whole source
// snapshot; exact record compares fence changed unit state. A caller must not
// commit these mutations separately from child Task/receipt authority.
type MutationPlan struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
}

func (plan MutationPlan) Conditions() []etcdstore.Condition { return slices.Clone(plan.conditions) }
func (plan MutationPlan) Mutations() []etcdstore.Mutation   { return slices.Clone(plan.mutations) }
func (plan MutationPlan) Clear()                            { etcdstore.ClearMutationValues(plan.mutations) }

func PrepareMutation(
	snapshot Snapshot, applied []AppliedChange, executions []ExecutionChange,
) (MutationPlan, error) {
	if ids.Validate(ids.KindEnvironment, snapshot.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, snapshot.HeadTaskID) != nil ||
		snapshot.ReadRevision <= 0 || snapshot.HeadRevision <= 0 || snapshot.EpochRevision < 0 ||
		(snapshot.EpochRevision == 0 && snapshot.Epoch.Sequence != 0) ||
		(snapshot.EpochRevision > 0 && (snapshot.Epoch.EnvironmentID != snapshot.EnvironmentID ||
			snapshot.Epoch.Sequence == 0 || snapshot.Epoch.Sequence == ^uint64(0))) ||
		len(applied)+len(executions) == 0 {
		return MutationPlan{}, invalidRecord()
	}
	knownApplied := make(map[ResourceKey]etcdstore.Versioned[AppliedRecord], len(snapshot.Applied))
	for _, versioned := range snapshot.Applied {
		if versioned.Record.EnvironmentID != snapshot.EnvironmentID ||
			validateApplied(versioned.Record) != nil || versioned.Revision <= 0 {
			return MutationPlan{}, invalidRecord()
		}
		knownApplied[versioned.Record.Target] = versioned
	}
	knownExecutions := make(map[string]etcdstore.Versioned[ExecutionRecord], len(snapshot.Executions))
	for _, versioned := range snapshot.Executions {
		if versioned.Record.EnvironmentID != snapshot.EnvironmentID ||
			validateExecution(versioned.Record) != nil || versioned.Revision <= 0 {
			return MutationPlan{}, invalidRecord()
		}
		knownExecutions[versioned.Record.PlanID] = versioned
	}
	epoch := EpochRecord{EnvironmentID: snapshot.EnvironmentID, Sequence: snapshot.Epoch.Sequence + 1}
	epochValue, err := EncodeEpoch(epoch)
	if err != nil {
		return MutationPlan{}, err
	}
	plan := MutationPlan{
		conditions: []etcdstore.Condition{
			{Key: blueprints.EnvironmentBlueprintHeadKey(snapshot.EnvironmentID), ModRevision: snapshot.HeadRevision},
			{Key: EpochKey(snapshot.EnvironmentID), ModRevision: snapshot.EpochRevision},
		},
		mutations: []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: EpochKey(snapshot.EnvironmentID), Value: epochValue}},
	}
	seenApplied := make(map[ResourceKey]bool, len(applied))
	for _, change := range applied {
		if !validKey(change.Target) || seenApplied[change.Target] {
			plan.Clear()
			return MutationPlan{}, invalidRecord()
		}
		seenApplied[change.Target] = true
		key := AppliedKey(snapshot.EnvironmentID, change.Target)
		current, exists := knownApplied[change.Target]
		plan.conditions = append(plan.conditions, etcdstore.Condition{Key: key, ModRevision: current.Revision})
		if change.Next == nil {
			if !exists {
				plan.Clear()
				return MutationPlan{}, invalidRecord()
			}
			plan.mutations = append(plan.mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: key})
			continue
		}
		if change.Next.EnvironmentID != snapshot.EnvironmentID || change.Next.Target != change.Target ||
			!validAppliedAdvance(exists, *change.Next, knownExecutions) {
			plan.Clear()
			return MutationPlan{}, invalidRecord()
		}
		value, err := EncodeApplied(*change.Next)
		if err != nil {
			plan.Clear()
			return MutationPlan{}, err
		}
		plan.mutations = append(plan.mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: value})
	}
	seenExecutions := make(map[string]bool, len(executions))
	for _, change := range executions {
		if ids.Validate(ids.KindPlan, change.PlanID) != nil || seenExecutions[change.PlanID] {
			plan.Clear()
			return MutationPlan{}, invalidRecord()
		}
		seenExecutions[change.PlanID] = true
		key := ExecutionKey(snapshot.EnvironmentID, change.PlanID)
		current, exists := knownExecutions[change.PlanID]
		plan.conditions = append(plan.conditions, etcdstore.Condition{Key: key, ModRevision: current.Revision})
		if change.Next == nil {
			if !exists {
				plan.Clear()
				return MutationPlan{}, invalidRecord()
			}
			plan.mutations = append(plan.mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: key})
			continue
		}
		if change.Next.EnvironmentID != snapshot.EnvironmentID || change.Next.PlanID != change.PlanID ||
			!validExecutionAdvance(snapshot.HeadTaskID, current.Record, exists, *change.Next) {
			plan.Clear()
			return MutationPlan{}, invalidRecord()
		}
		value, err := EncodeExecution(*change.Next)
		if err != nil {
			plan.Clear()
			return MutationPlan{}, err
		}
		plan.mutations = append(plan.mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: value})
	}
	return plan, nil
}

func validAppliedAdvance(
	exists bool, next AppliedRecord, executions map[string]etcdstore.Versioned[ExecutionRecord],
) bool {
	if next.SourceTaskID == "" {
		return !exists && next.State == Absent
	}
	execution, found := executions[next.SourcePlanID]
	if !found || execution.Record.TaskID != next.SourceTaskID || execution.Record.Epoch <= 0 ||
		execution.Record.Epoch > math.MaxUint32 || uint32(execution.Record.Epoch) != next.ExecutionEpoch ||
		!slices.Contains(execution.Record.Unit.Writes, next.Target) {
		return false
	}
	switch next.State {
	case Applied:
		return !execution.Record.Unit.Removal && next.Target == execution.Record.Unit.Target &&
			next.Fingerprint == execution.Record.Unit.Fingerprint
	case Absent:
		return execution.Record.Unit.Removal && next.Target == execution.Record.Unit.Target
	case Diverged, Uncertain:
		for _, affected := range next.AffectedWrites {
			if !slices.Contains(execution.Record.Unit.Writes, affected) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func validExecutionAdvance(headTaskID string, current ExecutionRecord, exists bool, next ExecutionRecord) bool {
	if !exists {
		return next.ParentTaskID == headTaskID && next.State == Pending
	}
	if current.EnvironmentID != next.EnvironmentID || current.ParentTaskID != next.ParentTaskID ||
		current.TaskID != next.TaskID || current.PlanID != next.PlanID ||
		current.Unit.Target != next.Unit.Target || current.Unit.Removal != next.Unit.Removal ||
		current.Unit.Fingerprint != next.Unit.Fingerprint ||
		!slices.Equal(current.Unit.Reads, next.Unit.Reads) ||
		!slices.Equal(current.Unit.Writes, next.Unit.Writes) ||
		!slices.Equal(current.Unit.After, next.Unit.After) {
		return false
	}
	switch current.State {
	case Pending:
		return next.ParentTaskID == headTaskID && next.State == Running && next.Epoch > 0
	case Running:
		return next.State == Draining && next.Epoch == current.Epoch
	default:
		return false
	}
}
