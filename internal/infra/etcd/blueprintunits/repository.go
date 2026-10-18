package blueprintunits

import (
	"context"
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
	EnvironmentID  string
	HeadTaskID     string
	HeadRevision   int64
	Epoch          EpochRecord
	EpochRevision  int64
	Desired        *etcdstore.Versioned[DesiredPlan]
	Applied        []etcdstore.Versioned[AppliedRecord]
	Executions     []etcdstore.Versioned[ExecutionRecord]
	ReadRevision   int64
	initialAbsence []etcdstore.Versioned[initialAbsenceRecord]
}

func (repository *Repository) Load(ctx context.Context, environmentID string) (Snapshot, error) {
	return repository.LoadAtRevision(ctx, environmentID, 0)
}

// LoadAtRevision joins native lifecycle receipts to the caller's exact terminal
// transaction view rather than selecting a later Blueprint head.
func (repository *Repository) LoadAtRevision(
	ctx context.Context,
	environmentID string,
	revision int64,
) (Snapshot, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return Snapshot{}, err
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || revision < 0 {
		return Snapshot{}, invalidRecord()
	}
	headKey := blueprints.EnvironmentBlueprintHeadKey(environmentID)
	epochKey := EpochKey(environmentID)
	desiredKey := DesiredPlanKey(environmentID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{headKey, epochKey, desiredKey}, Revision: revision,
	})
	if err != nil {
		return Snapshot{}, err
	}
	if read == nil || read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision ||
		len(read.Values) != 3 || read.Values[0] == nil {
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
	if read.Values[2] != nil {
		desired, decodeErr := DecodeDesiredPlan(read.Values[2].Value)
		if decodeErr != nil || desired.EnvironmentID != environmentID ||
			(snapshot.EpochRevision == 0 || read.Values[2].ModRevision > snapshot.EpochRevision) {
			return Snapshot{}, corruptRecord()
		}
		snapshot.Desired = &etcdstore.Versioned[DesiredPlan]{
			Record: desired, Revision: read.Values[2].ModRevision, ReadRevision: snapshot.ReadRevision,
		}
	}
	if err := repository.loadInitialAbsence(ctx, &snapshot); err != nil {
		return Snapshot{}, err
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
						value.Key != ExecutionKey(environmentID, record.WorkItemID) {
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
	if err := materializeInitialAbsence(&snapshot); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

type AppliedChange struct {
	Target ResourceKey
	Next   *AppliedRecord
}

type ExecutionChange struct {
	WorkItemID string
	Next       *ExecutionRecord
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
	return prepareMutation(snapshot, applied, executions, "")
}

func prepareMutation(
	snapshot Snapshot, applied []AppliedChange, executions []ExecutionChange, nativeOperationID string,
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
	seeded, err := seededTargets(snapshot)
	if err != nil {
		return MutationPlan{}, err
	}
	knownApplied := make(map[ResourceKey]etcdstore.Versioned[AppliedRecord], len(snapshot.Applied))
	for _, versioned := range snapshot.Applied {
		if versioned.Record.EnvironmentID != snapshot.EnvironmentID ||
			validateApplied(versioned.Record) != nil || versioned.Revision < 0 ||
			(versioned.Revision == 0 && (!seeded[versioned.Record.Target] ||
				versioned.Record.State != Absent || versioned.Record.HasEffectSource())) ||
			(versioned.Revision > 0 && !versioned.Record.HasEffectSource()) {
			return MutationPlan{}, invalidRecord()
		}
		if _, duplicate := knownApplied[versioned.Record.Target]; duplicate {
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
		knownExecutions[versioned.Record.WorkItemID] = versioned
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
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: EpochKey(snapshot.EnvironmentID), Value: epochValue},
		},
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
			if !exists || seeded[change.Target] {
				plan.Clear()
				return MutationPlan{}, invalidRecord()
			}
			plan.mutations = append(plan.mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: key})
			continue
		}
		if change.Next.EnvironmentID != snapshot.EnvironmentID || change.Next.Target != change.Target ||
			!validAppliedAdvance(snapshot, exists, *change.Next, knownExecutions, nativeOperationID) {
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
		if ids.Validate(ids.KindWorkItem, change.WorkItemID) != nil || seenExecutions[change.WorkItemID] {
			plan.Clear()
			return MutationPlan{}, invalidRecord()
		}
		seenExecutions[change.WorkItemID] = true
		key := ExecutionKey(snapshot.EnvironmentID, change.WorkItemID)
		current, exists := knownExecutions[change.WorkItemID]
		plan.conditions = append(plan.conditions, etcdstore.Condition{Key: key, ModRevision: current.Revision})
		if change.Next == nil {
			if !exists {
				plan.Clear()
				return MutationPlan{}, invalidRecord()
			}
			plan.mutations = append(plan.mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: key})
			continue
		}
		if change.Next.EnvironmentID != snapshot.EnvironmentID || change.Next.WorkItemID != change.WorkItemID ||
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
	snapshot Snapshot, exists bool, next AppliedRecord, executions map[string]etcdstore.Versioned[ExecutionRecord],
	nativeOperationID string,
) bool {
	if next.MetadataDNS != nil {
		return exists && nativeOperationID == "" && validMetadataDNSReceipt(next) &&
			next.MetadataDNS.AuthoredRevisionID == snapshot.HeadTaskID && next.MetadataDNS.AuthoredRevision == snapshot.HeadRevision &&
			(snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != snapshot.HeadTaskID)
	}
	if next.NativeOperationID != "" {
		return nativeOperationID != "" && next.NativeOperationID == nativeOperationID &&
			validateApplied(next) == nil
	}
	if next.ControllerOperationID != "" {
		if !exists || (next.State != Applied &&
			!((next.Target.Kind == ids.KindScript || next.Target.Kind == ids.KindEnvEntry ||
				next.Target.Kind == ids.KindRoute || next.Target.Kind == ids.KindComponent || next.Target.Kind == ids.KindDNSRecord ||
				validServiceDeclarationAbsence(next)) && next.State == Absent)) ||
			(next.Target.Kind != ids.KindAttach && next.Target.Kind != ids.KindEnvEntry && next.Target.Kind != ids.KindService &&
				next.Target.Kind != ids.KindRoute && next.Target.Kind != ids.KindScript && next.Target.Kind != ids.KindComponent && next.Target.Kind != ids.KindDNSRecord) ||
			snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != snapshot.HeadTaskID ||
			next.ParentTaskID != snapshot.HeadTaskID {
			return false
		}
		for _, unit := range snapshot.Desired.Record.Units {
			if unit.Target == next.Target {
				writeScopeValid := len(unit.Writes) == 1 && unit.Writes[0] == next.Target
				if next.Target.Kind == ids.KindComponent {
					writeScopeValid = validControllerComponentWrites(unit)
				}
				return unit.Removal == (next.State == Absent) && next.Fingerprint == unit.Fingerprint &&
					writeScopeValid
			}
		}
		return false
	}
	if next.Execution == nil || next.Execution.Validate() != nil {
		return false
	}
	execution, found := executions[next.Execution.WorkItemID]
	if !found || execution.Record.TaskID != next.ParentTaskID ||
		execution.Record.TaskID != next.Execution.TaskID || execution.Record.AttemptID != next.Execution.AttemptID ||
		execution.Record.Executor == nil || *execution.Record.Executor != next.Execution.Executor ||
		execution.Record.Input == nil || *execution.Record.Input != next.Execution.Input ||
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
	if validateExecution(next) != nil {
		return false
	}
	if !exists {
		return next.TaskID == headTaskID && next.State == Pending
	}
	if current.EnvironmentID != next.EnvironmentID || current.TaskID != next.TaskID ||
		current.WorkItemID != next.WorkItemID ||
		current.Unit.Target != next.Unit.Target || current.Unit.Removal != next.Unit.Removal ||
		current.Unit.Fingerprint != next.Unit.Fingerprint ||
		!slices.Equal(current.Unit.Reads, next.Unit.Reads) ||
		!slices.Equal(current.Unit.Writes, next.Unit.Writes) ||
		!slices.Equal(current.Unit.After, next.Unit.After) ||
		current.Input != nil && (next.Input == nil || *current.Input != *next.Input) {
		return false
	}
	switch current.State {
	case Pending:
		return next.TaskID == headTaskID && (next.State == Pending ||
			next.State == Running && next.AttemptNumber == 1)
	case Running, Draining:
		if next.AttemptNumber == current.AttemptNumber {
			return next.State == Draining && next.AttemptID == current.AttemptID &&
				next.Executor != nil && current.Executor != nil && *next.Executor == *current.Executor
		}
		return current.AttemptNumber < ^uint64(0) && next.AttemptNumber == current.AttemptNumber+1 &&
			next.AttemptID != current.AttemptID &&
			(next.State == Draining || current.State == Running && next.State == Running && next.TaskID == headTaskID)
	default:
		return false
	}
}
