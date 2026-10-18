package resolverwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// State is a read-only resource snapshot. Applied authority outlives Tasks;
// only unresolved work requires retained executable input and its native root.
type State struct {
	HostID          string
	Applied         *Applied
	AppliedRevision int64
	IntentRevision  int64
	Request         *Request
	RequestRevision int64
	CurrentWorkID   string
	ClaimOwner      *workqueue.ClaimOwner
	Latest          *workexecution.Item
	Pending         *CapturedState
	ActiveItem      *workexecution.Item
	Active          *CapturedState
	Conditions      []keyvalue.Condition
	ReadRevision    int64
}

type CapturedState struct {
	Input   resolverexecution.Input
	Capture Capture
}

// Presentation reads only resource heads and claim identity. It does not load
// a Task or executable input merely to display the last applied DNS answers.
func ReadLocalResourceState(ctx context.Context, store hostbindings.Reader, componentID string, revision int64) (State, error) {
	if ids.Validate(ids.KindComponent, componentID) != nil {
		return State{}, invalid("DNS resource read requires its Component")
	}
	host, err := hostbindings.ReadLocalIdentity(ctx, store, revision)
	if err != nil {
		return State{}, err
	}
	guard := keyvalue.Condition{Key: hostbindings.LocalIdentityKey(), ModRevision: host.Revision}
	if host.Revision == 0 {
		return State{ReadRevision: host.ReadRevision, Conditions: []keyvalue.Condition{guard}}, nil
	}
	state, err := ReadResourceState(ctx, store, host.Record, componentID, host.ReadRevision)
	state.Conditions = append(state.Conditions, guard)
	return state, err
}

func ReadLocalState(ctx context.Context, store hostbindings.Reader, componentID string, revision int64) (State, error) {
	state, err := ReadLocalResourceState(ctx, store, componentID, revision)
	if err != nil || state.HostID == "" {
		return state, err
	}
	return readExecutionState(ctx, store, componentID, state)
}

func ReadState(ctx context.Context, store hostbindings.Reader, hostID, componentID string, revision int64) (State, error) {
	state, err := ReadResourceState(ctx, store, hostID, componentID, revision)
	if err != nil {
		return State{}, err
	}
	return readExecutionState(ctx, store, componentID, state)
}

func ReadResourceState(ctx context.Context, store hostbindings.Reader, hostID, componentID string, revision int64) (State, error) {
	if ctx == nil || store == nil || ids.Validate(ids.KindHost, hostID) != nil || ids.Validate(ids.KindComponent, componentID) != nil || revision < 0 {
		return State{}, invalid("DNS resource snapshot requires its host and Component")
	}
	keys := []string{AppliedKey(hostID, componentID), IntentKey(hostID, componentID), RequestKey(hostID, componentID),
		workqueue.ClaimKey(workexecution.Claim{HostID: hostID, ScopeID: componentID})}
	read, err := readStateKeys(ctx, store, keys, revision)
	if err != nil {
		return State{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	state := State{HostID: hostID, ReadRevision: read.ReadRevision, AppliedRevision: keyvalue.RevisionOf(read.Values[0]),
		IntentRevision: keyvalue.RevisionOf(read.Values[1]), RequestRevision: keyvalue.RevisionOf(read.Values[2])}
	for i, key := range keys {
		state.Conditions = append(state.Conditions, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[i])})
	}
	if value := read.Values[0]; value != nil {
		applied, err := DecodeApplied(value.Value)
		if err != nil {
			return State{}, err
		}
		if applied.HostID != hostID || applied.ComponentID != componentID {
			return State{}, invalid("DNS applied snapshot belongs to another resource")
		}
		state.Applied = &applied
	}
	if claim := read.Values[3]; claim != nil {
		owner, err := workqueue.DecodeClaimOwner(claim.Value)
		if err != nil {
			return State{}, err
		}
		state.ClaimOwner = &owner
	}
	if (read.Values[1] == nil) != (read.Values[2] == nil) {
		return State{}, invalid("DNS latest request and work identity disagree")
	}
	if read.Values[1] == nil {
		return state, nil
	}
	workID := string(read.Values[1].Value)
	request, err := decodeRequest(read.Values[2].Value)
	if err != nil {
		return State{}, err
	}
	if request.HostID != hostID || request.ComponentID != componentID || request.WorkID != workID {
		return State{}, invalid("DNS request belongs to another current action")
	}
	state.Request, state.CurrentWorkID = &request, workID
	return state, nil
}

// Actions additionally inspect both the latest input and the older execution
// still holding the resource. These comparisons are not presentation fallbacks.
func readExecutionState(ctx context.Context, store hostbindings.Reader, componentID string, state State) (State, error) {
	if state.ClaimOwner != nil {
		active, captured, guards, err := readActiveState(ctx, store, state.HostID, componentID, *state.ClaimOwner, state.ReadRevision)
		if err != nil {
			return State{}, err
		}
		state.ActiveItem, state.Active = &active, captured
		state.Conditions = append(state.Conditions, guards...)
	}
	if state.CurrentWorkID == "" {
		return state, nil
	}
	key := workqueue.ItemKey(state.CurrentWorkID)
	work, err := readStateKeys(ctx, store, []string{key}, state.ReadRevision)
	if err != nil {
		return State{}, err
	}
	defer keyvalue.ClearValues(work.Values)
	state.Conditions = append(state.Conditions, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(work.Values[0])})
	if work.Values[0] == nil {
		return state, nil
	} // settled history was collected
	item, err := workqueue.DecodeItem(work.Values[0].Value)
	if err != nil {
		return State{}, err
	}
	if item.ID != state.CurrentWorkID || item.Operation != workexecution.Resolver || item.TargetID != componentID ||
		item.Placement != (workexecution.Placement{Executor: workexecution.Agent, HostID: state.HostID}) {
		return State{}, invalid("DNS current work belongs to another resource")
	}
	state.Latest = &item
	if item.Phase == workexecution.Settled {
		return state, nil
	}
	if state.ActiveItem != nil && state.ActiveItem.ID == item.ID {
		state.Pending = state.Active
		return state, nil
	}
	captured, guards, err := readCapturedState(ctx, store, item, state.ReadRevision)
	if err != nil {
		return State{}, err
	}
	state.Pending = &captured
	state.Conditions = append(state.Conditions, guards...)
	return state, nil
}

func readCapturedState(ctx context.Context, store hostbindings.Reader, item workexecution.Item, revision int64) (CapturedState, []keyvalue.Condition, error) {
	if item.Input == nil {
		return CapturedState{}, nil, invalid("pending DNS work has no immutable input")
	}
	keys := []string{workqueue.TaskKey(item.TaskID), InputKey(item.ID), CaptureKey(item.ID)}
	read, err := readStateKeys(ctx, store, keys, revision)
	if err != nil {
		return CapturedState{}, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	for _, value := range read.Values {
		if value == nil {
			return CapturedState{}, nil, invalid("pending DNS work lost its captured authority")
		}
	}
	task, err := workqueue.DecodeTask(read.Values[0].Value)
	if err != nil {
		return CapturedState{}, nil, err
	}
	input, reference, err := resolverexecution.Decode(item.Input.ID, read.Values[1].Value)
	if err != nil {
		return CapturedState{}, nil, err
	}
	if err := matchesWork(input, reference, task, item); err != nil {
		return CapturedState{}, nil, err
	}
	capture, err := decodeCapture(read.Values[2].Value, input)
	if err != nil {
		return CapturedState{}, nil, err
	}
	if task.CollectionStartedAt != nil {
		return CapturedState{}, nil, invalid("pending DNS work cannot be collected")
	}
	var guards []keyvalue.Condition
	for i, key := range keys {
		guards = append(guards, keyvalue.Condition{Key: key, ModRevision: read.Values[i].ModRevision})
	}
	return CapturedState{Input: input, Capture: capture}, guards, nil
}

func readStateKeys(ctx context.Context, store hostbindings.Reader, keys []string, revision int64) (*keyvalue.GetManyResult, error) {
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, invalid("DNS resource snapshot is incomplete")
	}
	for i, value := range read.Values {
		if value != nil && (value.Key != keys[i] || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision) {
			keyvalue.ClearValues(read.Values)
			return nil, invalid("DNS resource snapshot revision or key is corrupt")
		}
	}
	return read, nil
}
