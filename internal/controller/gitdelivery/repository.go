package gitdelivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	root         = "/v1/git-delivery/"
	autoPrefix   = root + "automatic/"
	bindingKind  = "git-delivery-binding"
	runKind      = "git-delivery-run"
	acceptedKind = "git-delivery-request"
)

type repositoryStore interface {
	Get(context.Context, string) (*keyvalue.GetResult, error)
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type Repository struct {
	store repositoryStore
}

func NewRepository(store keyvalue.Store) (*Repository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "GitHub delivery store is required")
	}
	return &Repository{store: store}, nil
}

type BindingSnapshot struct {
	Binding  Binding `json:"binding"`
	Revision int64   `json:"revision"`
}

type RunPage struct {
	Items    []Run  `json:"items"`
	Next     string `json:"next,omitempty"`
	Revision int64  `json:"revision"`
}

type acceptedRun struct {
	ID             string `json:"id"`
	ExpectedCommit string `json:"expected_commit"`
	Automatic      bool   `json:"automatic"`
}

type admission struct {
	binding      BindingSnapshot
	head         int64
	headID       string
	lock         int64
	readRevision int64
	replay       *acceptedRun
}

type runView struct {
	run      Run
	rev      int64
	head     int64
	lock     int64
	lockID   string
	isLatest bool
}

func bindingKey(serviceID string) string { return root + "bindings/" + serviceID }
func headKey(serviceID string) string    { return root + "heads/" + serviceID }
func lockKey(serviceID string) string    { return root + "activation-claims/" + serviceID }
func runKey(id string) string            { return root + "runs/" + id }
func activeKey(id string) string         { return root + "active/" + id }
func historyPrefix(serviceID string) string {
	return root + "history/" + serviceID + "/"
}

func requestKey(serviceID string, input StartInput) string {
	replayKey := "manual:" + input.IdempotencyKey
	if input.Automatic {
		replayKey = "automatic:" + input.IdempotencyKey
	}
	digest := sha256.Sum256([]byte(replayKey))
	return root + "requests/" + serviceID + "/" + hex.EncodeToString(digest[:])
}

func (repository *Repository) bind(
	ctx context.Context,
	binding Binding,
	expectedRevision int64,
	guards []keyvalue.Condition,
	effects []BindingEffects,
) (BindingSnapshot, error) {
	value, err := recordcodec.Encode(bindingKind, binding)
	if err != nil {
		return BindingSnapshot{}, err
	}
	bindingAddress := bindingKey(binding.ServiceID)
	conditions := []keyvalue.Condition{
		condition(bindingAddress, expectedRevision),
		condition(lockKey(binding.ServiceID), 0),
	}
	conditions = append(conditions, guards...)
	mutations := []keyvalue.Mutation{put(bindingAddress, value)}
	if binding.Automatic {
		mutations = append(mutations, put(autoPrefix+binding.ServiceID, []byte(binding.ServiceID)))
	} else if expectedRevision != 0 {
		mutations = append(mutations, remove(autoPrefix+binding.ServiceID))
	}
	webhookTargets, err := repository.pushTargetChanges(ctx, binding, expectedRevision)
	if err != nil {
		return BindingSnapshot{}, err
	}
	mutations = append(mutations, webhookTargets...)
	conditions, mutations, err = applyEffects(binding, conditions, mutations, effects)
	if err != nil {
		return BindingSnapshot{}, err
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return BindingSnapshot{}, err
	}
	if !result.Succeeded {
		return BindingSnapshot{}, errs.New(errs.KindStateConflict, "GitHub binding changed or a deployment is active")
	}
	return BindingSnapshot{Binding: binding, Revision: result.Revision}, nil
}

func (repository *Repository) binding(ctx context.Context, serviceID string) (BindingSnapshot, error) {
	result, err := repository.store.Get(ctx, bindingKey(serviceID))
	if err != nil {
		return BindingSnapshot{}, err
	}
	return decodeBinding(result.Entry, serviceID)
}

func (repository *Repository) admission(ctx context.Context, serviceID string, input StartInput) (admission, error) {
	keys := []string{bindingKey(serviceID), headKey(serviceID), lockKey(serviceID)}
	if !input.Automatic {
		keys = append(keys, requestKey(serviceID, input))
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return admission{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return admission{}, errs.New(errs.KindInternal, "GitHub deployment admission snapshot is incomplete")
	}
	if !input.Automatic && read.Values[3] != nil {
		record, err := recordcodec.Decode[acceptedRun](read.Values[3].Value, acceptedKind)
		if err != nil {
			return admission{}, err
		}
		if record.ExpectedCommit != input.ExpectedCommit || record.Automatic != input.Automatic {
			return admission{}, errs.New(
				errs.KindIdempotencyMismatch,
				"GitHub deployment request differs from the accepted inputs",
			)
		}
		return admission{replay: &record}, nil
	}
	snapshot, err := decodeBinding(read.Values[0], serviceID)
	if err != nil {
		return admission{}, err
	}
	return admission{
		binding: snapshot, head: revision(read.Values[1]), headID: identity(read.Values[1]),
		lock: revision(read.Values[2]), readRevision: read.ReadRevision,
	}, nil
}

// latestIntent reads the Run selected by the exact admission snapshot. The
// head's modification revision identifies a transition even when a SHA recurs.
func (repository *Repository) latestIntent(ctx context.Context, captured admission) (Run, error) {
	if captured.head == 0 {
		return Run{}, nil
	}
	if ids.Validate(ids.KindOperation, captured.headID) != nil {
		return Run{}, errs.New(errs.KindInternal, "GitHub deployment head has an invalid Run ID")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{runKey(captured.headID)}, Revision: captured.readRevision,
	})
	if err != nil {
		return Run{}, err
	}
	if read == nil || read.ReadRevision != captured.readRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return Run{}, errs.New(errs.KindInternal, "GitHub deployment head has no captured Run")
	}
	run, err := decodeRun(read.Values[0], captured.headID)
	if err != nil {
		return Run{}, err
	}
	if run.Binding.Target != captured.binding.Binding.Target {
		return Run{}, errs.New(errs.KindInternal, "GitHub deployment head has an inconsistent owner")
	}
	return run, nil
}

func (repository *Repository) createRun(
	ctx context.Context,
	captured admission,
	run Run,
	input StartInput,
	guards []keyvalue.Condition,
	effects []RunEffects,
) error {
	if err := run.validate(); err != nil {
		return err
	}
	runValue, err := recordcodec.Encode(runKind, run)
	if err != nil {
		return err
	}
	acceptedValue, err := recordcodec.Encode(acceptedKind, acceptedRun{
		ID: run.ID, ExpectedCommit: input.ExpectedCommit, Automatic: input.Automatic,
	})
	if err != nil {
		return err
	}
	serviceID := run.Binding.ServiceID
	acceptedAddress := requestKey(serviceID, input)
	indexAddress := historyPrefix(serviceID) + run.ID
	conditions := []keyvalue.Condition{
		condition(bindingKey(serviceID), captured.binding.Revision),
		condition(headKey(serviceID), captured.head),
		condition(lockKey(serviceID), 0),
		condition(acceptedAddress, 0), condition(runKey(run.ID), 0),
		condition(indexAddress, 0), condition(activeKey(run.ID), 0),
	}
	conditions = append(conditions, guards...)
	mutations := []keyvalue.Mutation{
		put(acceptedAddress, acceptedValue),
		put(runKey(run.ID), runValue),
		put(headKey(serviceID), []byte(run.ID)),
		put(indexAddress, []byte(run.ID)),
		put(activeKey(run.ID), []byte(run.ID)),
	}
	conditions, mutations, err = applyEffects(run, conditions, mutations, effects)
	if err != nil {
		return err
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindExecutionWriteContention, "GitHub deployment intent changed during acceptance")
	}
	return nil
}

func (repository *Repository) run(ctx context.Context, id string) (runView, error) {
	result, err := repository.store.Get(ctx, runKey(id))
	if err != nil {
		return runView{}, err
	}
	run, err := decodeRun(result.Entry, id)
	if err != nil {
		return runView{}, err
	}
	serviceID := run.Binding.ServiceID
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{bindingKey(serviceID), headKey(serviceID), lockKey(serviceID)}, Revision: result.ReadRevision,
	})
	if err != nil {
		return runView{}, err
	}
	return runView{
		run: run, rev: result.Entry.ModRevision,
		head: revision(read.Values[1]), lock: revision(read.Values[2]), lockID: identity(read.Values[2]),
		isLatest: identity(read.Values[1]) == id && revision(read.Values[0]) == run.BindingRevision,
	}, nil
}

func (repository *Repository) updateRun(
	ctx context.Context,
	previous runView,
	updated Run,
	guards []keyvalue.Condition,
	effects ...RunEffects,
) (Run, error) {
	serviceID := previous.run.Binding.ServiceID
	conditions := []keyvalue.Condition{
		condition(runKey(updated.ID), previous.rev),
		condition(headKey(serviceID), previous.head),
		condition(lockKey(serviceID), previous.lock),
	}
	if updated.Phase != Superseded {
		conditions = append(conditions, condition(bindingKey(serviceID), previous.run.BindingRevision))
	}
	conditions = append(conditions, guards...)
	value, err := recordcodec.Encode(runKind, updated)
	if err != nil {
		return Run{}, err
	}
	if err := updated.validate(); err != nil {
		return Run{}, err
	}
	mutations := []keyvalue.Mutation{put(runKey(updated.ID), value)}
	if updated.Phase.terminal() {
		mutations = append(mutations, remove(activeKey(updated.ID)))
	}
	if previous.run.Phase.activationOwned() && !updated.Phase.activationOwned() {
		if previous.lockID != updated.ID {
			return Run{}, errs.New(errs.KindInternal, "GitHub activation claim has changed ownership")
		}
		mutations = append(mutations, remove(lockKey(serviceID)))
	} else if !previous.run.Phase.activationOwned() && updated.Phase.activationOwned() {
		if previous.lock != 0 {
			return Run{}, errs.New(errs.KindStateConflict, "another GitHub activation is in progress")
		}
		mutations = append(mutations, put(lockKey(serviceID), []byte(updated.ID)))
	}
	conditions, mutations, err = applyEffects(updated, conditions, mutations, effects)
	if err != nil {
		return Run{}, err
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return Run{}, err
	}
	if !result.Succeeded {
		return Run{}, errs.New(errs.KindExecutionWriteContention, "GitHub deployment changed while advancing")
	}
	return updated, nil
}

func (repository *Repository) get(ctx context.Context, id string) (Run, error) {
	result, err := repository.store.Get(ctx, runKey(id))
	if err != nil {
		return Run{}, err
	}
	return decodeRun(result.Entry, id)
}

func (repository *Repository) list(
	ctx context.Context,
	serviceID, after string,
	limit, revision int64,
) (RunPage, error) {
	prefix := historyPrefix(serviceID)
	request := keyvalue.RangeRequest{Prefix: prefix, Limit: limit, Descending: true, Revision: revision}
	if after != "" {
		request.StartExclusive = prefix + after
	}
	index, err := repository.store.Range(ctx, request)
	if err != nil {
		return RunPage{}, err
	}
	page := RunPage{Items: make([]Run, 0, len(index.Values)), Revision: index.ReadRevision}
	if len(index.Values) == 0 {
		return page, nil
	}
	addresses := make([]string, len(index.Values))
	for i, value := range index.Values {
		id := string(value.Value)
		if ids.Validate(ids.KindOperation, id) != nil || value.Key != prefix+id {
			return RunPage{}, errs.New(errs.KindInternal, "invalid GitHub deployment history index")
		}
		addresses[i] = runKey(id)
	}
	runs, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: addresses, Revision: index.ReadRevision})
	if err != nil {
		return RunPage{}, err
	}
	for i, value := range runs.Values {
		id := string(index.Values[i].Value)
		run, err := decodeRun(value, id)
		if err != nil {
			return RunPage{}, err
		}
		if run.Binding.ServiceID != serviceID {
			return RunPage{}, errs.New(errs.KindInternal, "GitHub deployment history has an inconsistent owner")
		}
		page.Items = append(page.Items, run)
	}
	if index.More {
		page.Next = page.Items[len(page.Items)-1].ID
	}
	return page, nil
}

func (repository *Repository) active(ctx context.Context, after string) ([]string, string, error) {
	return repository.index(ctx, root+"active/", ids.KindOperation, after, 64)
}

func (repository *Repository) index(
	ctx context.Context,
	prefix string,
	kind ids.Kind,
	after string,
	limit int64,
) ([]string, string, error) {
	request := keyvalue.RangeRequest{Prefix: prefix, Limit: limit}
	if after != "" {
		request.StartExclusive = prefix + after
	}
	result, err := repository.store.Range(ctx, request)
	if err != nil {
		return nil, "", err
	}
	found := make([]string, 0, len(result.Values))
	for _, value := range result.Values {
		id := strings.TrimPrefix(value.Key, prefix)
		if ids.Validate(kind, id) != nil || id != string(value.Value) {
			return nil, "", errs.New(errs.KindInternal, "invalid GitHub delivery work index")
		}
		found = append(found, id)
	}
	if result.More && len(found) == 0 {
		return nil, "", errs.New(errs.KindInternal, "GitHub delivery work index did not advance")
	}
	if result.More {
		return found, found[len(found)-1], nil
	}
	return found, "", nil
}

func decodeBinding(record *keyvalue.KeyValue, serviceID string) (BindingSnapshot, error) {
	if record == nil {
		return BindingSnapshot{}, errs.New(errs.KindRequestNotFound, "GitHub deployment binding not found")
	}
	binding, err := recordcodec.Decode[Binding](record.Value, bindingKind)
	if err != nil {
		return BindingSnapshot{}, err
	}
	if binding.ServiceID != serviceID || binding.Validate() != nil {
		return BindingSnapshot{}, errs.New(errs.KindInternal, "invalid stored GitHub deployment binding")
	}
	return BindingSnapshot{Binding: binding, Revision: record.ModRevision}, nil
}

func decodeRun(record *keyvalue.KeyValue, id string) (Run, error) {
	if record == nil {
		return Run{}, errs.New(errs.KindRequestNotFound, "GitHub deployment not found")
	}
	run, err := recordcodec.Decode[Run](record.Value, runKind)
	if err != nil {
		return Run{}, err
	}
	if run.ID != id {
		return Run{}, errs.New(errs.KindInternal, "unexpected GitHub deployment record identity")
	}
	return run, run.validate()
}

func applyEffects[T any](
	value T,
	conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
	effects []func(T) ([]keyvalue.Condition, []keyvalue.Mutation, error),
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	for _, makeEffects := range effects {
		additionalConditions, additionalMutations, err := makeEffects(value)
		if err != nil {
			return nil, nil, err
		}
		conditions = append(conditions, additionalConditions...)
		mutations = append(mutations, additionalMutations...)
	}
	unique := make(map[string]keyvalue.Condition, len(conditions))
	deduplicated := conditions[:0]
	for _, guard := range conditions {
		if previous, exists := unique[guard.Key]; exists {
			if previous != guard {
				return nil, nil, errs.New(
					errs.KindStateConflict,
					"GitHub deployment authority changed during validation",
				)
			}
			continue
		}
		unique[guard.Key] = guard
		deduplicated = append(deduplicated, guard)
	}
	return deduplicated, mutations, nil
}

func identity(entry *keyvalue.KeyValue) string {
	if entry == nil {
		return ""
	}
	return string(entry.Value)
}

func revision(entry *keyvalue.KeyValue) int64 {
	if entry == nil {
		return 0
	}
	return entry.ModRevision
}

func condition(key string, revision int64) keyvalue.Condition {
	return keyvalue.Condition{Key: key, ModRevision: revision}
}

func put(key string, value []byte) keyvalue.Mutation {
	return keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value}
}

func remove(key string) keyvalue.Mutation {
	return keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key}
}
