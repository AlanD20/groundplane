// Package runnerscoped owns the durable handoff between an installed Runner's
// legacy and scoped runtime generations.
package runnerscoped

import (
	"context"
	"encoding/hex"
	"net/netip"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/githubconnection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Phase string

const (
	Pending  Phase = "pending"
	Cutover  Phase = "cutover"
	Complete Phase = "complete"
)

type Migration struct {
	ID                string                         `json:"id"`
	Phase             Phase                          `json:"phase"`
	Legacy            runnerwork.AppliedRuntime      `json:"legacy"`
	Candidate         runnerallocation.RuntimePlan   `json:"candidate"`
	CandidateSoftware buildversion.ContainerIdentity `json:"candidate_software"`
	GitHubID          int64                          `json:"github_id"`
	Request           Request                        `json:"request"`
	AcceptedAt        time.Time                      `json:"accepted_at"`
	CompletedAt       time.Time                      `json:"completed_at,omitzero"`
}

type Request struct {
	ConnectionID string `json:"connection_id"`
	RepositoryID int64  `json:"repository_id"`
	KeyDigest    string `json:"key_digest"`
}

func Key(runnerID string) string { return "/v1/runtime/runners/scoped-migration/" + runnerID }

type Repository struct {
	store   keyvalue.Store
	runners *runners.Reader
}

func NewRepository(store keyvalue.Store) *Repository {
	return &Repository{store: store, runners: runners.NewReader(store)}
}

func (repository *Repository) RegistrationOnlyAt(ctx context.Context, runnerID string, revision int64) (bool, error) {
	if ids.Validate(ids.KindRunner, runnerID) != nil || revision <= 0 {
		return false, errs.New(errs.KindValidationFailed, "Runner registration mode requires a captured identity")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{runnerwork.AppliedKey(runnerID)}, Revision: revision,
	})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return false, errs.New(errs.KindInternal, "Runner registration mode read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return false, errs.New(errs.KindStateConflict, "Runner has no applied registration mode")
	}
	applied, err := runnerwork.DecodeApplied(read.Values[0].Value)
	if err == nil && applied.Plan.RunnerID != runnerID {
		return false, errs.New(errs.KindStateConflict, "Runner registration mode belongs to another Runner")
	}
	return applied.Plan.Egress.RegistrationOnly, err
}

type source struct {
	keyvalue.Versioned[Migration]
	current    runners.RunnerRecord
	owner      runners.RunnerRuntimeOwnershipRecord
	applied    runnerwork.AppliedRuntime
	conditions []keyvalue.Condition
	revision   int64
}

func (repository *Repository) Predecessor(ctx context.Context, runnerID string) (runnerallocation.RuntimePlan, error) {
	view, err := repository.read(ctx, runnerID)
	if err != nil {
		return runnerallocation.RuntimePlan{}, err
	}
	return view.applied.Plan.Clone(), nil
}

func (repository *Repository) CheckCutover(ctx context.Context, runnerID string, revision int64) error {
	view, err := repository.read(ctx, runnerID)
	if err != nil {
		return err
	}
	if revision <= 0 || view.Revision != revision || view.Record.Phase != Cutover ||
		view.Record.Legacy.Plan.Digest() != view.applied.Plan.Digest() {
		return errs.New(errs.KindStateConflict, "Runner migration cannot acquire the captured predecessor")
	}
	return nil
}

func (repository *Repository) Load(
	ctx context.Context, runnerID string,
) (keyvalue.Versioned[Migration], bool, error) {
	if ids.Validate(ids.KindRunner, runnerID) != nil {
		return keyvalue.Versioned[Migration]{}, false, errs.New(errs.KindValidationFailed, "Runner identity is invalid")
	}
	read, err := repository.store.Get(ctx, Key(runnerID))
	if err != nil {
		return keyvalue.Versioned[Migration]{}, false, err
	}
	if read == nil || read.ReadRevision <= 0 {
		return keyvalue.Versioned[Migration]{}, false, errs.New(
			errs.KindInternal,
			"Runner migration read is incomplete",
		)
	}
	if read.Entry == nil {
		return keyvalue.Versioned[Migration]{}, false, nil
	}
	defer clear(read.Entry.Value)
	record, err := Decode(read.Entry.Value, runnerID)
	return keyvalue.Versioned[Migration]{
		Record:       record,
		Revision:     read.Entry.ModRevision,
		ReadRevision: read.ReadRevision,
	}, err == nil, err
}

func (repository *Repository) PrepareBegin(ctx context.Context, runnerID, taskID string, expectedRevision int64,
	candidate runnerallocation.RuntimePlan, software buildversion.ContainerIdentity, githubID int64,
	request Request, provenance []keyvalue.Condition, now time.Time,
) (Migration, workqueue.Change, error) {
	view, err := repository.read(ctx, runnerID)
	if err != nil {
		return Migration{}, workqueue.Change{}, err
	}
	if expectedRevision <= 0 || view.revision != expectedRevision || view.Revision != 0 ||
		view.conditions[5].ModRevision != 0 ||
		candidate.RunnerID != runnerID ||
		githubID <= 0 ||
		len(provenance) == 0 {
		return Migration{}, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Runner is changed, busy, or already scoped",
		)
	}
	conditions, err := mergeConditions(view.conditions, provenance)
	if err != nil {
		return Migration{}, workqueue.Change{}, err
	}
	legacy := view.applied
	legacy.Evidence.DaemonNonce, legacy.Evidence.SocketDevice, legacy.Evidence.SocketInode =
		view.owner.DaemonInstanceNonce, view.owner.SocketDevice, view.owner.SocketInode
	record := Migration{ID: taskID, Phase: Pending, Legacy: legacy, Candidate: candidate.Clone(),
		CandidateSoftware: software, GitHubID: githubID, Request: request, AcceptedAt: now}
	change, err := prepare(record, conditions,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: runnerchecks.ActiveKey(runnerID), Value: []byte(record.ID)})
	return record, change, err
}

func (repository *Repository) PrepareCancel(
	ctx context.Context,
	expected keyvalue.Versioned[Migration],
) (workqueue.Change, error) {
	view, err := repository.read(ctx, expected.Record.Candidate.RunnerID)
	if err != nil {
		return workqueue.Change{}, err
	}
	if view.Revision != expected.Revision || view.Record.ID != expected.Record.ID || view.Record.Phase != Pending {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Runner migration can only cancel before granting host effects",
		)
	}
	// Store proof of predecessor authority until the same CAS releases the lease.
	id := view.Record.Candidate.RunnerID
	return workqueue.Change{Conditions: view.conditions, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationDelete, Key: Key(id)},
		{Type: keyvalue.MutationDelete, Key: runnerchecks.ActiveKey(id)},
	}}, nil
}

func (repository *Repository) Advance(ctx context.Context, runnerID string, revision int64,
	evidence *runnerallocation.RunnerRuntimeEvidence, now time.Time, authorized ...workqueue.Change,
) (keyvalue.Versioned[Migration], error) {
	view, err := repository.read(ctx, runnerID)
	if err != nil {
		return keyvalue.Versioned[Migration]{}, err
	}
	if revision <= 0 || view.Revision != revision || view.Record.Legacy.Plan.Digest() != view.applied.Plan.Digest() {
		return keyvalue.Versioned[Migration]{}, errs.New(
			errs.KindStateConflict,
			"Runner migration no longer owns the predecessor",
		)
	}
	record := view.Record
	switch {
	case record.Phase == Pending && evidence == nil:
		for _, change := range authorized {
			view.conditions, err = mergeConditions(view.conditions, change.Conditions)
			if err != nil {
				return keyvalue.Versioned[Migration]{}, err
			}
		}
		record.Phase = Cutover
		return repository.publish(ctx, record, view.conditions)
	case record.Phase == Cutover && evidence == nil:
		if len(authorized) != 1 || len(authorized[0].Conditions) == 0 || len(authorized[0].Mutations) != 0 {
			return keyvalue.Versioned[Migration]{}, errs.New(
				errs.KindStateConflict,
				"Runner recovery has no current attempt authority",
			)
		}
		conditions, err := mergeConditions(view.conditions, authorized[0].Conditions)
		if err != nil {
			return keyvalue.Versioned[Migration]{}, err
		}
		// A freshly authorized manual Recovery attempt may advance the same
		// idempotent host transition. Ordinary reconciliation is read-only.
		return repository.publish(ctx, record, conditions)
	case record.Phase == Cutover && evidence != nil:
		return repository.complete(ctx, view, *evidence, now, authorized...)
	default:
		return keyvalue.Versioned[Migration]{}, errs.New(
			errs.KindStateConflict,
			"Runner migration transition is invalid",
		)
	}
}

func (repository *Repository) complete(ctx context.Context, view source,
	evidence runnerallocation.RunnerRuntimeEvidence, now time.Time, authorized ...workqueue.Change,
) (keyvalue.Versioned[Migration], error) {
	record, plan := view.Record, view.Record.Candidate
	if !evidence.Valid() || evidence.ContainerID == record.Legacy.Evidence.ContainerID ||
		evidence.Software != record.CandidateSoftware {
		return keyvalue.Versioned[Migration]{}, errs.New(
			errs.KindStateConflict,
			"Runner candidate differs from the sealed migration",
		)
	}
	lifecycle := view.current.RunnerLifecycleRecord
	lifecycle.ContainerID, lifecycle.RuntimeEpoch = evidence.ContainerID, plan.RuntimeEpoch
	lifecycle.Software = evidence.Software
	desired := view.current.Desired
	desired.ImageRef = plan.Container.ImageRef
	desiredValue, err := runners.EncodeRunnerDesiredRecord(desired)
	if err != nil {
		return keyvalue.Versioned[Migration]{}, err
	}
	owner := view.owner
	owner.RuntimeEpoch, owner.CreatedAt = plan.RuntimeEpoch, now
	owner.DaemonInstanceNonce, owner.SocketDevice, owner.SocketInode = evidence.DaemonNonce, evidence.SocketDevice, evidence.SocketInode
	lifecycleValue, err := runners.EncodeRunnerLifecycleRecord(lifecycle)
	if err != nil {
		return keyvalue.Versioned[Migration]{}, err
	}
	ownerValue, err := runners.EncodeRunnerRuntimeOwnership(owner)
	if err != nil {
		return keyvalue.Versioned[Migration]{}, err
	}
	appliedValue, err := runnerwork.EncodeApplied(runnerwork.AppliedRuntime{Plan: plan, Evidence: evidence})
	if err != nil {
		return keyvalue.Versioned[Migration]{}, err
	}
	allocationValue, err := runnerwork.EncodeAllocationPlan(plan)
	if err != nil {
		return keyvalue.Versioned[Migration]{}, err
	}
	id := plan.RunnerID
	record.Phase, record.CompletedAt = Complete, now
	changes := []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: runners.RunnerKey(id), Value: desiredValue},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: runners.RunnerLifecycleKey(id), Value: lifecycleValue},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: runners.RunnerRuntimeOwnershipKey(id), Value: ownerValue},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: runnerwork.AppliedKey(id), Value: appliedValue},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: runnerwork.AllocationPlanKey(id), Value: allocationValue},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: runnerchecks.ActiveKey(id)},
	}
	for _, change := range authorized {
		view.conditions, err = mergeConditions(view.conditions, change.Conditions)
		if err != nil {
			return keyvalue.Versioned[Migration]{}, err
		}
		changes = append(changes, change.Mutations...)
	}
	return repository.publish(ctx, record, view.conditions, changes...)
}

type ActiveAuthority struct {
	GitHubID   int64
	API        netip.AddrPort
	Registry   netip.AddrPort
	Conditions []keyvalue.Condition
}

// Authority proves a completed scoped cutover at the current runtime generation.
// Callers must separately prove that the scoped registry and the particular job
// authorization are available before passing this evidence into admission.
func (repository *Repository) Authority(ctx context.Context, runnerID string) (ActiveAuthority, error) {
	view, err := repository.read(ctx, runnerID)
	if err != nil {
		return ActiveAuthority{}, err
	}
	if view.Record.Phase != Complete || view.conditions[5].ModRevision != 0 ||
		!view.applied.Plan.Egress.ScopedRegistry.IsValid() || view.Record.Candidate.Digest() != view.applied.Plan.Digest() {
		return ActiveAuthority{}, errs.New(
			errs.KindStateConflict,
			"Runner has no verified scoped API and registry runtime",
		)
	}
	egress := view.applied.Plan.Egress
	return ActiveAuthority{
		GitHubID: view.Record.GitHubID, API: egress.ScopedEndpoint, Registry: egress.ScopedRegistry,
		Conditions: view.conditions,
	}, nil
}

func (repository *Repository) read(ctx context.Context, id string) (source, error) {
	if ids.Validate(ids.KindRunner, id) != nil {
		return source{}, errs.New(errs.KindValidationFailed, "Runner identity is invalid")
	}
	keys := []string{
		runners.RunnerKey(id), runners.RunnerLifecycleKey(id), runners.RunnerRuntimeOwnershipKey(id),
		runnerwork.AppliedKey(id), runnerwork.AllocationPlanKey(id), runnerchecks.ActiveKey(id), Key(id),
		deletions.TombstoneKey(string(deletions.DeletionTargetRunner), id),
		workqueue.ClaimKey(workexecution.Claim{ScopeID: id}),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return source{}, err
	}
	if read == nil || len(read.Values) != len(keys) || read.ReadRevision <= 0 {
		return source{}, errs.New(errs.KindInternal, "Runner migration source read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for _, value := range read.Values[:5] {
		if value == nil {
			return source{}, errs.New(errs.KindStateConflict, "Runner has no applied runtime to migrate")
		}
	}
	if read.Values[7] != nil {
		return source{}, errs.New(errs.KindResourceInUse, "Runner has active work or is being removed")
	}
	current, err := runners.DecodeRunnerAggregate(read.Values[0], read.Values[1])
	if err != nil {
		return source{}, err
	}
	owner, err := runners.DecodeRunnerRuntimeOwnership(read.Values[2].Value)
	if err != nil {
		return source{}, err
	}
	applied, err := runnerwork.DecodeApplied(read.Values[3].Value)
	if err != nil {
		return source{}, err
	}
	allocation, err := runnerwork.DecodeAllocationPlan(read.Values[4].Value)
	if err != nil {
		return source{}, err
	}
	if current.ProvisioningState != runners.RunnerProvisioningReady ||
		current.Desired.ID != id || owner.RunnerID != id || applied.Plan.RunnerID != id ||
		owner.RuntimeEpoch != current.RuntimeEpoch || applied.Plan.RuntimeEpoch != current.RuntimeEpoch ||
		current.ContainerID != applied.Evidence.ContainerID || current.Software != applied.Evidence.Software ||
		allocation.IdentityDigest() != applied.Plan.IdentityDigest() {
		return source{}, errs.New(errs.KindStateConflict, "Runner lifecycle and applied execution identity differ")
	}
	parents, err := repository.runners.ResolveRunnerParentsAtRevision(ctx, current.Desired, read.ReadRevision)
	if err != nil {
		return source{}, err
	}
	view := source{current: current, owner: owner, applied: applied, revision: read.Values[0].ModRevision}
	view.ReadRevision = read.ReadRevision
	for index, key := range keys {
		view.conditions = append(
			view.conditions,
			keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])},
		)
	}
	view.conditions = append(view.conditions, parents.AdmissionConditions()...)
	if read.Values[6] != nil {
		view.Revision = read.Values[6].ModRevision
		view.Record, err = Decode(read.Values[6].Value, id)
		if err != nil {
			return source{}, err
		}
		if view.Record.Phase != Complete && (read.Values[5] == nil || string(read.Values[5].Value) != view.Record.ID) {
			return source{}, errs.New(errs.KindStateConflict, "Runner migration lost its active runtime fence")
		}
	}
	if read.Values[8] != nil {
		claim, err := workqueue.DecodeClaimOwner(read.Values[8].Value)
		if err != nil {
			return source{}, err
		}
		if claim.TaskID != view.Record.ID {
			return source{}, errs.New(errs.KindResourceInUse, "Runner has another active execution")
		}
	}
	return view, nil
}

func (repository *Repository) publish(ctx context.Context, record Migration, conditions []keyvalue.Condition,
	mutations ...keyvalue.Mutation,
) (keyvalue.Versioned[Migration], error) {
	change, err := prepare(record, conditions, mutations...)
	if err != nil {
		return keyvalue.Versioned[Migration]{}, err
	}
	result, err := repository.store.Transact(ctx, change.Conditions, change.Mutations)
	if err != nil {
		return keyvalue.Versioned[Migration]{}, err
	}
	if !result.Succeeded {
		return keyvalue.Versioned[Migration]{}, errs.New(
			errs.KindStateConflict,
			"Runner migration lost its captured runtime authority",
		)
	}
	return keyvalue.Versioned[Migration]{Record: record, Revision: result.Revision, ReadRevision: result.Revision}, nil
}

func prepare(
	record Migration,
	conditions []keyvalue.Condition,
	mutations ...keyvalue.Mutation,
) (workqueue.Change, error) {
	id := record.Candidate.RunnerID
	if err := validate(record, id); err != nil {
		return workqueue.Change{}, err
	}
	value, err := recordcodec.Encode("runner_scoped_migration", record)
	if err != nil {
		return workqueue.Change{}, err
	}
	mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: Key(id), Value: value})
	return workqueue.Change{Conditions: conditions, Mutations: mutations}, nil
}

func Decode(value []byte, id string) (Migration, error) {
	if len(value) > 64<<10 {
		return Migration{}, errs.New(errs.KindInternal, "Runner migration exceeds its bound")
	}
	record, err := recordcodec.Decode[Migration](value, "runner_scoped_migration")
	if err != nil {
		return Migration{}, err
	}
	return record, validate(record, id)
}

func validate(record Migration, id string) error {
	legacy, next := record.Legacy.Plan, record.Candidate
	if ids.Validate(ids.KindTask, record.ID) != nil || ids.Validate(ids.KindRunner, id) != nil ||
		ids.Validate(githubconnection.IDKind, record.Request.ConnectionID) != nil ||
		record.Request.RepositoryID <= 0 || !validRequestDigest(record.Request.KeyDigest) ||
		record.CandidateSoftware.Validate() != nil || record.CandidateSoftware.Image != next.Container.ImageRef ||
		record.GitHubID <= 0 || !recordcodec.IsCanonicalUTC(record.AcceptedAt) ||
		legacy.RunnerID != id || next.RunnerID != id || !record.Legacy.Evidence.Valid() ||
		legacy.Egress.ScopedEndpoint.IsValid() || next.Egress.ScopedEndpoint.Port() == 0 ||
		legacy.RuntimeEpoch == ^uint64(0) || next.RuntimeEpoch != legacy.RuntimeEpoch+1 ||
		next.IdentityDigest() != legacy.IdentityDigest() ||
		(record.Phase != Pending && record.Phase != Cutover && record.Phase != Complete) ||
		(record.Phase == Complete && (!recordcodec.IsCanonicalUTC(record.CompletedAt) || record.CompletedAt.Before(record.AcceptedAt))) ||
		(record.Phase != Complete && !record.CompletedAt.IsZero()) {
		return errs.New(errs.KindStateConflict, "Runner migration record is invalid")
	}
	return nil
}

func validRequestDigest(value string) bool {
	digest, err := hex.DecodeString(value)
	return len(digest) == 32 && err == nil && hex.EncodeToString(digest) == value
}

func mergeConditions(base, added []keyvalue.Condition) ([]keyvalue.Condition, error) {
	result := slices.Clone(base)
	for _, condition := range added {
		if condition.Key == "" || condition.Prefix || condition.ModRevision < 0 {
			return nil, errs.New(errs.KindValidationFailed, "Runner registration proof is invalid")
		}
		index := slices.IndexFunc(
			result,
			func(existing keyvalue.Condition) bool { return existing.Key == condition.Key },
		)
		if index < 0 {
			result = append(result, condition)
		} else if result[index] != condition {
			return nil, errs.New(errs.KindStateConflict, "Runner registration proof changed")
		}
	}
	return result, nil
}
