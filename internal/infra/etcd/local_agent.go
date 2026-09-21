package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	localagentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

type localAgentRepositoryStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

type LocalAgentRepository struct {
	store localAgentRepositoryStore
}

type localAgentEvidence struct {
	record          localagentrecord.LocalAgentRecord
	primaryRevision int64
	readRevision    int64
	singleton       *etcdstore.KeyValue
	primary         *etcdstore.KeyValue
	owner           *etcdstore.KeyValue
	config          *etcdstore.KeyValue
	token           *etcdstore.KeyValue
	digest          *etcdstore.KeyValue
}

func NewLocalAgentRepository(store etcdstore.Store) (*LocalAgentRepository, error) {
	return newLocalAgentRepository(store)
}

func newLocalAgentRepository(store localAgentRepositoryStore) (*LocalAgentRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "local Agent store is required")
	}
	return &LocalAgentRepository{store: store}, nil
}

func (repository *LocalAgentRepository) CreateSingleton(
	ctx context.Context,
	record localagentrecord.LocalAgentRecord,
) (etcdstore.Versioned[localagentrecord.LocalAgentRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	if record.Phase != localagentrecord.LocalAgentPhaseProvisioning {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindValidationFailed,
			"local Agent must be created in provisioning phase",
		)
	}
	if record.TokenUpdatedAt.IsZero() {
		record.TokenUpdatedAt = record.CreatedAt
	}
	if err := localagentrecord.ValidateLocalAgentRecord(record); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	primaryValue, configValue, tokenValue, reference, err := localagentrecord.EncodeLocalAgentValues(record)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	defer clear(primaryValue)
	defer clear(configValue)
	defer clear(tokenValue)
	defer clear(reference)
	keys := localagentrecord.LocalAgentKeys(record.ID, record.TokenDigest)
	conditions := make([]etcdstore.Condition, len(keys))
	mutations := make([]etcdstore.Mutation, len(keys))
	values := [][]byte{reference, primaryValue, reference, configValue, tokenValue, reference}
	for index, key := range keys {
		conditions[index] = etcdstore.Condition{Key: key}
		mutations[index] = etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: values[index]}
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	clearKeyValues(result.FailureReads)
	if !result.Succeeded {
		if len(result.FailureReads) == len(conditions) && result.FailureReads[0] != nil {
			return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(errs.KindStateConflict, "the local Agent already exists")
		}
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindInternal,
			"local Agent creation collided with durable state",
		)
	}
	return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{
		Record: localagentrecord.CloneLocalAgentRecord(record), Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *LocalAgentRepository) GetSingleton(
	ctx context.Context,
) (etcdstore.Versioned[localagentrecord.LocalAgentRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	evidence, err := repository.readSingleton(ctx)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{
		Record: evidence.record, Revision: evidence.primaryRevision,
		ReadRevision: evidence.readRevision,
	}, nil
}

func (repository *LocalAgentRepository) MarkReady(
	ctx context.Context,
	agentID string,
	generation uint64,
	revision int64,
	readyAt time.Time,
) (etcdstore.Versioned[localagentrecord.LocalAgentRecord], error) {
	if err := recordcodec.ValidateTimestamp("local Agent ready_at", readyAt); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	return repository.transitionPhase(
		ctx,
		agentID,
		generation,
		revision,
		localagentrecord.LocalAgentPhaseProvisioning,
		localagentrecord.LocalAgentPhaseReady,
		false,
		readyAt,
	)
}

// ReplaceGeneration atomically rotates the complete authenticated runtime
// identity while retaining the stable Agent aggregate and first Ready time.
func (repository *LocalAgentRepository) ReplaceGeneration(
	ctx context.Context,
	current etcdstore.Versioned[localagentrecord.LocalAgentRecord],
	image string,
	encryptedToken []byte,
	tokenDigest string,
	updatedAt time.Time,
) (etcdstore.Versioned[localagentrecord.LocalAgentRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision ||
		current.Record.ID == "" || !imageref.IsDigestPinned(image) ||
		len(encryptedToken) == 0 || !localagentrecord.ValidLocalAgentDigest(tokenDigest) ||
		current.Record.Generation == ^uint64(0) {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindValidationFailed,
			"local Agent replacement identity is invalid",
		)
	}
	if err := recordcodec.ValidateTimestamp("local Agent token updated_at", updatedAt); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	evidence, err := repository.readSingleton(ctx)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	if evidence.record.ID != current.Record.ID ||
		evidence.record.Generation != current.Record.Generation ||
		evidence.record.Image != current.Record.Image ||
		evidence.record.Phase != current.Record.Phase ||
		evidence.primaryRevision != current.Revision {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindStateConflict,
			"local Agent generation or revision changed",
		)
	}
	if evidence.record.Phase != localagentrecord.LocalAgentPhaseReady &&
		evidence.record.Phase != localagentrecord.LocalAgentPhaseUpdating {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindStateConflict,
			"local Agent is not replaceable",
		)
	}
	if evidence.digest == nil || tokenDigest == evidence.record.TokenDigest ||
		updatedAt.Before(evidence.record.TokenUpdatedAt) {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindStateConflict,
			"local Agent replacement token is not a new generation",
		)
	}

	replacement := localagentrecord.CloneLocalAgentRecord(evidence.record)
	replacement.Image = image
	replacement.Generation++
	replacement.Phase = localagentrecord.LocalAgentPhaseUpdating
	replacement.EncryptedToken = append([]byte(nil), encryptedToken...)
	replacement.TokenDigest = tokenDigest
	replacement.TokenUpdatedAt = updatedAt
	primaryValue, configValue, tokenValue, reference, err := localagentrecord.EncodeLocalAgentValues(replacement)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	defer clear(primaryValue)
	defer clear(configValue)
	defer clear(tokenValue)
	defer clear(reference)
	newDigestKey := localagentrecord.LocalAgentDigestKey(tokenDigest)
	result, err := repository.store.Transact(ctx, []etcdstore.Condition{
		{Key: localagentrecord.LocalAgentSingletonKey, ModRevision: evidence.singleton.ModRevision},
		{Key: localagentrecord.LocalAgentPrimaryKey(replacement.ID), ModRevision: evidence.primary.ModRevision},
		{Key: localagentrecord.LocalAgentConfigKey(replacement.ID), ModRevision: evidence.config.ModRevision},
		{Key: localagentrecord.LocalAgentTokenKey(replacement.ID), ModRevision: evidence.token.ModRevision},
		{Key: evidence.digest.Key, ModRevision: evidence.digest.ModRevision},
		{Key: newDigestKey},
	}, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: localagentrecord.LocalAgentPrimaryKey(replacement.ID), Value: primaryValue},
		{Type: etcdstore.MutationPut, Key: localagentrecord.LocalAgentConfigKey(replacement.ID), Value: configValue},
		{Type: etcdstore.MutationPut, Key: localagentrecord.LocalAgentTokenKey(replacement.ID), Value: tokenValue},
		{Type: etcdstore.MutationDelete, Key: evidence.digest.Key},
		{Type: etcdstore.MutationPut, Key: newDigestKey, Value: reference},
	})
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	clearKeyValues(result.FailureReads)
	if !result.Succeeded {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindStateConflict,
			"local Agent replacement state changed",
		)
	}
	return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *LocalAgentRepository) MarkReplacementReady(
	ctx context.Context,
	agentID string,
	generation uint64,
	revision int64,
) (etcdstore.Versioned[localagentrecord.LocalAgentRecord], error) {
	return repository.transitionPhase(
		ctx,
		agentID,
		generation,
		revision,
		localagentrecord.LocalAgentPhaseUpdating,
		localagentrecord.LocalAgentPhaseReady,
		true,
		time.Time{},
	)
}

// UpdateConfigIdempotent atomically replaces the generation-bound config
// singleton and commits the exact completed replay marker. The lifecycle
// primary and singleton pointer fence deletion or replacement of the Agent.
func (repository *LocalAgentRepository) UpdateConfigIdempotent(
	ctx context.Context,
	current etcdstore.Versioned[localagentrecord.LocalAgentRecord],
	config localagentrecord.LocalAgentConfig,
	marker idempotencyrecord.IdempotencyMarker,
) (etcdstore.Versioned[localagentrecord.LocalAgentRecord], IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, IdempotencyTransactionResult{}, err
	}
	if err := localagentrecord.ValidateLocalAgentConfig(config); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, IdempotencyTransactionResult{}, err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision ||
		current.Record.ID == "" || marker.Kind != idempotencyrecord.IdempotencyMarkerDirect ||
		marker.State != idempotencyrecord.IdempotencyMarkerCompleted || marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopePlatform ||
		marker.Locator.ScopeID != "-" || marker.Locator.Method != "PUT" ||
		marker.Locator.Route != "/agents/{id}/config" {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"local Agent config mutation identity is invalid",
		)
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, IdempotencyTransactionResult{}, err
	}
	evidence, err := repository.readSingleton(ctx)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, IdempotencyTransactionResult{}, err
	}
	if evidence.record.ID != current.Record.ID || evidence.record.Generation != current.Record.Generation ||
		evidence.primaryRevision != current.Revision {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"local Agent generation or revision changed",
		)
	}
	if evidence.record.Phase == localagentrecord.LocalAgentPhaseDeleting {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"deleting local Agent config cannot be changed",
		)
	}
	replacement := localagentrecord.CloneLocalAgentRecord(evidence.record)
	replacement.Config = localagentrecord.CloneLocalAgentConfig(config)
	configValue, err := localagentrecord.EncodeLocalAgentConfig(replacement)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, IdempotencyTransactionResult{}, err
	}
	defer clear(configValue)
	plan, err := newIdempotencyMutationPlan(
		[]etcdstore.Condition{
			{Key: localagentrecord.LocalAgentSingletonKey, ModRevision: evidence.singleton.ModRevision},
			{Key: localagentrecord.LocalAgentPrimaryKey(current.Record.ID), ModRevision: evidence.primary.ModRevision},
			{Key: localagentrecord.LocalAgentConfigKey(current.Record.ID), ModRevision: evidence.config.ModRevision},
		},
		[]etcdstore.Mutation{{
			Type: etcdstore.MutationPut, Key: localagentrecord.LocalAgentConfigKey(current.Record.ID), Value: configValue,
		}},
		classifyLocalAgentConfigConflict(current.Record.ID),
	)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, IdempotencyTransactionResult{}, err
	}
	result, err := idempotency.Apply(ctx, marker, plan)
	return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{
		Record: replacement, Revision: evidence.primaryRevision, ReadRevision: result.revision,
	}, result, err
}

func classifyLocalAgentConfigConflict(agentID string) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != 3 {
			return errs.New(errs.KindInternal, "local Agent config compare evidence is incomplete")
		}
		if values[0] == nil || values[1] == nil {
			return errs.New(errs.KindAgentNotFound, "local Agent was not found")
		}
		if values[2] == nil {
			return errs.New(errs.KindInternal, "local Agent config record is missing")
		}
		resolvedID, err := localagentrecord.DecodeLocalAgentReference(values[0].Value)
		if err != nil {
			return err
		}
		if resolvedID != agentID {
			return errs.New(errs.KindAgentNotFound, "local Agent was not found")
		}
		return errs.New(errs.KindStateConflict, "local Agent config changed")
	}
}

func (repository *LocalAgentRepository) BeginDelete(
	ctx context.Context,
	agentID string,
	generation uint64,
	revision int64,
) (etcdstore.Versioned[localagentrecord.LocalAgentRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	evidence, err := repository.readSingleton(ctx)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	if evidence.record.ID != agentID || evidence.record.Generation != generation ||
		evidence.primaryRevision != revision {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindStateConflict,
			"local Agent generation or revision changed",
		)
	}
	if evidence.record.Phase == localagentrecord.LocalAgentPhaseDeleting {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{
			Record: evidence.record, Revision: evidence.primaryRevision,
			ReadRevision: evidence.readRevision,
		}, nil
	}
	if evidence.digest == nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindInternal,
			"active local Agent is missing its credential index",
		)
	}
	replacement := localagentrecord.CloneLocalAgentRecord(evidence.record)
	replacement.Phase = localagentrecord.LocalAgentPhaseDeleting
	replacement.TokenDigest = ""
	primaryValue, err := localagentrecord.EncodeLocalAgentPrimary(replacement)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	defer clear(primaryValue)
	result, err := repository.store.Transact(ctx, []etcdstore.Condition{
		{Key: localagentrecord.LocalAgentPrimaryKey(agentID), ModRevision: evidence.primary.ModRevision},
		{Key: evidence.digest.Key, ModRevision: evidence.digest.ModRevision},
	}, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: localagentrecord.LocalAgentPrimaryKey(agentID), Value: primaryValue},
		{Type: etcdstore.MutationDelete, Key: evidence.digest.Key},
	})
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	clearKeyValues(result.FailureReads)
	if !result.Succeeded {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(errs.KindStateConflict, "local Agent deletion state changed")
	}
	return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *LocalAgentRepository) Delete(
	ctx context.Context,
	agentID string,
	generation uint64,
	revision int64,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	evidence, err := repository.readSingleton(ctx)
	if errorsIsAgentNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if evidence.record.ID != agentID || evidence.record.Generation != generation ||
		evidence.primaryRevision != revision || evidence.record.Phase != localagentrecord.LocalAgentPhaseDeleting ||
		evidence.digest != nil {
		return errs.New(errs.KindStateConflict, "local Agent is not at the deletable generation and revision")
	}
	conditions := []etcdstore.Condition{
		{Key: localagentrecord.LocalAgentSingletonKey, ModRevision: evidence.singleton.ModRevision},
		{Key: localagentrecord.LocalAgentPrimaryKey(agentID), ModRevision: evidence.primary.ModRevision},
		{Key: localagentrecord.LocalAgentOwnerKey(agentID), ModRevision: evidence.owner.ModRevision},
		{Key: localagentrecord.LocalAgentConfigKey(agentID), ModRevision: evidence.config.ModRevision},
		{Key: localagentrecord.LocalAgentTokenKey(agentID), ModRevision: evidence.token.ModRevision},
	}
	mutations := make([]etcdstore.Mutation, len(conditions))
	for index, condition := range conditions {
		mutations[index] = etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: condition.Key}
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return err
	}
	clearKeyValues(result.FailureReads)
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "local Agent deletion state changed")
	}
	return nil
}

func (repository *LocalAgentRepository) ResolveAgentChannel(
	ctx context.Context,
	presentedAgentID string,
	token [agentprotocol.RawTokenBytes]byte,
) (localagentrecord.LocalAgentChannelAuthorization, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return localagentrecord.LocalAgentChannelAuthorization{}, err
	}
	if err := ids.Validate(ids.KindAgent, presentedAgentID); err != nil {
		return localagentrecord.LocalAgentChannelAuthorization{}, agentCredentialNotFound()
	}
	digest := sha256.Sum256(token[:])
	digestText := base64.RawURLEncoding.EncodeToString(digest[:])
	lookup, err := repository.store.Get(ctx, localagentrecord.LocalAgentDigestKey(digestText))
	if err != nil {
		return localagentrecord.LocalAgentChannelAuthorization{}, err
	}
	if lookup == nil || lookup.Entry == nil {
		return localagentrecord.LocalAgentChannelAuthorization{}, agentCredentialNotFound()
	}
	resolvedAgentID, err := localagentrecord.DecodeLocalAgentReference(lookup.Entry.Value)
	if err != nil || resolvedAgentID != presentedAgentID {
		return localagentrecord.LocalAgentChannelAuthorization{}, agentCredentialNotFound()
	}
	values, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			localagentrecord.LocalAgentSingletonKey,
			localagentrecord.LocalAgentPrimaryKey(resolvedAgentID),
			localagentrecord.LocalAgentConfigKey(resolvedAgentID),
		},
		Revision: lookup.ReadRevision,
	})
	if err != nil {
		return localagentrecord.LocalAgentChannelAuthorization{}, err
	}
	if len(values.Values) != 3 || values.Values[0] == nil || values.Values[1] == nil || values.Values[2] == nil {
		return localagentrecord.LocalAgentChannelAuthorization{}, errs.New(
			errs.KindInternal,
			"Agent credential references incomplete durable state",
		)
	}
	singletonAgentID, err := localagentrecord.DecodeLocalAgentReference(values.Values[0].Value)
	if err != nil || singletonAgentID != resolvedAgentID {
		return localagentrecord.LocalAgentChannelAuthorization{}, errs.New(
			errs.KindInternal,
			"Agent singleton does not match credential lookup",
		)
	}
	primary, err := localagentrecord.DecodeLocalAgentPrimary(values.Values[1].Value)
	if err != nil {
		return localagentrecord.LocalAgentChannelAuthorization{}, err
	}
	config, err := localagentrecord.DecodeLocalAgentConfig(values.Values[2].Value)
	if err != nil {
		return localagentrecord.LocalAgentChannelAuthorization{}, err
	}
	if primary.ID != resolvedAgentID || config.AgentID != resolvedAgentID ||
		primary.Generation != config.Generation || primary.Phase == localagentrecord.LocalAgentPhaseDeleting {
		return localagentrecord.LocalAgentChannelAuthorization{}, agentCredentialNotFound()
	}
	return localagentrecord.LocalAgentChannelAuthorization{
		AgentID: resolvedAgentID, Generation: primary.Generation,
		Config: localagentrecord.CloneLocalAgentConfig(config.Config),
	}, nil
}

func (repository *LocalAgentRepository) transitionPhase(
	ctx context.Context,
	agentID string,
	generation uint64,
	revision int64,
	expected localagentrecord.LocalAgentPhase,
	next localagentrecord.LocalAgentPhase,
	idempotent bool,
	readyAt time.Time,
) (etcdstore.Versioned[localagentrecord.LocalAgentRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	evidence, err := repository.readSingleton(ctx)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	if evidence.record.ID != agentID || evidence.record.Generation != generation ||
		evidence.primaryRevision != revision {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindStateConflict,
			"local Agent generation or revision changed",
		)
	}
	if idempotent && evidence.record.Phase == next {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{
			Record: evidence.record, Revision: evidence.primaryRevision,
			ReadRevision: evidence.readRevision,
		}, nil
	}
	if evidence.record.Phase != expected {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(errs.KindStateConflict, "local Agent phase changed")
	}
	replacement := localagentrecord.CloneLocalAgentRecord(evidence.record)
	replacement.Phase = next
	if expected != localagentrecord.LocalAgentPhaseUpdating {
		replacement.ReadyAt = readyAt
	}
	primaryValue, err := localagentrecord.EncodeLocalAgentPrimary(replacement)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	defer clear(primaryValue)
	result, err := repository.store.Transact(ctx, []etcdstore.Condition{{
		Key: localagentrecord.LocalAgentPrimaryKey(agentID), ModRevision: evidence.primary.ModRevision,
	}}, []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: localagentrecord.LocalAgentPrimaryKey(agentID), Value: primaryValue}})
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	clearKeyValues(result.FailureReads)
	if !result.Succeeded {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(errs.KindStateConflict, "local Agent phase changed")
	}
	return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *LocalAgentRepository) readSingleton(ctx context.Context) (localAgentEvidence, error) {
	pointer, err := repository.store.Get(ctx, localagentrecord.LocalAgentSingletonKey)
	if err != nil {
		return localAgentEvidence{}, err
	}
	if pointer == nil || pointer.Entry == nil {
		return localAgentEvidence{}, errs.New(errs.KindAgentNotFound, "local Agent was not found")
	}
	agentID, err := localagentrecord.DecodeLocalAgentReference(pointer.Entry.Value)
	if err != nil {
		return localAgentEvidence{}, err
	}
	values, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			localagentrecord.LocalAgentPrimaryKey(agentID), localagentrecord.LocalAgentOwnerKey(agentID),
			localagentrecord.LocalAgentConfigKey(agentID), localagentrecord.LocalAgentTokenKey(agentID),
		},
		Revision: pointer.ReadRevision,
	})
	if err != nil {
		return localAgentEvidence{}, err
	}
	if len(values.Values) != 4 {
		return localAgentEvidence{}, errs.New(errs.KindInternal, "local Agent aggregate read is incomplete")
	}
	for _, value := range values.Values {
		if value == nil {
			return localAgentEvidence{}, errs.New(errs.KindInternal, "local Agent aggregate is incomplete")
		}
	}
	primary, err := localagentrecord.DecodeLocalAgentPrimary(values.Values[0].Value)
	if err != nil {
		return localAgentEvidence{}, err
	}
	ownerAgentID, err := localagentrecord.DecodeLocalAgentReference(values.Values[1].Value)
	if err != nil || ownerAgentID != agentID {
		return localAgentEvidence{}, errs.New(errs.KindInternal, "local Agent owner index does not match primary")
	}
	config, err := localagentrecord.DecodeLocalAgentConfig(values.Values[2].Value)
	if err != nil {
		return localAgentEvidence{}, err
	}
	token, err := localagentrecord.DecodeLocalAgentToken(values.Values[3].Value)
	if err != nil {
		return localAgentEvidence{}, err
	}
	if primary.ID != agentID || config.AgentID != agentID || token.AgentID != agentID ||
		primary.Generation != config.Generation || primary.Generation != token.Generation {
		return localAgentEvidence{}, errs.New(errs.KindInternal, "local Agent aggregate identities do not match")
	}
	digests, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: localagentrecord.LocalAgentDigestPrefix, Limit: 2, Revision: pointer.ReadRevision,
	})
	if err != nil {
		return localAgentEvidence{}, err
	}
	if digests.More || len(digests.Values) > 1 {
		return localAgentEvidence{}, errs.New(errs.KindInternal, "local Agent has multiple credential indexes")
	}
	var digestValue *etcdstore.KeyValue
	digestText := ""
	if len(digests.Values) == 1 {
		value := digests.Values[0]
		indexedAgentID, decodeErr := localagentrecord.DecodeLocalAgentReference(value.Value)
		parsedDigest, parseErr := localagentrecord.LocalAgentDigestFromKey(value.Key)
		if decodeErr != nil || parseErr != nil || indexedAgentID != agentID {
			return localAgentEvidence{}, errs.New(errs.KindInternal, "local Agent credential index is corrupt")
		}
		digestText = parsedDigest
		digestValue = &value
	}
	if primary.Phase == localagentrecord.LocalAgentPhaseDeleting {
		if digestValue != nil {
			return localAgentEvidence{}, errs.New(
				errs.KindInternal,
				"deleting local Agent still has credential authority",
			)
		}
	} else if digestValue == nil {
		return localAgentEvidence{}, errs.New(errs.KindInternal, "active local Agent is missing credential authority")
	}
	record := localagentrecord.LocalAgentRecord{
		ID: primary.ID, EnrollmentTaskID: primary.EnrollmentTaskID,
		Image: primary.Image, Generation: primary.Generation, Phase: primary.Phase,
		Config: config.Config, EncryptedToken: token.EncryptedToken,
		TokenDigest: digestText, CreatedAt: primary.CreatedAt, ReadyAt: primary.ReadyAt,
		TokenUpdatedAt: token.UpdatedAt,
	}
	if err := localagentrecord.ValidateLocalAgentRecord(record); err != nil {
		return localAgentEvidence{}, errs.New(errs.KindInternal, "local Agent aggregate is corrupt")
	}
	return localAgentEvidence{
		record: record, primaryRevision: values.Values[0].ModRevision,
		readRevision: pointer.ReadRevision, singleton: pointer.Entry,
		primary: values.Values[0], owner: values.Values[1], config: values.Values[2],
		token: values.Values[3], digest: digestValue,
	}, nil
}

func agentCredentialNotFound() error {
	return errs.New(errs.KindAgentNotFound, "Agent credential was not found")
}

func errorsIsAgentNotFound(err error) bool {
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindAgentNotFound
}
