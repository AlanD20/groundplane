package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	localAgentSingletonKey  = "/v1/singletons/local-agent/-"
	localAgentPrimaryPrefix = "/v1/records/agents/"
	localAgentOwnerPrefix   = "/v1/indexes/agents/by-owner/platform/-/"
	localAgentConfigPrefix  = "/v1/singletons/agent-configs/"
	localAgentTokenPrefix   = "/v1/runtime/agent-channel-tokens/"
	localAgentDigestPrefix  = "/v1/indexes/agent-channel-tokens/by-digest/global/-/"
)

type LocalAgentPhase string

const (
	LocalAgentPhaseProvisioning LocalAgentPhase = "provisioning"
	LocalAgentPhaseReady        LocalAgentPhase = "ready"
	LocalAgentPhaseDeleting     LocalAgentPhase = "deleting"
)

type LocalAgentConfig struct {
	PullIntervalSeconds int32
	MaxConcurrentTasks  int32
	Labels              map[string]string
}

type LocalAgentRecord struct {
	ID               string
	EnrollmentTaskID string
	Image            string
	Generation       uint64
	Phase            LocalAgentPhase
	Config           LocalAgentConfig
	EncryptedToken   []byte
	TokenDigest      string
	CreatedAt        time.Time
	ReadyAt          time.Time
	TokenUpdatedAt   time.Time
}

type LocalAgentChannelAuthorization struct {
	AgentID    string
	Generation uint64
	Config     LocalAgentConfig
}

type localAgentPrimaryData struct {
	ID               string          `json:"id"`
	EnrollmentTaskID string          `json:"enrollment_task_id"`
	Image            string          `json:"image"`
	Generation       uint64          `json:"generation"`
	Phase            LocalAgentPhase `json:"phase"`
	CreatedAt        string          `json:"created_at"`
	ReadyAt          string          `json:"ready_at,omitempty"`
}

type localAgentConfigData struct {
	AgentID             string            `json:"agent_id"`
	Generation          uint64            `json:"generation"`
	PullIntervalSeconds int32             `json:"pull_interval_seconds"`
	MaxConcurrentTasks  int32             `json:"max_concurrent_tasks"`
	Labels              map[string]string `json:"labels,omitempty"`
}

type localAgentTokenData struct {
	AgentID    string `json:"agent_id"`
	Generation uint64 `json:"generation"`
	Ciphertext string `json:"ciphertext"`
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
}

type localAgentReference struct {
	Schema   int    `json:"schema"`
	RecordID string `json:"record_id"`
}

type localAgentRepositoryStore interface {
	Get(context.Context, string) (*GetResult, error)
	GetMany(context.Context, GetManyRequest) (*GetManyResult, error)
	Range(context.Context, RangeRequest) (*RangeResult, error)
	Transact(context.Context, []Condition, []Mutation) (TransactionResult, error)
}

type LocalAgentRepository struct {
	store localAgentRepositoryStore
}

type localAgentEvidence struct {
	record          LocalAgentRecord
	primaryRevision int64
	readRevision    int64
	singleton       *KeyValue
	primary         *KeyValue
	owner           *KeyValue
	config          *KeyValue
	token           *KeyValue
	digest          *KeyValue
}

func NewLocalAgentRepository(store Store) (*LocalAgentRepository, error) {
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
	record LocalAgentRecord,
) (Versioned[LocalAgentRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[LocalAgentRecord]{}, err
	}
	if record.Phase != LocalAgentPhaseProvisioning {
		return Versioned[LocalAgentRecord]{}, errs.New(errs.KindValidationFailed, "local Agent must be created in provisioning phase")
	}
	if record.TokenUpdatedAt.IsZero() {
		record.TokenUpdatedAt = record.CreatedAt
	}
	if err := validateLocalAgentRecord(record); err != nil {
		return Versioned[LocalAgentRecord]{}, err
	}
	primaryValue, configValue, tokenValue, reference, err := encodeLocalAgentValues(record)
	if err != nil {
		return Versioned[LocalAgentRecord]{}, err
	}
	defer clear(primaryValue)
	defer clear(configValue)
	defer clear(tokenValue)
	defer clear(reference)
	keys := localAgentKeys(record.ID, record.TokenDigest)
	conditions := make([]Condition, len(keys))
	mutations := make([]Mutation, len(keys))
	values := [][]byte{reference, primaryValue, reference, configValue, tokenValue, reference}
	for index, key := range keys {
		conditions[index] = Condition{Key: key}
		mutations[index] = Mutation{Type: MutationPut, Key: key, Value: values[index]}
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return Versioned[LocalAgentRecord]{}, err
	}
	clearKeyValues(result.FailureReads)
	if !result.Succeeded {
		if len(result.FailureReads) == len(conditions) && result.FailureReads[0] != nil {
			return Versioned[LocalAgentRecord]{}, errs.New(errs.KindStateConflict, "the local Agent already exists")
		}
		return Versioned[LocalAgentRecord]{}, errs.New(errs.KindInternal, "local Agent creation collided with durable state")
	}
	return Versioned[LocalAgentRecord]{
		Record: cloneLocalAgentRecord(record), Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *LocalAgentRepository) GetSingleton(
	ctx context.Context,
) (Versioned[LocalAgentRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[LocalAgentRecord]{}, err
	}
	evidence, err := repository.readSingleton(ctx)
	if err != nil {
		return Versioned[LocalAgentRecord]{}, err
	}
	return Versioned[LocalAgentRecord]{
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
) (Versioned[LocalAgentRecord], error) {
	if err := validateTimestamp("local Agent ready_at", readyAt); err != nil {
		return Versioned[LocalAgentRecord]{}, err
	}
	return repository.transitionPhase(
		ctx,
		agentID,
		generation,
		revision,
		LocalAgentPhaseProvisioning,
		LocalAgentPhaseReady,
		false,
		readyAt,
	)
}

// UpdateConfig replaces only the generation-bound config singleton. The
// primary record revision fences lifecycle changes while the config revision
// serializes concurrent replacements. Equal desired state is idempotent.
func (repository *LocalAgentRepository) UpdateConfig(
	ctx context.Context,
	agentID string,
	generation uint64,
	revision int64,
	config LocalAgentConfig,
) (Versioned[LocalAgentRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[LocalAgentRecord]{}, err
	}
	if err := validateLocalAgentConfig(config); err != nil {
		return Versioned[LocalAgentRecord]{}, err
	}
	evidence, err := repository.readSingleton(ctx)
	if err != nil {
		return Versioned[LocalAgentRecord]{}, err
	}
	if evidence.record.ID != agentID || evidence.record.Generation != generation ||
		evidence.primaryRevision != revision {
		return Versioned[LocalAgentRecord]{}, errs.New(errs.KindStateConflict, "local Agent generation or revision changed")
	}
	if evidence.record.Phase == LocalAgentPhaseDeleting {
		return Versioned[LocalAgentRecord]{}, errs.New(errs.KindStateConflict, "deleting local Agent config cannot be changed")
	}
	if equalLocalAgentConfig(evidence.record.Config, config) {
		return Versioned[LocalAgentRecord]{
			Record: cloneLocalAgentRecord(evidence.record), Revision: evidence.primaryRevision,
			ReadRevision: evidence.readRevision,
		}, nil
	}
	replacement := cloneLocalAgentRecord(evidence.record)
	replacement.Config = cloneLocalAgentConfig(config)
	configValue, err := encodeLocalAgentConfig(replacement)
	if err != nil {
		return Versioned[LocalAgentRecord]{}, err
	}
	defer clear(configValue)
	result, err := repository.store.Transact(ctx, []Condition{
		{Key: localAgentPrimaryKey(agentID), ModRevision: evidence.primary.ModRevision},
		{Key: localAgentConfigKey(agentID), ModRevision: evidence.config.ModRevision},
	}, []Mutation{{Type: MutationPut, Key: localAgentConfigKey(agentID), Value: configValue}})
	if err != nil {
		return Versioned[LocalAgentRecord]{}, err
	}
	clearKeyValues(result.FailureReads)
	if !result.Succeeded {
		return Versioned[LocalAgentRecord]{}, errs.New(errs.KindStateConflict, "local Agent config changed")
	}
	return Versioned[LocalAgentRecord]{
		Record: replacement, Revision: evidence.primaryRevision, ReadRevision: result.Revision,
	}, nil
}

func (repository *LocalAgentRepository) BeginDelete(
	ctx context.Context,
	agentID string,
	generation uint64,
	revision int64,
) (Versioned[LocalAgentRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[LocalAgentRecord]{}, err
	}
	evidence, err := repository.readSingleton(ctx)
	if err != nil {
		return Versioned[LocalAgentRecord]{}, err
	}
	if evidence.record.ID != agentID || evidence.record.Generation != generation ||
		evidence.primaryRevision != revision {
		return Versioned[LocalAgentRecord]{}, errs.New(errs.KindStateConflict, "local Agent generation or revision changed")
	}
	if evidence.record.Phase == LocalAgentPhaseDeleting {
		return Versioned[LocalAgentRecord]{
			Record: evidence.record, Revision: evidence.primaryRevision,
			ReadRevision: evidence.readRevision,
		}, nil
	}
	if evidence.digest == nil {
		return Versioned[LocalAgentRecord]{}, errs.New(errs.KindInternal, "active local Agent is missing its credential index")
	}
	replacement := cloneLocalAgentRecord(evidence.record)
	replacement.Phase = LocalAgentPhaseDeleting
	replacement.TokenDigest = ""
	primaryValue, err := encodeLocalAgentPrimary(replacement)
	if err != nil {
		return Versioned[LocalAgentRecord]{}, err
	}
	defer clear(primaryValue)
	result, err := repository.store.Transact(ctx, []Condition{
		{Key: localAgentPrimaryKey(agentID), ModRevision: evidence.primary.ModRevision},
		{Key: evidence.digest.Key, ModRevision: evidence.digest.ModRevision},
	}, []Mutation{
		{Type: MutationPut, Key: localAgentPrimaryKey(agentID), Value: primaryValue},
		{Type: MutationDelete, Key: evidence.digest.Key},
	})
	if err != nil {
		return Versioned[LocalAgentRecord]{}, err
	}
	clearKeyValues(result.FailureReads)
	if !result.Succeeded {
		return Versioned[LocalAgentRecord]{}, errs.New(errs.KindStateConflict, "local Agent deletion state changed")
	}
	return Versioned[LocalAgentRecord]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *LocalAgentRepository) Delete(
	ctx context.Context,
	agentID string,
	generation uint64,
	revision int64,
) error {
	if err := validateContext(ctx); err != nil {
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
		evidence.primaryRevision != revision || evidence.record.Phase != LocalAgentPhaseDeleting ||
		evidence.digest != nil {
		return errs.New(errs.KindStateConflict, "local Agent is not at the deletable generation and revision")
	}
	conditions := []Condition{
		{Key: localAgentSingletonKey, ModRevision: evidence.singleton.ModRevision},
		{Key: localAgentPrimaryKey(agentID), ModRevision: evidence.primary.ModRevision},
		{Key: localAgentOwnerKey(agentID), ModRevision: evidence.owner.ModRevision},
		{Key: localAgentConfigKey(agentID), ModRevision: evidence.config.ModRevision},
		{Key: localAgentTokenKey(agentID), ModRevision: evidence.token.ModRevision},
	}
	mutations := make([]Mutation, len(conditions))
	for index, condition := range conditions {
		mutations[index] = Mutation{Type: MutationDelete, Key: condition.Key}
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
) (LocalAgentChannelAuthorization, error) {
	if err := validateContext(ctx); err != nil {
		return LocalAgentChannelAuthorization{}, err
	}
	if err := ids.Validate(ids.KindAgent, presentedAgentID); err != nil {
		return LocalAgentChannelAuthorization{}, agentCredentialNotFound()
	}
	digest := sha256.Sum256(token[:])
	digestText := base64.RawURLEncoding.EncodeToString(digest[:])
	lookup, err := repository.store.Get(ctx, localAgentDigestKey(digestText))
	if err != nil {
		return LocalAgentChannelAuthorization{}, err
	}
	if lookup == nil || lookup.Entry == nil {
		return LocalAgentChannelAuthorization{}, agentCredentialNotFound()
	}
	resolvedAgentID, err := decodeLocalAgentReference(lookup.Entry.Value)
	if err != nil || resolvedAgentID != presentedAgentID {
		return LocalAgentChannelAuthorization{}, agentCredentialNotFound()
	}
	values, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			localAgentSingletonKey,
			localAgentPrimaryKey(resolvedAgentID),
			localAgentConfigKey(resolvedAgentID),
		},
		Revision: lookup.ReadRevision,
	})
	if err != nil {
		return LocalAgentChannelAuthorization{}, err
	}
	if len(values.Values) != 3 || values.Values[0] == nil || values.Values[1] == nil || values.Values[2] == nil {
		return LocalAgentChannelAuthorization{}, errs.New(errs.KindInternal, "Agent credential references incomplete durable state")
	}
	singletonAgentID, err := decodeLocalAgentReference(values.Values[0].Value)
	if err != nil || singletonAgentID != resolvedAgentID {
		return LocalAgentChannelAuthorization{}, errs.New(errs.KindInternal, "Agent singleton does not match credential lookup")
	}
	primary, err := decodeLocalAgentPrimary(values.Values[1].Value)
	if err != nil {
		return LocalAgentChannelAuthorization{}, err
	}
	config, err := decodeLocalAgentConfig(values.Values[2].Value)
	if err != nil {
		return LocalAgentChannelAuthorization{}, err
	}
	if primary.ID != resolvedAgentID || config.AgentID != resolvedAgentID ||
		primary.Generation != config.Generation || primary.Phase == LocalAgentPhaseDeleting {
		return LocalAgentChannelAuthorization{}, agentCredentialNotFound()
	}
	return LocalAgentChannelAuthorization{
		AgentID: resolvedAgentID, Generation: primary.Generation,
		Config: cloneLocalAgentConfig(config.Config),
	}, nil
}

func (repository *LocalAgentRepository) transitionPhase(
	ctx context.Context,
	agentID string,
	generation uint64,
	revision int64,
	expected LocalAgentPhase,
	next LocalAgentPhase,
	idempotent bool,
	readyAt time.Time,
) (Versioned[LocalAgentRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[LocalAgentRecord]{}, err
	}
	evidence, err := repository.readSingleton(ctx)
	if err != nil {
		return Versioned[LocalAgentRecord]{}, err
	}
	if evidence.record.ID != agentID || evidence.record.Generation != generation ||
		evidence.primaryRevision != revision {
		return Versioned[LocalAgentRecord]{}, errs.New(errs.KindStateConflict, "local Agent generation or revision changed")
	}
	if idempotent && evidence.record.Phase == next {
		return Versioned[LocalAgentRecord]{
			Record: evidence.record, Revision: evidence.primaryRevision,
			ReadRevision: evidence.readRevision,
		}, nil
	}
	if evidence.record.Phase != expected {
		return Versioned[LocalAgentRecord]{}, errs.New(errs.KindStateConflict, "local Agent phase changed")
	}
	replacement := cloneLocalAgentRecord(evidence.record)
	replacement.Phase = next
	replacement.ReadyAt = readyAt
	primaryValue, err := encodeLocalAgentPrimary(replacement)
	if err != nil {
		return Versioned[LocalAgentRecord]{}, err
	}
	defer clear(primaryValue)
	result, err := repository.store.Transact(ctx, []Condition{{
		Key: localAgentPrimaryKey(agentID), ModRevision: evidence.primary.ModRevision,
	}}, []Mutation{{Type: MutationPut, Key: localAgentPrimaryKey(agentID), Value: primaryValue}})
	if err != nil {
		return Versioned[LocalAgentRecord]{}, err
	}
	clearKeyValues(result.FailureReads)
	if !result.Succeeded {
		return Versioned[LocalAgentRecord]{}, errs.New(errs.KindStateConflict, "local Agent phase changed")
	}
	return Versioned[LocalAgentRecord]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *LocalAgentRepository) readSingleton(ctx context.Context) (localAgentEvidence, error) {
	pointer, err := repository.store.Get(ctx, localAgentSingletonKey)
	if err != nil {
		return localAgentEvidence{}, err
	}
	if pointer == nil || pointer.Entry == nil {
		return localAgentEvidence{}, errs.New(errs.KindAgentNotFound, "local Agent was not found")
	}
	agentID, err := decodeLocalAgentReference(pointer.Entry.Value)
	if err != nil {
		return localAgentEvidence{}, err
	}
	values, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			localAgentPrimaryKey(agentID), localAgentOwnerKey(agentID),
			localAgentConfigKey(agentID), localAgentTokenKey(agentID),
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
	primary, err := decodeLocalAgentPrimary(values.Values[0].Value)
	if err != nil {
		return localAgentEvidence{}, err
	}
	ownerAgentID, err := decodeLocalAgentReference(values.Values[1].Value)
	if err != nil || ownerAgentID != agentID {
		return localAgentEvidence{}, errs.New(errs.KindInternal, "local Agent owner index does not match primary")
	}
	config, err := decodeLocalAgentConfig(values.Values[2].Value)
	if err != nil {
		return localAgentEvidence{}, err
	}
	token, err := decodeLocalAgentToken(values.Values[3].Value)
	if err != nil {
		return localAgentEvidence{}, err
	}
	if primary.ID != agentID || config.AgentID != agentID || token.AgentID != agentID ||
		primary.Generation != config.Generation || primary.Generation != token.Generation {
		return localAgentEvidence{}, errs.New(errs.KindInternal, "local Agent aggregate identities do not match")
	}
	digests, err := repository.store.Range(ctx, RangeRequest{
		Prefix: localAgentDigestPrefix, Limit: 2, Revision: pointer.ReadRevision,
	})
	if err != nil {
		return localAgentEvidence{}, err
	}
	if digests.More || len(digests.Values) > 1 {
		return localAgentEvidence{}, errs.New(errs.KindInternal, "local Agent has multiple credential indexes")
	}
	var digestValue *KeyValue
	digestText := ""
	if len(digests.Values) == 1 {
		value := digests.Values[0]
		indexedAgentID, decodeErr := decodeLocalAgentReference(value.Value)
		parsedDigest, parseErr := localAgentDigestFromKey(value.Key)
		if decodeErr != nil || parseErr != nil || indexedAgentID != agentID {
			return localAgentEvidence{}, errs.New(errs.KindInternal, "local Agent credential index is corrupt")
		}
		digestText = parsedDigest
		digestValue = &value
	}
	if primary.Phase == LocalAgentPhaseDeleting {
		if digestValue != nil {
			return localAgentEvidence{}, errs.New(errs.KindInternal, "deleting local Agent still has credential authority")
		}
	} else if digestValue == nil {
		return localAgentEvidence{}, errs.New(errs.KindInternal, "active local Agent is missing credential authority")
	}
	record := LocalAgentRecord{
		ID: primary.ID, EnrollmentTaskID: primary.EnrollmentTaskID,
		Image: primary.Image, Generation: primary.Generation, Phase: primary.Phase,
		Config: config.Config, EncryptedToken: token.EncryptedToken,
		TokenDigest: digestText, CreatedAt: primary.CreatedAt, ReadyAt: primary.ReadyAt,
		TokenUpdatedAt: token.UpdatedAt,
	}
	if err := validateLocalAgentRecord(record); err != nil {
		return localAgentEvidence{}, errs.New(errs.KindInternal, "local Agent aggregate is corrupt")
	}
	return localAgentEvidence{
		record: record, primaryRevision: values.Values[0].ModRevision,
		readRevision: pointer.ReadRevision, singleton: pointer.Entry,
		primary: values.Values[0], owner: values.Values[1], config: values.Values[2],
		token: values.Values[3], digest: digestValue,
	}, nil
}

type decodedLocalAgentConfig struct {
	AgentID    string
	Generation uint64
	Config     LocalAgentConfig
}

type decodedLocalAgentToken struct {
	AgentID        string
	Generation     uint64
	EncryptedToken []byte
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func encodeLocalAgentValues(record LocalAgentRecord) ([]byte, []byte, []byte, []byte, error) {
	primary, err := encodeLocalAgentPrimary(record)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	config, err := encodeLocalAgentConfig(record)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	token, err := encodeEnvelope("agent_channel_token", localAgentTokenData{
		AgentID: record.ID, Generation: record.Generation,
		Ciphertext: base64.RawURLEncoding.EncodeToString(record.EncryptedToken),
		CreatedAt:  record.CreatedAt.Format(time.RFC3339Nano),
		UpdatedAt:  record.TokenUpdatedAt.Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}
	reference, err := encodeLocalAgentReference(record.ID)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return primary, config, token, reference, nil
}

func encodeLocalAgentConfig(record LocalAgentRecord) ([]byte, error) {
	return encodeEnvelope("agent_config", localAgentConfigData{
		AgentID: record.ID, Generation: record.Generation,
		PullIntervalSeconds: record.Config.PullIntervalSeconds,
		MaxConcurrentTasks:  record.Config.MaxConcurrentTasks,
		Labels:              cloneStringMap(record.Config.Labels),
	})
}

func encodeLocalAgentPrimary(record LocalAgentRecord) ([]byte, error) {
	readyAt := ""
	if !record.ReadyAt.IsZero() {
		readyAt = record.ReadyAt.Format(time.RFC3339Nano)
	}
	return encodeEnvelope("agent", localAgentPrimaryData{
		ID: record.ID, EnrollmentTaskID: record.EnrollmentTaskID,
		Image: record.Image, Generation: record.Generation,
		Phase: record.Phase, CreatedAt: record.CreatedAt.Format(time.RFC3339Nano), ReadyAt: readyAt,
	})
}

func decodeLocalAgentPrimary(value []byte) (LocalAgentRecord, error) {
	data, err := decodeEnvelope[localAgentPrimaryData](value, "agent")
	if err != nil {
		return LocalAgentRecord{}, err
	}
	createdAt, err := parseCanonicalTimestamp(data.CreatedAt)
	if err != nil {
		return LocalAgentRecord{}, corruptRecord()
	}
	var readyAt time.Time
	if data.ReadyAt != "" {
		readyAt, err = parseCanonicalTimestamp(data.ReadyAt)
		if err != nil {
			return LocalAgentRecord{}, corruptRecord()
		}
	}
	record := LocalAgentRecord{
		ID: data.ID, EnrollmentTaskID: data.EnrollmentTaskID,
		Image: data.Image, Generation: data.Generation,
		Phase: data.Phase, CreatedAt: createdAt, ReadyAt: readyAt,
	}
	if err := validateLocalAgentPrimary(record); err != nil {
		return LocalAgentRecord{}, corruptRecord()
	}
	return record, nil
}

func decodeLocalAgentConfig(value []byte) (decodedLocalAgentConfig, error) {
	data, err := decodeEnvelope[localAgentConfigData](value, "agent_config")
	if err != nil {
		return decodedLocalAgentConfig{}, err
	}
	config := LocalAgentConfig{
		PullIntervalSeconds: data.PullIntervalSeconds,
		MaxConcurrentTasks:  data.MaxConcurrentTasks,
		Labels:              cloneStringMap(data.Labels),
	}
	if ids.Validate(ids.KindAgent, data.AgentID) != nil || data.Generation == 0 ||
		validateLocalAgentConfig(config) != nil {
		return decodedLocalAgentConfig{}, corruptRecord()
	}
	return decodedLocalAgentConfig{AgentID: data.AgentID, Generation: data.Generation, Config: config}, nil
}

func decodeLocalAgentToken(value []byte) (decodedLocalAgentToken, error) {
	data, err := decodeEnvelope[localAgentTokenData](value, "agent_channel_token")
	if err != nil {
		return decodedLocalAgentToken{}, err
	}
	ciphertext, err := base64.RawURLEncoding.Strict().DecodeString(data.Ciphertext)
	if err != nil || len(ciphertext) == 0 ||
		base64.RawURLEncoding.EncodeToString(ciphertext) != data.Ciphertext {
		return decodedLocalAgentToken{}, corruptRecord()
	}
	createdAt, err := parseCanonicalTimestamp(data.CreatedAt)
	if err != nil {
		return decodedLocalAgentToken{}, corruptRecord()
	}
	updatedAt, err := parseCanonicalTimestamp(data.UpdatedAt)
	if err != nil || updatedAt.Before(createdAt) {
		return decodedLocalAgentToken{}, corruptRecord()
	}
	if ids.Validate(ids.KindAgent, data.AgentID) != nil || data.Generation == 0 {
		return decodedLocalAgentToken{}, corruptRecord()
	}
	return decodedLocalAgentToken{
		AgentID: data.AgentID, Generation: data.Generation, EncryptedToken: ciphertext,
		CreatedAt: createdAt, UpdatedAt: updatedAt,
	}, nil
}

func encodeLocalAgentReference(agentID string) ([]byte, error) {
	if err := ids.Validate(ids.KindAgent, agentID); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "local Agent reference id is invalid")
	}
	return json.Marshal(localAgentReference{Schema: 1, RecordID: agentID})
}

func decodeLocalAgentReference(value []byte) (string, error) {
	if rejectDuplicateJSONFields(value) != nil {
		return "", corruptRecord()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var reference localAgentReference
	if err := decoder.Decode(&reference); err != nil || requireJSONEOF(decoder) != nil ||
		reference.Schema != 1 || ids.Validate(ids.KindAgent, reference.RecordID) != nil {
		return "", corruptRecord()
	}
	return reference.RecordID, nil
}

func validateLocalAgentRecord(record LocalAgentRecord) error {
	if err := validateLocalAgentPrimary(record); err != nil {
		return err
	}
	if err := validateLocalAgentConfig(record.Config); err != nil {
		return err
	}
	if len(record.EncryptedToken) == 0 {
		return errs.New(errs.KindValidationFailed, "local Agent encrypted token is required")
	}
	if record.Phase == LocalAgentPhaseDeleting {
		if record.TokenDigest != "" {
			return errs.New(errs.KindInternal, "deleting local Agent must not retain a token digest")
		}
	} else if !validLocalAgentDigest(record.TokenDigest) {
		return errs.New(errs.KindValidationFailed, "local Agent token digest is invalid")
	}
	if err := validateTimestamp("local Agent token updated_at", record.TokenUpdatedAt); err != nil {
		return err
	}
	if record.TokenUpdatedAt.Before(record.CreatedAt) {
		return errs.New(errs.KindInternal, "local Agent token updated_at precedes creation")
	}
	return nil
}

func validateLocalAgentPrimary(record LocalAgentRecord) error {
	if err := validateStableID(ids.KindAgent, record.ID); err != nil {
		return err
	}
	if err := validateStableID(ids.KindTask, record.EnrollmentTaskID); err != nil {
		return err
	}
	if !imageref.IsDigestPinned(record.Image) {
		return errs.New(errs.KindValidationFailed, "local Agent image must be digest-pinned")
	}
	if record.Generation == 0 {
		return errs.New(errs.KindValidationFailed, "local Agent generation must be positive")
	}
	if err := validateTimestamp("local Agent created_at", record.CreatedAt); err != nil {
		return err
	}
	switch record.Phase {
	case LocalAgentPhaseProvisioning:
		if !record.ReadyAt.IsZero() {
			return errs.New(errs.KindValidationFailed, "provisioning local Agent ready_at must be empty")
		}
	case LocalAgentPhaseReady, LocalAgentPhaseDeleting:
		if err := validateTimestamp("local Agent ready_at", record.ReadyAt); err != nil {
			return err
		}
		if record.ReadyAt.Before(record.CreatedAt) {
			return errs.New(errs.KindValidationFailed, "local Agent ready_at precedes creation")
		}
	default:
		return errs.New(errs.KindValidationFailed, "local Agent phase is invalid")
	}
	return nil
}

func validateLocalAgentConfig(config LocalAgentConfig) error {
	if config.PullIntervalSeconds <= 0 || config.MaxConcurrentTasks <= 0 {
		return errs.New(errs.KindValidationFailed, "local Agent config limits must be positive")
	}
	for key, value := range config.Labels {
		if !utf8.ValidString(key) || strings.IndexByte(key, 0) >= 0 ||
			!utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
			return errs.New(errs.KindValidationFailed, "local Agent labels are invalid")
		}
	}
	return nil
}

func validLocalAgentDigest(digest string) bool {
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(digest)
	return err == nil && len(decoded) == sha256.Size &&
		base64.RawURLEncoding.EncodeToString(decoded) == digest
}

func localAgentKeys(agentID string, digest string) []string {
	return []string{
		localAgentSingletonKey,
		localAgentPrimaryKey(agentID),
		localAgentOwnerKey(agentID),
		localAgentConfigKey(agentID),
		localAgentTokenKey(agentID),
		localAgentDigestKey(digest),
	}
}

func localAgentPrimaryKey(agentID string) string { return localAgentPrimaryPrefix + agentID }
func localAgentOwnerKey(agentID string) string   { return localAgentOwnerPrefix + agentID }
func localAgentConfigKey(agentID string) string  { return localAgentConfigPrefix + agentID }
func localAgentTokenKey(agentID string) string   { return localAgentTokenPrefix + agentID }
func localAgentDigestKey(digest string) string   { return localAgentDigestPrefix + "~" + digest }

func localAgentDigestFromKey(key string) (string, error) {
	if !strings.HasPrefix(key, localAgentDigestPrefix+"~") {
		return "", corruptRecord()
	}
	digest := strings.TrimPrefix(key, localAgentDigestPrefix+"~")
	if !validLocalAgentDigest(digest) {
		return "", corruptRecord()
	}
	return digest, nil
}

func cloneLocalAgentRecord(record LocalAgentRecord) LocalAgentRecord {
	record.Config = cloneLocalAgentConfig(record.Config)
	record.EncryptedToken = append([]byte(nil), record.EncryptedToken...)
	return record
}

func cloneLocalAgentConfig(config LocalAgentConfig) LocalAgentConfig {
	config.Labels = cloneStringMap(config.Labels)
	return config
}

func equalLocalAgentConfig(left LocalAgentConfig, right LocalAgentConfig) bool {
	if left.PullIntervalSeconds != right.PullIntervalSeconds ||
		left.MaxConcurrentTasks != right.MaxConcurrentTasks || len(left.Labels) != len(right.Labels) {
		return false
	}
	for key, value := range left.Labels {
		if right.Labels[key] != value {
			return false
		}
	}
	return true
}

func agentCredentialNotFound() error {
	return errs.New(errs.KindAgentNotFound, "Agent credential was not found")
}

func errorsIsAgentNotFound(err error) bool {
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindAgentNotFound
}
