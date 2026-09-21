package localagents

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"maps"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	LocalAgentSingletonKey  = "/v1/singletons/local-agent/-"
	localAgentPrimaryPrefix = "/v1/records/agents/"
	localAgentOwnerPrefix   = "/v1/indexes/agents/by-owner/platform/-/"
	localAgentConfigPrefix  = "/v1/singletons/agent-configs/"
	localAgentTokenPrefix   = "/v1/runtime/agent-channel-tokens/"
	LocalAgentDigestPrefix  = "/v1/indexes/agent-channel-tokens/by-digest/global/-/"
)

type LocalAgentPhase string

const (
	LocalAgentPhaseProvisioning LocalAgentPhase = "provisioning"
	LocalAgentPhaseReady        LocalAgentPhase = "ready"
	LocalAgentPhaseUpdating     LocalAgentPhase = "updating"
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

func EncodeLocalAgentValues(record LocalAgentRecord) ([]byte, []byte, []byte, []byte, error) {
	primary, err := EncodeLocalAgentPrimary(record)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	config, err := EncodeLocalAgentConfig(record)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	token, err := recordcodec.Encode("agent_channel_token", localAgentTokenData{
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

func EncodeLocalAgentConfig(record LocalAgentRecord) ([]byte, error) {
	return recordcodec.Encode("agent_config", localAgentConfigData{
		AgentID: record.ID, Generation: record.Generation,
		PullIntervalSeconds: record.Config.PullIntervalSeconds,
		MaxConcurrentTasks:  record.Config.MaxConcurrentTasks,
		Labels:              maps.Clone(record.Config.Labels),
	})
}

func EncodeLocalAgentPrimary(record LocalAgentRecord) ([]byte, error) {
	readyAt := ""
	if !record.ReadyAt.IsZero() {
		readyAt = record.ReadyAt.Format(time.RFC3339Nano)
	}
	return recordcodec.Encode("agent", localAgentPrimaryData{
		ID: record.ID, EnrollmentTaskID: record.EnrollmentTaskID,
		Image: record.Image, Generation: record.Generation,
		Phase: record.Phase, CreatedAt: record.CreatedAt.Format(time.RFC3339Nano), ReadyAt: readyAt,
	})
}

func DecodeLocalAgentPrimary(value []byte) (LocalAgentRecord, error) {
	data, err := recordcodec.Decode[localAgentPrimaryData](value, "agent")
	if err != nil {
		return LocalAgentRecord{}, err
	}
	createdAt, err := recordcodec.ParseCanonicalTimestamp(data.CreatedAt)
	if err != nil {
		return LocalAgentRecord{}, recordcodec.CorruptRecord()
	}
	var readyAt time.Time
	if data.ReadyAt != "" {
		readyAt, err = recordcodec.ParseCanonicalTimestamp(data.ReadyAt)
		if err != nil {
			return LocalAgentRecord{}, recordcodec.CorruptRecord()
		}
	}
	record := LocalAgentRecord{
		ID: data.ID, EnrollmentTaskID: data.EnrollmentTaskID,
		Image: data.Image, Generation: data.Generation,
		Phase: data.Phase, CreatedAt: createdAt, ReadyAt: readyAt,
	}
	if err := validateLocalAgentPrimary(record); err != nil {
		return LocalAgentRecord{}, recordcodec.CorruptRecord()
	}
	return record, nil
}

func DecodeLocalAgentConfig(value []byte) (decodedLocalAgentConfig, error) {
	data, err := recordcodec.Decode[localAgentConfigData](value, "agent_config")
	if err != nil {
		return decodedLocalAgentConfig{}, err
	}
	config := LocalAgentConfig{
		PullIntervalSeconds: data.PullIntervalSeconds,
		MaxConcurrentTasks:  data.MaxConcurrentTasks,
		Labels:              maps.Clone(data.Labels),
	}
	if ids.Validate(ids.KindAgent, data.AgentID) != nil || data.Generation == 0 ||
		ValidateLocalAgentConfig(config) != nil {
		return decodedLocalAgentConfig{}, recordcodec.CorruptRecord()
	}
	return decodedLocalAgentConfig{AgentID: data.AgentID, Generation: data.Generation, Config: config}, nil
}

func DecodeLocalAgentToken(value []byte) (decodedLocalAgentToken, error) {
	data, err := recordcodec.Decode[localAgentTokenData](value, "agent_channel_token")
	if err != nil {
		return decodedLocalAgentToken{}, err
	}
	ciphertext, err := base64.RawURLEncoding.Strict().DecodeString(data.Ciphertext)
	if err != nil || len(ciphertext) == 0 ||
		base64.RawURLEncoding.EncodeToString(ciphertext) != data.Ciphertext {
		return decodedLocalAgentToken{}, recordcodec.CorruptRecord()
	}
	createdAt, err := recordcodec.ParseCanonicalTimestamp(data.CreatedAt)
	if err != nil {
		return decodedLocalAgentToken{}, recordcodec.CorruptRecord()
	}
	updatedAt, err := recordcodec.ParseCanonicalTimestamp(data.UpdatedAt)
	if err != nil || updatedAt.Before(createdAt) {
		return decodedLocalAgentToken{}, recordcodec.CorruptRecord()
	}
	if ids.Validate(ids.KindAgent, data.AgentID) != nil || data.Generation == 0 {
		return decodedLocalAgentToken{}, recordcodec.CorruptRecord()
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

func DecodeLocalAgentReference(value []byte) (string, error) {
	if recordcodec.RejectDuplicateFields(value) != nil {
		return "", recordcodec.CorruptRecord()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var reference localAgentReference
	if err := decoder.Decode(&reference); err != nil || recordcodec.RequireEOF(decoder) != nil ||
		reference.Schema != 1 || ids.Validate(ids.KindAgent, reference.RecordID) != nil {
		return "", recordcodec.CorruptRecord()
	}
	return reference.RecordID, nil
}

func ValidateLocalAgentRecord(record LocalAgentRecord) error {
	if err := validateLocalAgentPrimary(record); err != nil {
		return err
	}
	if err := ValidateLocalAgentConfig(record.Config); err != nil {
		return err
	}
	if len(record.EncryptedToken) == 0 {
		return errs.New(errs.KindValidationFailed, "local Agent encrypted token is required")
	}
	if record.Phase == LocalAgentPhaseDeleting {
		if record.TokenDigest != "" {
			return errs.New(errs.KindInternal, "deleting local Agent must not retain a token digest")
		}
	} else if !ValidLocalAgentDigest(record.TokenDigest) {
		return errs.New(errs.KindValidationFailed, "local Agent token digest is invalid")
	}
	if err := recordcodec.ValidateTimestamp("local Agent token updated_at", record.TokenUpdatedAt); err != nil {
		return err
	}
	if record.TokenUpdatedAt.Before(record.CreatedAt) {
		return errs.New(errs.KindInternal, "local Agent token updated_at precedes creation")
	}
	return nil
}

func validateLocalAgentPrimary(record LocalAgentRecord) error {
	if err := recordcodec.ValidateID(ids.KindAgent, record.ID); err != nil {
		return err
	}
	if err := recordcodec.ValidateID(ids.KindTask, record.EnrollmentTaskID); err != nil {
		return err
	}
	if !imageref.IsDigestPinned(record.Image) {
		return errs.New(errs.KindValidationFailed, "local Agent image must be digest-pinned")
	}
	if record.Generation == 0 {
		return errs.New(errs.KindValidationFailed, "local Agent generation must be positive")
	}
	if err := recordcodec.ValidateTimestamp("local Agent created_at", record.CreatedAt); err != nil {
		return err
	}
	switch record.Phase {
	case LocalAgentPhaseProvisioning:
		if !record.ReadyAt.IsZero() {
			return errs.New(errs.KindValidationFailed, "provisioning local Agent ready_at must be empty")
		}
	case LocalAgentPhaseReady, LocalAgentPhaseUpdating, LocalAgentPhaseDeleting:
		if err := recordcodec.ValidateTimestamp("local Agent ready_at", record.ReadyAt); err != nil {
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

func ValidateLocalAgentConfig(config LocalAgentConfig) error {
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

func ValidLocalAgentDigest(digest string) bool {
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(digest)
	return err == nil && len(decoded) == sha256.Size &&
		base64.RawURLEncoding.EncodeToString(decoded) == digest
}

func LocalAgentKeys(agentID string, digest string) []string {
	return []string{
		LocalAgentSingletonKey,
		LocalAgentPrimaryKey(agentID),
		LocalAgentOwnerKey(agentID),
		LocalAgentConfigKey(agentID),
		LocalAgentTokenKey(agentID),
		LocalAgentDigestKey(digest),
	}
}

func LocalAgentPrimaryKey(agentID string) string { return localAgentPrimaryPrefix + agentID }
func LocalAgentOwnerKey(agentID string) string   { return localAgentOwnerPrefix + agentID }
func LocalAgentConfigKey(agentID string) string  { return localAgentConfigPrefix + agentID }
func LocalAgentTokenKey(agentID string) string   { return localAgentTokenPrefix + agentID }
func LocalAgentDigestKey(digest string) string   { return LocalAgentDigestPrefix + "~" + digest }

func LocalAgentDigestFromKey(key string) (string, error) {
	if !strings.HasPrefix(key, LocalAgentDigestPrefix+"~") {
		return "", recordcodec.CorruptRecord()
	}
	digest := strings.TrimPrefix(key, LocalAgentDigestPrefix+"~")
	if !ValidLocalAgentDigest(digest) {
		return "", recordcodec.CorruptRecord()
	}
	return digest, nil
}

func CloneLocalAgentRecord(record LocalAgentRecord) LocalAgentRecord {
	record.Config = CloneLocalAgentConfig(record.Config)
	record.EncryptedToken = append([]byte(nil), record.EncryptedToken...)
	return record
}

func CloneLocalAgentConfig(config LocalAgentConfig) LocalAgentConfig {
	config.Labels = maps.Clone(config.Labels)
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
