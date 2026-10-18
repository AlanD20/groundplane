package componentplanning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const ProxyCandidateInputChunkBytes = 256 * 1024

type ProxyInputOwner string

const (
	ProxyInputAgentWork            ProxyInputOwner = "agent_work"
	ProxyInputControllerSettlement ProxyInputOwner = "controller_settlement"
)

// ProxyCandidateInputReference selects inert preparation, never applied runtime.
// Its content-addressed chunks live inside the parent's desired revision, so
// they share its retention and cleanup owner rather than creating another GC.
type ProxyCandidateInputReference struct {
	EnvironmentID string          `json:"environment_id"`
	ParentTaskID  string          `json:"parent_task_id"`
	SourceID      string          `json:"source_id"`
	SourceOwner   ProxyInputOwner `json:"source_owner"`
	ComponentID   string          `json:"component_id"`
	SHA256        string          `json:"sha256"`
	ByteSize      int             `json:"byte_size"`
}

func ProxyCandidateInputKey(reference ProxyCandidateInputReference) string {
	return blueprints.EnvironmentBlueprintRevisionPrefixFinal(reference.EnvironmentID, reference.ParentTaskID) +
		"proxy-candidate-inputs/" + reference.SourceID + "/" + reference.SHA256 + "/root"
}

func ProxyCandidateInputChunkKey(reference ProxyCandidateInputReference, ordinal int) string {
	return blueprints.EnvironmentBlueprintRevisionPrefixFinal(reference.EnvironmentID, reference.ParentTaskID) +
		"proxy-candidate-inputs/" + reference.SourceID + "/" + reference.SHA256 + "/chunks/" + strconv.Itoa(ordinal)
}

func (reference ProxyCandidateInputReference) Validate() error {
	digest, err := hex.DecodeString(reference.SHA256)
	ownerValid := reference.SourceOwner == ProxyInputAgentWork &&
		ids.Validate(ids.KindWorkItem, reference.SourceID) == nil ||
		reference.SourceOwner == ProxyInputControllerSettlement &&
			ids.Validate(ids.KindOperation, reference.SourceID) == nil
	if ids.Validate(ids.KindEnvironment, reference.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, reference.ParentTaskID) != nil || !ownerValid ||
		reference.SourceID == reference.ParentTaskID || ids.Validate(ids.KindComponent, reference.ComponentID) != nil ||
		err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != reference.SHA256 ||
		reference.ByteSize <= 0 || reference.ByteSize > projectionrecord.EnvironmentBlueprintProjectionMaxBytes {
		return errs.New(errs.KindStateConflict, "prepared router Environment input reference is invalid")
	}
	return nil
}

func (reference ProxyCandidateInputReference) ValidateProjection(
	projection projectionrecord.EnvironmentComposeProjection,
) error {
	if err := reference.Validate(); err != nil {
		return err
	}
	if projection.EnvironmentID != reference.EnvironmentID || projection.RevisionID != reference.ParentTaskID {
		return errs.New(errs.KindStateConflict, "prepared router Environment input scope changed")
	}
	value, err := projectionrecord.EncodePreparedEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		return err
	}
	defer clear(value)
	digest := sha256.Sum256(value)
	if len(value) != reference.ByteSize || hex.EncodeToString(digest[:]) != reference.SHA256 {
		return errs.New(
			errs.KindStateConflict,
			"Environment runtime inputs changed after router preparation; Apply the current Blueprint again",
		)
	}
	return nil
}

func ReadProxyCandidateInput(ctx context.Context, store planningStore,
	reference ProxyCandidateInputReference, revision int64,
) (keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection], error) {
	if ctx == nil || store == nil || revision < 0 {
		return keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection]{}, errs.New(
			errs.KindValidationFailed,
			"prepared router input read is invalid",
		)
	}
	if err := reference.Validate(); err != nil {
		return keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection]{}, err
	}
	root, err := store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{ProxyCandidateInputKey(reference)}, Revision: revision},
	)
	if err != nil {
		return keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection]{}, err
	}
	if root == nil || len(root.Values) != 1 || root.Values[0] == nil ||
		root.Values[0].Key != ProxyCandidateInputKey(reference) ||
		root.ReadRevision <= 0 ||
		revision > 0 && root.ReadRevision != revision {
		return keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection]{}, errs.New(
			errs.KindStateConflict,
			"prepared router Environment input is missing",
		)
	}
	defer keyvalue.ClearValues(root.Values)
	stored, err := recordcodec.Decode[ProxyCandidateInputReference](root.Values[0].Value, "proxy-candidate-input")
	if err != nil || stored != reference {
		return keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection]{}, errs.New(
			errs.KindStateConflict,
			"prepared router input reference changed",
		)
	}
	count := (reference.ByteSize + ProxyCandidateInputChunkBytes - 1) / ProxyCandidateInputChunkBytes
	keys := make([]string, count)
	for ordinal := range keys {
		keys[ordinal] = ProxyCandidateInputChunkKey(reference, ordinal)
	}
	chunks, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: root.ReadRevision})
	if err != nil {
		return keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection]{}, err
	}
	if chunks == nil || chunks.ReadRevision != root.ReadRevision || len(chunks.Values) != count {
		return keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection]{}, errs.New(
			errs.KindStateConflict,
			"prepared router input chunks are incomplete",
		)
	}
	defer keyvalue.ClearValues(chunks.Values)
	value := make([]byte, 0, reference.ByteSize)
	defer func() { clear(value) }()
	for ordinal, chunk := range chunks.Values {
		size := min(ProxyCandidateInputChunkBytes, reference.ByteSize-ordinal*ProxyCandidateInputChunkBytes)
		if chunk == nil || chunk.Key != keys[ordinal] || len(chunk.Value) != size {
			return keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection]{}, errs.New(
				errs.KindStateConflict,
				"prepared router input chunk changed",
			)
		}
		value = append(value, chunk.Value...)
	}
	digest := sha256.Sum256(value)
	if hex.EncodeToString(digest[:]) != reference.SHA256 {
		return keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection]{}, errs.New(
			errs.KindStateConflict,
			"prepared router input digest changed",
		)
	}
	projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(value)
	if err != nil {
		return keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection]{}, err
	}
	if err := reference.ValidateProjection(projection); err != nil {
		return keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection]{}, err
	}
	return keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection]{
		Record:       projection,
		Revision:     root.Values[0].ModRevision,
		ReadRevision: root.ReadRevision,
	}, nil
}

func BindWorkProxyCandidateInput(task workexecution.Task, item workexecution.Item,
	preflight ProxyServicePreflight,
	reference keyvalue.Versioned[ProxyCandidateInputReference],
) (*ProxyServicePreflight, keyvalue.Condition, error) {
	if reference.Revision <= 0 || reference.ReadRevision < reference.Revision || reference.Record.Validate() != nil ||
		task.Validate() != nil || item.Validate() != nil || preflight.validate() != nil ||
		task.Action != workexecution.ActionApply || item.TaskID != task.ID ||
		item.Operation != workexecution.Component || item.Placement.Executor != workexecution.Agent ||
		(item.Phase != workexecution.Waiting && item.Phase != workexecution.Ready) ||
		item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.ForwardRevoked ||
		task.Stop != nil || task.FinishedAt != nil ||
		reference.Record.SourceOwner != ProxyInputAgentWork || item.ID != reference.Record.SourceID ||
		task.Scope.EnvironmentID != reference.Record.EnvironmentID || task.ID != reference.Record.ParentTaskID ||
		preflight.ParentTaskID != task.ID || preflight.EnvironmentID != task.Scope.EnvironmentID ||
		item.TargetID != reference.Record.ComponentID {
		return nil, keyvalue.Condition{}, errs.New(
			errs.KindValidationFailed,
			"prepared router input differs from its Component work",
		)
	}
	if preflight.CandidateInput != nil && *preflight.CandidateInput != reference.Record {
		return nil, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Component work already captured different router input")
	}
	result := cloneProxyServicePreflight(&preflight)
	owned := reference.Record
	result.CandidateInput = &owned
	return result, keyvalue.Condition{Key: ProxyCandidateInputKey(owned), ModRevision: reference.Revision}, nil
}

// Runtime publication consumes only the original router's actual receipt. A
// prepared input cannot satisfy the receipt prerequisite; it merely constrains
// the complete input that may be sealed after those effects are acknowledged.
func ValidateBlueprintProxyCandidateProjection(ctx context.Context, store planningStore,
	receipt blueprintunits.AppliedRecord, receiptRevision, readRevision int64,
	projection projectionrecord.EnvironmentComposeProjection,
) (*keyvalue.Condition, error) {
	preflight, err := readAppliedProxyServicePreflight(ctx, store, receipt, receiptRevision, readRevision)
	if err != nil || preflight == nil || preflight.CandidateInput == nil {
		return nil, err
	}
	reference := *preflight.CandidateInput
	if err := reference.ValidateProjection(projection); err != nil {
		return nil, err
	}
	stored, err := ReadProxyCandidateInput(ctx, store, reference, readRevision)
	if err != nil {
		return nil, err
	}
	return &keyvalue.Condition{Key: ProxyCandidateInputKey(reference), ModRevision: stored.Revision}, nil
}
