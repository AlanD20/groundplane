// Package acceptedactions owns bounded encrypted operator-request captures.
// Every request uses the same chunked format, including small requests.
package acceptedactions

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const MaximumCiphertextBytes = 3 << 20
const MaximumPlaintextBytes = 2 << 20
const PartPlaintextBytes = 32 << 10
const ChunkBytes = 48 << 10
const StagingRetention = time.Hour

type Record struct {
	TaskID           string              `json:"task_id"`
	Kind             string              `json:"kind"`
	Reference        workexecution.Input `json:"reference"`
	Bytes            uint32              `json:"bytes"`
	PlaintextBytes   uint32              `json:"plaintext_bytes"`
	Parts            []Part              `json:"parts"`
	CiphertextSHA256 string              `json:"ciphertext_sha256"`
	Chunks           uint16              `json:"chunks"`
	Stored           uint16              `json:"stored"`
	Sealed           bool                `json:"sealed"`
	CreatedAt        time.Time           `json:"created_at"`
	ExpiresAt        time.Time           `json:"expires_at"`
}

// Part is one independently protected envelope. Physical storage chunks may
// cross part boundaries; ordered hashes and lengths seal those boundaries.
type Part struct {
	Bytes          uint32 `json:"bytes"`
	PlaintextBytes uint32 `json:"plaintext_bytes"`
	SHA256         string `json:"sha256"`
}

func NewRecord(taskID, kind, planID string, plaintextBytes uint32, parts []Part, ciphertext []byte) (Record, error) {
	if len(ciphertext) == 0 || len(ciphertext) > MaximumCiphertextBytes {
		return Record{}, errs.New(errs.KindValidationFailed, "accepted action ciphertext is too large")
	}
	at := time.Now().UTC()
	digest := sha256.Sum256(ciphertext)
	record := Record{
		TaskID:           taskID,
		Kind:             kind,
		Reference:        workexecution.Input{ID: planID},
		Bytes:            uint32(len(ciphertext)),
		PlaintextBytes:   plaintextBytes,
		Parts:            slices.Clone(parts),
		CiphertextSHA256: hex.EncodeToString(digest[:]),
		Chunks: uint16(
			(len(ciphertext) + ChunkBytes - 1) / ChunkBytes,
		),
		CreatedAt: at,
		ExpiresAt: at.Add(StagingRetention),
	}
	commitment, err := record.commitment()
	if err != nil {
		return Record{}, err
	}
	record.Reference.SHA256 = commitment
	if err := record.Validate(); err != nil {
		return Record{}, err
	}
	return record, nil
}

func (record Record) commitment() (string, error) {
	metadata := struct {
		TaskID           string `json:"task_id"`
		Kind             string `json:"kind"`
		Bytes            uint32 `json:"bytes"`
		PlaintextBytes   uint32 `json:"plaintext_bytes"`
		Parts            []Part `json:"parts"`
		CiphertextSHA256 string `json:"ciphertext_sha256"`
	}{record.TaskID, record.Kind, record.Bytes, record.PlaintextBytes, record.Parts, record.CiphertextSHA256}
	value, err := json.Marshal(metadata)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:]), nil
}

func validDigest(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

type Snapshot struct {
	Record     Record
	Revision   int64
	Ciphertext []byte
}

type Store interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
	MeasureTransaction(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionBudget, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type Repository struct{ store Store }

func NewRepository(store Store) (*Repository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "accepted action storage is required")
	}
	return &Repository{store: store}, nil
}

func Prefix(taskID string) string  { return "/v1/execution/accepted-inputs/" + taskID + "/" }
func Key(taskID string) string     { return Prefix(taskID) + "manifest" }
func taskKey(taskID string) string { return "/v1/execution/tasks/" + taskID + "/record" }
func chunkKey(taskID string, index uint16) string {
	return fmt.Sprintf("%schunks/%04d", Prefix(taskID), index)
}

const expiryPrefix = "/v1/execution/accepted-input-expiry/"
const timeLayout = "20060102T150405.000000000Z"

func expiryKey(record Record) string {
	return expiryPrefix + record.ExpiresAt.Format(timeLayout) + "/" + record.TaskID
}

func (record Record) Validate() error {
	if ids.Validate(ids.KindTask, record.TaskID) != nil || record.Reference.Validate() != nil ||
		len(record.Kind) == 0 || len(record.Kind) > 100 || strings.ContainsAny(record.Kind, " \n\r\t/\\") ||
		record.Bytes == 0 || record.Bytes > MaximumCiphertextBytes ||
		record.PlaintextBytes == 0 || record.PlaintextBytes > MaximumPlaintextBytes ||
		len(record.Parts) == 0 || len(record.Parts) > (MaximumPlaintextBytes+PartPlaintextBytes-1)/PartPlaintextBytes ||
		!validDigest(record.CiphertextSHA256) ||
		record.Chunks != uint16((int(record.Bytes)+ChunkBytes-1)/ChunkBytes) ||
		record.Stored > record.Chunks || record.Sealed && record.Stored != record.Chunks ||
		!recordcodec.IsCanonicalUTC(record.CreatedAt) || !recordcodec.IsCanonicalUTC(record.ExpiresAt) ||
		!record.ExpiresAt.Equal(record.CreatedAt.Add(StagingRetention)) {
		return errs.New(errs.KindValidationFailed, "accepted action capture manifest is invalid")
	}
	var plaintextBytes, ciphertextBytes uint32
	for index, part := range record.Parts {
		if part.PlaintextBytes == 0 || part.PlaintextBytes > PartPlaintextBytes ||
			index < len(record.Parts)-1 && part.PlaintextBytes != PartPlaintextBytes ||
			part.Bytes <= part.PlaintextBytes || part.Bytes > ChunkBytes || !validDigest(part.SHA256) {
			return errs.New(errs.KindValidationFailed, "accepted action encrypted part is invalid")
		}
		plaintextBytes += part.PlaintextBytes
		ciphertextBytes += part.Bytes
	}
	commitment, err := record.commitment()
	if err != nil {
		return err
	}
	if plaintextBytes != record.PlaintextBytes || ciphertextBytes != record.Bytes ||
		commitment != record.Reference.SHA256 {
		return errs.New(
			errs.KindValidationFailed,
			"accepted action manifest differs from its immutable content reference",
		)
	}
	return nil
}

func encode(record Record) ([]byte, error) {
	if err := record.Validate(); err != nil {
		return nil, err
	}
	return recordcodec.Encode("accepted-action-input", record)
}

func (repository *Repository) commit(
	ctx context.Context,
	conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	budget, err := repository.store.MeasureTransaction(ctx, conditions, mutations)
	if err != nil {
		return keyvalue.TransactionResult{}, err
	}
	if !budget.Fits() {
		return keyvalue.TransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"accepted action capture exceeds atomic storage limits",
		)
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err == nil && result.Revision <= 0 {
		return keyvalue.TransactionResult{}, errs.New(
			errs.KindInternal,
			"accepted action staging returned no storage revision",
		)
	}
	if err == nil && !result.Succeeded {
		err = errs.New(errs.KindStateConflict, "accepted action capture changed while staging")
	}
	return result, err
}
