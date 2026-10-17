package volumeexecution

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	MaximumDirectoryCursorBytes = 16 * 1024
	DirectoryMutationBudget     = 128
	MaximumDirectoryBatchBytes  = 32 * 1024
)

// DirectoryBatch is durably saved before invoking one bounded filesystem batch.
// Its checkpoint authorizes deletion; its cursor only identifies traversal.
type DirectoryBatch struct {
	AttemptID  string              `json:"attempt_id"`
	VolumeID   string              `json:"volume_id"`
	Checkpoint workexecution.Input `json:"checkpoint"`
	Ordinal    uint64              `json:"ordinal"`
	Cursor     []byte              `json:"cursor,omitempty"`
	PreparedAt time.Time           `json:"prepared_at"`
}

type DirectoryResult struct {
	RequestSHA256 string    `json:"request_sha256"`
	NextCursor    []byte    `json:"next_cursor,omitempty"`
	Mutations     uint32    `json:"mutations"`
	Absent        bool      `json:"absent"`
	ObservedAt    time.Time `json:"observed_at"`
}

func (batch DirectoryBatch) Validate(attempt workexecution.Attempt, boundary Progress) error {
	if boundary.Stage != RemoveDirectory || !boundary.InFlight || batch.Ordinal == 0 ||
		len(batch.Cursor) > MaximumDirectoryCursorBytes || batch.Ordinal == 1 && len(batch.Cursor) != 0 ||
		batch.Ordinal > 1 && len(batch.Cursor) == 0 || batch.AttemptID != attempt.ID ||
		batch.PreparedAt.IsZero() || batch.PreparedAt.Before(boundary.UpdatedAt) || !batch.PreparedAt.Before(attempt.Deadline) {
		return invalidProgress()
	}
	checkpoint, err := boundary.Checkpoint(attempt, agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE, batch.VolumeID)
	if err != nil || checkpoint.Evidence != batch.Checkpoint {
		return invalidProgress()
	}
	return nil
}

func EncodeDirectoryBatch(attempt workexecution.Attempt, boundary Progress, batch DirectoryBatch) ([]byte, error) {
	if err := batch.Validate(attempt, boundary); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(batch)
	if err != nil || len(raw) > MaximumDirectoryBatchBytes {
		return nil, invalidProgress()
	}
	return jcs.Canonicalize(raw)
}

func DecodeDirectoryBatch(attempt workexecution.Attempt, boundary Progress, raw []byte) (DirectoryBatch, error) {
	if len(raw) == 0 || len(raw) > MaximumDirectoryBatchBytes {
		return DirectoryBatch{}, invalidProgress()
	}
	batch, err := jcs.Decode[DirectoryBatch](raw)
	if err != nil {
		return DirectoryBatch{}, invalidProgress()
	}
	canonical, err := EncodeDirectoryBatch(attempt, boundary, batch)
	if err != nil || !bytes.Equal(canonical, raw) {
		return DirectoryBatch{}, invalidProgress()
	}
	return batch, nil
}

func (result DirectoryResult) Validate(attempt workexecution.Attempt, boundary Progress, batch DirectoryBatch) error {
	raw, err := EncodeDirectoryBatch(attempt, boundary, batch)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(raw)
	if result.RequestSHA256 != hex.EncodeToString(digest[:]) || result.Mutations > DirectoryMutationBudget ||
		len(result.NextCursor) > MaximumDirectoryCursorBytes || result.Absent && len(result.NextCursor) != 0 ||
		!result.Absent && (len(result.NextCursor) == 0 || bytes.Equal(result.NextCursor, batch.Cursor)) ||
		result.ObservedAt.IsZero() || result.ObservedAt.Before(batch.PreparedAt) {
		return invalidProgress()
	}
	return nil
}

func EncodeDirectoryResult(attempt workexecution.Attempt, boundary Progress, batch DirectoryBatch, result DirectoryResult) ([]byte, error) {
	if err := result.Validate(attempt, boundary, batch); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(result)
	if err != nil || len(raw) > MaximumDirectoryBatchBytes {
		return nil, invalidProgress()
	}
	return jcs.Canonicalize(raw)
}

func DecodeDirectoryResult(attempt workexecution.Attempt, boundary Progress, batch DirectoryBatch, raw []byte) (DirectoryResult, error) {
	if len(raw) == 0 || len(raw) > MaximumDirectoryBatchBytes {
		return DirectoryResult{}, invalidProgress()
	}
	result, err := jcs.Decode[DirectoryResult](raw)
	if err != nil {
		return DirectoryResult{}, invalidProgress()
	}
	canonical, err := EncodeDirectoryResult(attempt, boundary, batch, result)
	if err != nil || !bytes.Equal(canonical, raw) {
		return DirectoryResult{}, invalidProgress()
	}
	return result, nil
}
