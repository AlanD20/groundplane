package runners

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const runnerReadinessProofPrefix = "/v1/runtime/runner-readiness-proofs/"

// RunnerReadinessProofRecord is the single-use durable witness produced after
// the active Runner creation Task proves its exact materialized runtime ready.
type RunnerReadinessProofRecord struct {
	RunnerID                 string `json:"runner_id"`
	TaskID                   string `json:"task_id"`
	RuntimeEpoch             uint64 `json:"runtime_epoch"`
	ContainerID              string `json:"container_id"`
	RuntimeOwnershipRevision int64  `json:"runtime_ownership_revision"`
	RuntimeOwnershipSHA256   string `json:"runtime_ownership_sha256"`
}

func validateRunnerReadinessProof(record RunnerReadinessProofRecord) error {
	if ids.Validate(ids.KindRunner, record.RunnerID) != nil ||
		ids.Validate(ids.KindTask, record.TaskID) != nil || record.RuntimeEpoch == 0 ||
		!ValidLowerHex(record.ContainerID, RunnerContainerIDEncodedLength) ||
		record.RuntimeOwnershipRevision <= 0 || !ValidLowerHex(record.RuntimeOwnershipSHA256, sha256.Size*2) {
		return errs.New(errs.KindValidationFailed, "runner readiness proof is invalid")
	}
	return nil
}

func EncodeRunnerReadinessProof(record RunnerReadinessProofRecord) ([]byte, error) {
	if err := validateRunnerReadinessProof(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("runner_readiness_proof", record)
}

func DecodeRunnerReadinessProof(value []byte) (RunnerReadinessProofRecord, error) {
	if len(value) > MaximumRunnerPersistenceBytes {
		return RunnerReadinessProofRecord{}, errs.New(errs.KindInternal, "runner readiness proof is corrupt")
	}
	record, err := recordcodec.Decode[RunnerReadinessProofRecord](value, "runner_readiness_proof")
	if err != nil || validateRunnerReadinessProof(record) != nil {
		return RunnerReadinessProofRecord{}, errs.New(errs.KindInternal, "runner readiness proof is corrupt")
	}
	return record, nil
}

func RunnerRuntimeOwnershipSHA256(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func RunnerReadinessProofKey(taskID string) string { return runnerReadinessProofPrefix + taskID }
