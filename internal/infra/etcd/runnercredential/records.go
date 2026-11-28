package runnercredential

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	identity "github.com/AlanD20/groundplane/internal/common/runnercredential"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	credentialPrefix   = "/v1/runner-credentials/by-digest/"
	runnerPrefix       = "/v1/runner-credentials/by-runner/"
	credentialKind     = "runner_credential"
	credentialSetKind  = "runner_credential_set"
	maximumCredentials = 32
	maximumRecordBytes = 32 << 10
)

type credentialRecord struct {
	Digest      string    `json:"digest"`
	RunnerID    string    `json:"runner_id"`
	RequestID   string    `json:"request_id,omitempty"`
	RunnerEpoch uint64    `json:"runner_epoch"`
	ExpiresAt   time.Time `json:"expires_at"`
	EpochBound  bool      `json:"epoch_bound,omitempty"`
}

type credentialSet struct {
	RunnerID string   `json:"runner_id"`
	Digests  []string `json:"digests"`
}

func credentialKey(digest [sha256.Size]byte) string {
	return credentialPrefix + hex.EncodeToString(digest[:])
}

func runnerKey(runnerID string) string { return runnerPrefix + runnerID }

func (record credentialRecord) binding(revision int64) identity.Binding {
	return identity.Binding{
		RunnerID: record.RunnerID, RequestID: record.RequestID, RunnerEpoch: record.RunnerEpoch,
		Revision: revision, ExpiresAt: record.ExpiresAt, EpochBound: record.EpochBound,
	}
}

func validIdentity(runnerID, requestID string) bool {
	return ids.Validate(ids.KindRunner, runnerID) == nil &&
		(requestID == "" || ids.Validate(ids.KindOperation, requestID) == nil)
}

func validCredentialRecord(record credentialRecord) bool {
	binding := record.binding(0)
	return validIdentity(record.RunnerID, record.RequestID) && record.RunnerEpoch != 0 &&
		binding.ActiveAt(time.Time{}) && (record.EpochBound || recordcodec.IsCanonicalUTC(record.ExpiresAt)) &&
		recordcodec.ValidNonZeroSHA256(record.Digest)
}

func decodeCredential(value *keyvalue.KeyValue, expectedKey string) (credentialRecord, error) {
	if value == nil || value.Key != expectedKey || value.ModRevision <= 0 || len(value.Value) > maximumRecordBytes {
		return credentialRecord{}, corrupt()
	}
	record, err := recordcodec.Decode[credentialRecord](value.Value, credentialKind)
	if err != nil || !validCredentialRecord(record) ||
		record.Digest != strings.TrimPrefix(expectedKey, credentialPrefix) {
		return credentialRecord{}, corrupt()
	}
	return record, nil
}

func decodeSet(value *keyvalue.KeyValue, runnerID string) (credentialSet, error) {
	if value == nil {
		return credentialSet{RunnerID: runnerID}, nil
	}
	if value.Key != runnerKey(runnerID) || value.ModRevision <= 0 || len(value.Value) > maximumRecordBytes {
		return credentialSet{}, corrupt()
	}
	record, err := recordcodec.Decode[credentialSet](value.Value, credentialSetKind)
	if err != nil || record.RunnerID != runnerID || len(record.Digests) == 0 ||
		len(record.Digests) > maximumCredentials {
		return credentialSet{}, corrupt()
	}
	seen := make(map[string]bool, len(record.Digests))
	for _, digest := range record.Digests {
		if seen[digest] || !recordcodec.ValidNonZeroSHA256(digest) {
			return credentialSet{}, corrupt()
		}
		seen[digest] = true
	}
	return record, nil
}

func corrupt() error {
	return errs.New(errs.KindInternal, "Runner credential records are invalid")
}
