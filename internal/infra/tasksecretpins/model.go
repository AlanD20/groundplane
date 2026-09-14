// Package tasksecretpins owns preparation and release of exact Secret value
// memberships for recoverable operations. It stores no value bytes.
package tasksecretpins

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const MaximumPins = 4096

const (
	preparationPrefix = "/v1/staging/task-secret-pin-sets/"
	rootPrefix        = "/v1/records/task-secret-pin-sets/"
	taskPrefix        = "/v1/tasks/"
	activeTaskPrefix  = "/v1/indexes/tasks/active-operation/"
	releaseBatchSize  = 16
)

type RootPhase string

const (
	RootPhaseActive    RootPhase = "active"
	RootPhaseReleasing RootPhase = "releasing"
)

type setPhase string

const (
	phasePreparing  setPhase = "preparing"
	phaseSealed     setPhase = "sealed"
	phaseAbandoning setPhase = "abandoning"
	phaseActive     setPhase = "active"
	phaseReleasing  setPhase = "releasing"
)

type setRecord struct {
	Schema            uint32   `json:"schema"`
	OperationID       string   `json:"operation_id"`
	TaskID            string   `json:"task_id"`
	MembershipCount   uint64   `json:"membership_count"`
	MembershipSHA256  string   `json:"membership_sha256"`
	Phase             setPhase `json:"phase"`
	PreparationCursor uint64   `json:"preparation_cursor"`
	ReleaseCursor     uint64   `json:"release_cursor"`
}

type Prepared struct {
	record   setRecord
	revision int64
}

func (prepared Prepared) IsZero() bool              { return prepared.record.OperationID == "" }
func (prepared Prepared) OperationID() string       { return prepared.record.OperationID }
func (prepared Prepared) TaskID() string            { return prepared.record.TaskID }
func (prepared Prepared) MembershipCount() uint64   { return prepared.record.MembershipCount }
func (prepared Prepared) MembershipSHA256() string  { return prepared.record.MembershipSHA256 }
func (prepared Prepared) DescriptorRevision() int64 { return prepared.revision }

type ActiveRoot struct {
	record   setRecord
	revision int64
}

func (root ActiveRoot) OperationID() string      { return root.record.OperationID }
func (root ActiveRoot) TaskID() string           { return root.record.TaskID }
func (root ActiveRoot) MembershipCount() uint64  { return root.record.MembershipCount }
func (root ActiveRoot) MembershipSHA256() string { return root.record.MembershipSHA256 }
func (root ActiveRoot) Revision() int64          { return root.revision }
func (root ActiveRoot) Phase() RootPhase         { return RootPhase(root.record.Phase) }

func (prepared Prepared) ActiveRoot(revision int64) (ActiveRoot, error) {
	if revision <= 0 || validatePrepared(prepared) != nil {
		return ActiveRoot{}, validation("active Secret pin root reference is invalid")
	}
	record := prepared.record
	record.Phase = phaseActive
	return ActiveRoot{record: record, revision: revision}, nil
}

type Fragment struct {
	Conditions []Condition
	Mutations  []Mutation
}

func (fragment *Fragment) Clear() {
	if fragment == nil {
		return
	}
	clearMutations(fragment.Mutations)
	*fragment = Fragment{}
}

func PreparationKey(operationID string) string { return preparationPrefix + operationID }
func RootKey(operationID string) string        { return rootPrefix + operationID + "/root" }
func ReversePrefix(operationID string) string  { return rootPrefix + operationID + "/members/" }

func ReverseKey(operationID string, ordinal uint64) string {
	return ReversePrefix(operationID) + ordinalKey(ordinal)
}

func taskKey(taskID string) string                { return taskPrefix + taskID }
func activeTaskKey(operationID string) string     { return activeTaskPrefix + operationID }
func secretMetadataKey(secretID string) string    { return "/v1/records/secrets/" + secretID }
func secretValueKey(secretID string) string       { return "/v1/secret-values/secrets/" + secretID }
func secretTombstoneKey(secretID string) string   { return tombstoneKey("secret", secretID) }
func projectTombstoneKey(projectID string) string { return tombstoneKey("project", projectID) }
func tenantTombstoneKey(tenantID string) string   { return tombstoneKey("tenant", tenantID) }

func tombstoneKey(kind, id string) string { return "/v1/runtime/deletions/" + kind + "/" + id }

func canonicalPins(
	operationID string,
	taskID string,
	pins []tasksecretpinrecord.Record,
) ([]tasksecretpinrecord.Record, string, error) {
	if ids.Validate(ids.KindOperation, operationID) != nil || ids.Validate(ids.KindTask, taskID) != nil ||
		len(pins) == 0 || len(pins) > MaximumPins {
		return nil, "", validation("secret pin preparation identity is invalid")
	}
	canonical := append([]tasksecretpinrecord.Record(nil), pins...)
	hasher := sha256.New()
	previous := ""
	for _, pin := range canonical {
		if tasksecretpinrecord.Validate(pin) != nil || pin.OperationID != operationID ||
			(previous != "" && pin.SecretID <= previous) {
			return nil, "", validation("secret pins are invalid or not uniquely sorted")
		}
		value, err := tasksecretpinrecord.Encode(pin)
		if err != nil {
			return nil, "", err
		}
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(value)))
		_, _ = hasher.Write(size[:])
		_, _ = hasher.Write(value)
		previous = pin.SecretID
	}
	return canonical, hex.EncodeToString(hasher.Sum(nil)), nil
}

func validation(message string) error { return errs.New(errs.KindValidationFailed, message) }
func conflict(message string) error   { return errs.New(errs.KindStateConflict, message) }
func corruption(message string) error { return errs.New(errs.KindInternal, message) }

func clearMutations(mutations []Mutation) {
	for index := range mutations {
		clear(mutations[index].Value)
	}
}
