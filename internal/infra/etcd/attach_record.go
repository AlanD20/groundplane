package etcd

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	MaximumAttachNameBytes           = 255
	MaximumAttachGrants              = 8
	MaximumAttachFactsPerSet         = 32
	MaximumAttachFactCiphertextBytes = 256 << 10
)

var (
	attachFactKeyPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,127}$`)
	attachNamePattern    = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)
)

type AttachOperation string

const (
	AttachOperationProvision AttachOperation = "provision"
	AttachOperationDetach    AttachOperation = "detach"
)

// AttachFactDefinition is listable, non-secret schema metadata. It never
// contains a rendered fact value.
type AttachFactDefinition struct {
	Key    string `json:"key"`
	Secret bool   `json:"secret"`
}

// AttachFactSetMetadata distinguishes the owning Attach's facts from the
// additional connection sets produced for grants. Empty GrantAttachID selects
// the owning Attach's database.
type AttachFactSetMetadata struct {
	GrantAttachID string                 `json:"grant_attach_id,omitempty"`
	Facts         []AttachFactDefinition `json:"facts"`
}

// AttachRecord stores only stable ownership, lifecycle, the resolved backing
// network binding, and listable fact schema. Rendered fact bytes live in the
// separately encrypted envelope.
type AttachRecord struct {
	ID                   string                  `json:"id"`
	EnvironmentID        string                  `json:"environment_id"`
	Name                 string                  `json:"name"`
	BackingProjectID     string                  `json:"backing_project_id"`
	BackingEnvironmentID string                  `json:"backing_environment_id"`
	BackingServiceID     string                  `json:"backing_service_id"`
	BackingNetworkID     string                  `json:"backing_network_id"`
	ServiceIDs           []string                `json:"service_ids"`
	GrantAttachIDs       []string                `json:"grant_attach_ids,omitempty"`
	FactSets             []AttachFactSetMetadata `json:"fact_sets,omitempty"`
	Status               core.AttachStatus       `json:"status"`
	Operation            AttachOperation         `json:"operation"`
	TaskID               string                  `json:"task_id"`
	CreatedAt            time.Time               `json:"created_at"`
}

// AttachEncryptedFacts is an opaque Controller-key envelope. Plaintext shape
// and cryptographic operations belong to the application layer.
type AttachEncryptedFacts struct {
	AttachID         string `json:"attach_id"`
	EnvelopeVersion  uint8  `json:"envelope_version"`
	Cipher           string `json:"cipher"`
	DigestAlgorithm  string `json:"digest_algorithm"`
	CiphertextSHA256 string `json:"ciphertext_sha256"`
	Ciphertext       []byte `json:"ciphertext"`
}

func NewPendingAttachRecord(
	id string,
	environmentID string,
	name string,
	backingProjectID string,
	backingEnvironmentID string,
	backingServiceID string,
	backingNetworkID string,
	serviceIDs []string,
	grantAttachIDs []string,
	factSets []AttachFactSetMetadata,
	taskID string,
	createdAt time.Time,
) (AttachRecord, error) {
	record := AttachRecord{
		ID:                   id,
		EnvironmentID:        environmentID,
		Name:                 name,
		BackingProjectID:     backingProjectID,
		BackingEnvironmentID: backingEnvironmentID,
		BackingServiceID:     backingServiceID,
		BackingNetworkID:     backingNetworkID,
		ServiceIDs:           append([]string(nil), serviceIDs...),
		GrantAttachIDs:       append([]string(nil), grantAttachIDs...),
		FactSets:             cloneAttachFactSets(factSets),
		Status:               core.AttachPending,
		Operation:            AttachOperationProvision,
		TaskID:               taskID,
		CreatedAt:            createdAt.UTC(),
	}
	slices.Sort(record.ServiceIDs)
	slices.Sort(record.GrantAttachIDs)
	slices.SortFunc(record.FactSets, func(left AttachFactSetMetadata, right AttachFactSetMetadata) int {
		return strings.Compare(left.GrantAttachID, right.GrantAttachID)
	})
	for index := range record.FactSets {
		slices.SortFunc(record.FactSets[index].Facts, func(left AttachFactDefinition, right AttachFactDefinition) int {
			return strings.Compare(left.Key, right.Key)
		})
	}
	if err := validateAttachRecord(record); err != nil {
		return AttachRecord{}, err
	}
	return record, nil
}

func NewAttachEncryptedFacts(
	attachID string,
	envelopeVersion uint8,
	cipher string,
	digestAlgorithm string,
	ciphertext []byte,
) (AttachEncryptedFacts, error) {
	digest := sha256.Sum256(ciphertext)
	value := AttachEncryptedFacts{
		AttachID:         attachID,
		EnvelopeVersion:  envelopeVersion,
		Cipher:           cipher,
		DigestAlgorithm:  digestAlgorithm,
		CiphertextSHA256: hex.EncodeToString(digest[:]),
		Ciphertext:       append([]byte(nil), ciphertext...),
	}
	if err := validateAttachEncryptedFacts(value); err != nil {
		clear(value.Ciphertext)
		return AttachEncryptedFacts{}, err
	}
	return value, nil
}

func MarkAttachProvisioning(record AttachRecord, taskID string) (AttachRecord, error) {
	if record.Status != core.AttachPending || record.Operation != AttachOperationProvision || record.TaskID != taskID {
		return AttachRecord{}, attachStateError(record, "cannot start provisioning")
	}
	record.Status = core.AttachProvisioning
	if err := validateAttachRecord(record); err != nil {
		return AttachRecord{}, err
	}
	return record, nil
}

func AbortPendingAttachProvisioning(record AttachRecord, taskID string) (AttachRecord, error) {
	if record.Status != core.AttachPending || record.Operation != AttachOperationProvision || record.TaskID != taskID {
		return AttachRecord{}, attachStateError(record, "cannot abort pending provisioning")
	}
	record.Status = core.AttachFailed
	if err := validateAttachRecord(record); err != nil {
		return AttachRecord{}, err
	}
	return record, nil
}

func CompleteAttachProvisioning(record AttachRecord, taskID string, succeeded bool) (AttachRecord, error) {
	if record.Status != core.AttachProvisioning || record.Operation != AttachOperationProvision ||
		record.TaskID != taskID {
		return AttachRecord{}, attachStateError(record, "cannot complete provisioning")
	}
	if succeeded {
		record.Status = core.AttachReady
	} else {
		record.Status = core.AttachFailed
	}
	if err := validateAttachRecord(record); err != nil {
		return AttachRecord{}, err
	}
	return record, nil
}

func RetryAttachOperation(record AttachRecord, taskID string) (AttachRecord, error) {
	if record.Status != core.AttachFailed {
		return AttachRecord{}, attachStateError(record, "cannot retry operation")
	}
	if err := validateAttachStableID(ids.KindTask, taskID, "Attach retry task"); err != nil {
		return AttachRecord{}, err
	}
	record.TaskID = taskID
	switch record.Operation {
	case AttachOperationProvision:
		record.Status = core.AttachPending
	case AttachOperationDetach:
		record.Status = core.AttachDetaching
	default:
		return AttachRecord{}, attachStateError(record, "cannot retry unknown operation")
	}
	if err := validateAttachRecord(record); err != nil {
		return AttachRecord{}, err
	}
	return record, nil
}

func BeginAttachDetaching(record AttachRecord, taskID string) (AttachRecord, error) {
	if record.Status != core.AttachReady &&
		(record.Status != core.AttachFailed || record.Operation != AttachOperationProvision) {
		return AttachRecord{}, attachStateError(record, "cannot begin detaching")
	}
	if err := validateAttachStableID(ids.KindTask, taskID, "Attach detach task"); err != nil {
		return AttachRecord{}, err
	}
	record.Status = core.AttachDetaching
	record.Operation = AttachOperationDetach
	record.TaskID = taskID
	if err := validateAttachRecord(record); err != nil {
		return AttachRecord{}, err
	}
	return record, nil
}

func CompleteAttachDetaching(record AttachRecord, taskID string, succeeded bool) (AttachRecord, error) {
	if record.Status != core.AttachDetaching || record.Operation != AttachOperationDetach || record.TaskID != taskID {
		return AttachRecord{}, attachStateError(record, "cannot complete detaching")
	}
	if succeeded {
		record.Status = core.AttachDetached
	} else {
		record.Status = core.AttachFailed
	}
	if err := validateAttachRecord(record); err != nil {
		return AttachRecord{}, err
	}
	return record, nil
}

func validateAttachRecord(record AttachRecord) error {
	for _, check := range []struct {
		kind  ids.Kind
		value string
		field string
	}{
		{ids.KindAttach, record.ID, "Attach"},
		{ids.KindEnvironment, record.EnvironmentID, "Attach environment"},
		{ids.KindProject, record.BackingProjectID, "Attach backing project"},
		{ids.KindEnvironment, record.BackingEnvironmentID, "Attach backing environment"},
		{ids.KindService, record.BackingServiceID, "Attach backing service"},
		{ids.KindNetwork, record.BackingNetworkID, "Attach backing network"},
		{ids.KindTask, record.TaskID, "Attach task"},
	} {
		if err := validateAttachStableID(check.kind, check.value, check.field); err != nil {
			return err
		}
	}
	if err := ValidateAttachName(record.Name); err != nil {
		return err
	}
	if record.CreatedAt.IsZero() {
		return errs.New(errs.KindValidationFailed, "Attach created_at is required")
	}
	if len(record.ServiceIDs) != 1 {
		return errs.New(errs.KindValidationFailed, "Attach must have exactly one consuming Service")
	}
	if len(record.GrantAttachIDs) > MaximumAttachGrants {
		return errs.Newf(errs.KindValidationFailed, "Attach may have at most %d grants", MaximumAttachGrants)
	}
	if err := validateSortedStableIDs(record.ServiceIDs, ids.KindService, "Attach service_ids"); err != nil {
		return err
	}
	if err := validateSortedStableIDs(record.GrantAttachIDs, ids.KindAttach, "Attach grant_attach_ids"); err != nil {
		return err
	}
	for _, grantID := range record.GrantAttachIDs {
		if grantID == record.ID {
			return errs.New(errs.KindValidationFailed, "Attach cannot grant itself")
		}
	}
	if err := validateAttachFactSets(record.FactSets, record.GrantAttachIDs); err != nil {
		return err
	}
	switch record.Status {
	case core.AttachPending, core.AttachProvisioning:
		if record.Operation != AttachOperationProvision {
			return attachStateError(record, "provisioning state has the wrong operation")
		}
	case core.AttachReady:
		if record.Operation != AttachOperationProvision {
			return attachStateError(record, "ready state has the wrong operation")
		}
	case core.AttachDetaching, core.AttachDetached:
		if record.Operation != AttachOperationDetach {
			return attachStateError(record, "detach state has the wrong operation")
		}
	case core.AttachFailed:
		if record.Operation != AttachOperationProvision && record.Operation != AttachOperationDetach {
			return attachStateError(record, "failed state has an unknown operation")
		}
	default:
		return attachStateError(record, "unknown lifecycle state")
	}
	return nil
}

func ValidateAttachName(name string) error {
	if len(name) == 0 || len(name) > MaximumAttachNameBytes || !attachNamePattern.MatchString(name) {
		return errs.Newf(
			errs.KindValidationFailed,
			"Attach name must be a lowercase ASCII hyphen label of 1-%d bytes",
			MaximumAttachNameBytes,
		)
	}
	return nil
}

func validateAttachEncryptedFacts(value AttachEncryptedFacts) error {
	if err := validateAttachStableID(ids.KindAttach, value.AttachID, "Attach fact envelope"); err != nil {
		return err
	}
	if value.EnvelopeVersion != 1 || value.Cipher != "age-x25519" || value.DigestAlgorithm != "sha256" {
		return errs.New(errs.KindValidationFailed, "Attach fact envelope metadata is unsupported")
	}
	if len(value.Ciphertext) == 0 || len(value.Ciphertext) > MaximumAttachFactCiphertextBytes {
		return errs.Newf(
			errs.KindValidationFailed,
			"Attach fact ciphertext must be between 1 and %d bytes",
			MaximumAttachFactCiphertextBytes,
		)
	}
	provided, err := hex.DecodeString(value.CiphertextSHA256)
	if err != nil || len(provided) != sha256.Size {
		return errs.New(errs.KindValidationFailed, "Attach fact ciphertext digest is invalid")
	}
	actual := sha256.Sum256(value.Ciphertext)
	if subtle.ConstantTimeCompare(provided, actual[:]) != 1 {
		return errs.New(errs.KindValidationFailed, "Attach fact ciphertext digest does not match")
	}
	return nil
}

func validateAttachFactSets(factSets []AttachFactSetMetadata, grantAttachIDs []string) error {
	if len(factSets) == 0 {
		if len(grantAttachIDs) != 0 {
			return errs.New(errs.KindValidationFailed, "Attach grants require corresponding fact sets")
		}
		return nil
	}
	if len(factSets) != len(grantAttachIDs)+1 {
		return errs.New(errs.KindValidationFailed, "Attach fact sets must contain the owning set and every grant set")
	}
	expected := append([]string{""}, grantAttachIDs...)
	for index, factSet := range factSets {
		if factSet.GrantAttachID != expected[index] {
			return errs.New(errs.KindValidationFailed, "Attach fact sets are not in canonical grant order")
		}
		if len(factSet.Facts) == 0 || len(factSet.Facts) > MaximumAttachFactsPerSet {
			return errs.Newf(
				errs.KindValidationFailed,
				"Attach fact set must have between 1 and %d facts",
				MaximumAttachFactsPerSet,
			)
		}
		prior := ""
		for _, fact := range factSet.Facts {
			if !attachFactKeyPattern.MatchString(fact.Key) || fact.Key <= prior {
				return errs.New(errs.KindValidationFailed, "Attach fact keys must be valid, unique, and sorted")
			}
			prior = fact.Key
		}
	}
	return nil
}

func validateSortedStableIDs(values []string, kind ids.Kind, field string) error {
	prior := ""
	for _, value := range values {
		if err := validateAttachStableID(kind, value, field); err != nil {
			return err
		}
		if value <= prior {
			return errs.Newf(errs.KindValidationFailed, "%s must be unique and sorted", field)
		}
		prior = value
	}
	return nil
}

func validateAttachStableID(kind ids.Kind, value string, field string) error {
	if err := ids.Validate(kind, value); err != nil {
		return errs.Newf(errs.KindValidationFailed, "%s has an invalid stable id", field)
	}
	return nil
}

func attachStateError(record AttachRecord, message string) error {
	return errs.Newf(errs.KindStateConflict, "%s for Attach %s in state %s", message, record.ID, record.Status)
}

func cloneAttachFactSets(factSets []AttachFactSetMetadata) []AttachFactSetMetadata {
	if factSets == nil {
		return nil
	}
	cloned := make([]AttachFactSetMetadata, len(factSets))
	for index, factSet := range factSets {
		cloned[index] = AttachFactSetMetadata{
			GrantAttachID: factSet.GrantAttachID,
			Facts:         append([]AttachFactDefinition(nil), factSet.Facts...),
		}
	}
	return cloned
}
