package etcd

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const scriptPrefix = "/v1/records/scripts/"

type storedScriptDesired struct {
	ID          string          `json:"id"`
	Slug        string          `json:"slug"`
	ServiceName string          `json:"service"`
	When        core.ScriptHook `json:"when"`
}

type storedScriptRecord struct {
	EnvironmentID     string              `json:"environment_id"`
	ServiceID         string              `json:"service_id"`
	Origin            string              `json:"origin"`
	ReconciliationKey string              `json:"reconciliation_key,omitempty"`
	ActiveGeneration  uint64              `json:"active_generation"`
	ActiveReferences  uint64              `json:"active_references"`
	Desired           storedScriptDesired `json:"desired"`
}

// ScriptBodyGenerationRecord is one append-only Script body generation.
type ScriptBodyGenerationRecord struct {
	ScriptID   string `json:"script_id"`
	Generation uint64 `json:"generation"`
	BodySize   uint32 `json:"body_size"`
	Body       string `json:"script"`
	BodySHA256 string `json:"body_sha256"`
}

// ScriptRecord is the durable boundary for one Environment-scoped Script.
// ServiceID is stable; Desired.ServiceName is the target's public label projection.
type ScriptRecord struct {
	EnvironmentID     string      `json:"environment_id"`
	ServiceID         string      `json:"service_id"`
	Origin            string      `json:"origin"`
	ReconciliationKey string      `json:"reconciliation_key,omitempty"`
	ActiveGeneration  uint64      `json:"active_generation"`
	ActiveReferences  uint64      `json:"active_references"`
	Desired           core.Script `json:"desired"`
}

func NewScriptRecord(environmentID string, serviceID string, desired core.Script) (ScriptRecord, error) {
	record := ScriptRecord{
		EnvironmentID: environmentID, ServiceID: serviceID, Origin: "api",
		ActiveGeneration: 1, Desired: desired,
	}
	if err := validateScriptRecord(record); err != nil {
		return ScriptRecord{}, err
	}
	return record, nil
}

// ReplaceScriptDesired changes authored behavior while preserving identity, ownership, and target.
func ReplaceScriptDesired(record ScriptRecord, desired core.Script) (ScriptRecord, error) {
	if err := validateScriptRecord(record); err != nil {
		return ScriptRecord{}, err
	}
	if desired.ID != record.Desired.ID {
		return ScriptRecord{}, errs.New(
			errs.KindValidationFailed,
			"Script replacement changed immutable identity",
		)
	}
	replacement := record
	replacement.Desired = desired
	if desired.Body != record.Desired.Body {
		if record.ActiveGeneration == ^uint64(0) {
			return ScriptRecord{}, errs.New(errs.KindStateConflict, "Script generation is exhausted")
		}
		replacement.ActiveGeneration++
	}
	if err := validateScriptRecord(replacement); err != nil {
		return ScriptRecord{}, err
	}
	return replacement, nil
}

func scriptKey(id string) string { return scriptPrefix + id }

func scriptBodyGenerationPrefix(id string) string { return scriptPrefix + id + "/generations/" }

func scriptBodyGenerationKey(id string, generation uint64) string {
	return scriptBodyGenerationPrefix(id) + strconv.FormatUint(generation, 10)
}

func scriptOwnerPrefix(environmentID string) string {
	return "/v1/indexes/scripts/by-owner/environment/" + environmentID + "/"
}

func scriptOwnerKey(environmentID string, scriptID string) string {
	return scriptOwnerPrefix(environmentID) + scriptID
}

func scriptSlugKey(environmentID string, slug string) string {
	return "/v1/indexes/scripts/by-slug/environment/" + environmentID + "/" + encodeDynamicSegment(slug)
}

func validateScriptRecord(record ScriptRecord) error {
	if err := validateScriptMetadataRecord(record); err != nil {
		return err
	}
	if err := core.ValidateScriptBody(record.Desired.Body); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	return nil
}

func validateScriptMetadataRecord(record ScriptRecord) error {
	if err := validateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if err := validateID(ids.KindService, record.ServiceID); err != nil {
		return err
	}
	if err := validateID(ids.KindScript, record.Desired.ID); err != nil {
		return err
	}
	if err := record.Desired.ValidateMetadata(); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	if record.Origin != "api" && record.Origin != "blueprint" ||
		record.Origin == "api" && record.ReconciliationKey != "" ||
		record.Origin == "blueprint" && record.ReconciliationKey == "" || record.ActiveGeneration == 0 {
		return errs.New(errs.KindValidationFailed, "Script origin or generation is invalid")
	}
	return nil
}

func encodeScriptRecord(record ScriptRecord) ([]byte, error) {
	if err := validateScriptRecord(record); err != nil {
		return nil, err
	}
	stored := storedScriptRecord{
		EnvironmentID: record.EnvironmentID, ServiceID: record.ServiceID, Origin: record.Origin,
		ReconciliationKey: record.ReconciliationKey, ActiveGeneration: record.ActiveGeneration,
		ActiveReferences: record.ActiveReferences,
		Desired: storedScriptDesired{
			ID: record.Desired.ID, Slug: record.Desired.Slug,
			ServiceName: record.Desired.ServiceName, When: record.Desired.When,
		},
	}
	return encodeEnvelope("script", stored)
}

func decodeScriptRecord(value []byte) (ScriptRecord, error) {
	stored, err := decodeEnvelope[storedScriptRecord](value, "script")
	if err != nil {
		return ScriptRecord{}, err
	}
	record := ScriptRecord{
		EnvironmentID: stored.EnvironmentID, ServiceID: stored.ServiceID, Origin: stored.Origin,
		ReconciliationKey: stored.ReconciliationKey, ActiveGeneration: stored.ActiveGeneration,
		ActiveReferences: stored.ActiveReferences,
		Desired: core.Script{
			ID: stored.Desired.ID, Slug: stored.Desired.Slug,
			ServiceName: stored.Desired.ServiceName, When: stored.Desired.When,
		},
	}
	if err := validateScriptMetadataRecord(record); err != nil {
		return ScriptRecord{}, corruptRecord()
	}
	return record, nil
}

func newScriptBodyGeneration(record ScriptRecord) (ScriptBodyGenerationRecord, error) {
	digest := sha256.Sum256([]byte(record.Desired.Body))
	generation := ScriptBodyGenerationRecord{
		ScriptID: record.Desired.ID, Generation: record.ActiveGeneration,
		BodySize: uint32(len(record.Desired.Body)), Body: record.Desired.Body,
		BodySHA256: hex.EncodeToString(digest[:]),
	}
	if err := validateScriptBodyGeneration(generation); err != nil {
		return ScriptBodyGenerationRecord{}, err
	}
	return generation, nil
}

func validateScriptBodyGeneration(generation ScriptBodyGenerationRecord) error {
	if err := validateID(ids.KindScript, generation.ScriptID); err != nil {
		return err
	}
	if generation.Generation == 0 || generation.BodySize != uint32(len(generation.Body)) {
		return errs.New(errs.KindValidationFailed, "Script body generation or size is invalid")
	}
	if err := core.ValidateScriptBody(generation.Body); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	digest := sha256.Sum256([]byte(generation.Body))
	if generation.BodySHA256 != hex.EncodeToString(digest[:]) {
		return errs.New(errs.KindValidationFailed, "Script body digest is invalid")
	}
	return nil
}

func encodeScriptBodyGeneration(generation ScriptBodyGenerationRecord) ([]byte, error) {
	if err := validateScriptBodyGeneration(generation); err != nil {
		return nil, err
	}
	return encodeEnvelope("script_body_generation", generation)
}

func decodeScriptBodyGeneration(value []byte) (ScriptBodyGenerationRecord, error) {
	generation, err := decodeEnvelope[ScriptBodyGenerationRecord](value, "script_body_generation")
	if err != nil {
		return ScriptBodyGenerationRecord{}, err
	}
	if err := validateScriptBodyGeneration(generation); err != nil {
		return ScriptBodyGenerationRecord{}, corruptRecord()
	}
	return generation, nil
}
