package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	scriptSetRecordPrefix          = "/v1/records/script-sets/"
	scriptLocatorPrefix            = "/v1/indexes/scripts/by-id/"
	scriptEnvironmentLocatorPrefix = "/v1/indexes/scripts/by-environment/"
)

type ScriptSetGenerationRecord struct {
	EnvironmentID string `json:"environment_id"`
	GenerationID  string `json:"generation_id"`
}

type scriptLocatorRecord struct {
	ScriptID      string `json:"script_id"`
	EnvironmentID string `json:"environment_id"`
}

type storedScriptDesired struct {
	ID          string                `json:"id"`
	Slug        string                `json:"slug"`
	ServiceName string                `json:"service"`
	When        core.ScriptHook       `json:"when"`
	Order       uint16                `json:"order,omitempty"`
	Execution   *core.ScriptExecution `json:"execution,omitempty"`
}

type storedScriptRecord struct {
	EnvironmentID       string              `json:"environment_id"`
	ServiceID           string              `json:"service_id"`
	Origin              string              `json:"origin"`
	ReconciliationKey   string              `json:"reconciliation_key,omitempty"`
	ActiveGeneration    uint64              `json:"active_generation"`
	ActiveReferences    uint64              `json:"active_references"`
	ScriptSetGeneration string              `json:"script_set_generation"`
	Desired             storedScriptDesired `json:"desired"`
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
	EnvironmentID       string      `json:"environment_id"`
	ServiceID           string      `json:"service_id"`
	Origin              string      `json:"origin"`
	ReconciliationKey   string      `json:"reconciliation_key,omitempty"`
	ActiveGeneration    uint64      `json:"active_generation"`
	ActiveReferences    uint64      `json:"active_references"`
	ScriptSetGeneration string      `json:"script_set_generation"`
	Desired             core.Script `json:"desired"`
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

func scriptLocatorKey(id string) string { return scriptLocatorPrefix + id }

func scriptEnvironmentLocatorPrefixFor(environmentID string) string {
	return scriptEnvironmentLocatorPrefix + environmentID + "/"
}

func scriptEnvironmentLocatorKey(environmentID, id string) string {
	return scriptEnvironmentLocatorPrefixFor(environmentID) + id
}

func scriptSetActiveKey(environmentID string) string {
	return scriptSetRecordPrefix + environmentID + "/active"
}

func scriptSetEnvironmentPrefix(environmentID string) string {
	return scriptSetRecordPrefix + environmentID + "/"
}

func scriptSetGenerationPrefix(environmentID, generationID string) string {
	return scriptSetEnvironmentPrefix(environmentID) + "generations/" + generationID + "/"
}

func scriptSetScriptKey(environmentID, generationID, id string) string {
	return scriptSetGenerationPrefix(environmentID, generationID) + "scripts/" + id
}

func scriptSetBodyGenerationPrefix(environmentID, generationID, id string) string {
	return scriptSetGenerationPrefix(environmentID, generationID) + "bodies/" + id + "/"
}

func scriptSetBodyGenerationKey(environmentID, generationID, id string, generation uint64) string {
	return scriptSetBodyGenerationPrefix(environmentID, generationID, id) + strconv.FormatUint(generation, 10)
}

func scriptSetOwnerPrefix(environmentID, generationID string) string {
	return scriptSetGenerationPrefix(environmentID, generationID) + "owners/"
}

func scriptSetOwnerKey(environmentID, generationID, scriptID string) string {
	return scriptSetOwnerPrefix(environmentID, generationID) + scriptID
}

func scriptSetSlugKey(environmentID, generationID, slug string) string {
	return scriptSetGenerationPrefix(environmentID, generationID) + "slugs/" + encodeDynamicSegment(slug)
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
		ActiveReferences: record.ActiveReferences, ScriptSetGeneration: record.ScriptSetGeneration,
		Desired: storedScriptDesired{
			ID: record.Desired.ID, Slug: record.Desired.Slug,
			ServiceName: record.Desired.ServiceName, When: record.Desired.When, Order: record.Desired.Order,
			Execution: record.Desired.Execution,
		},
	}
	return recordcodec.Encode("script", stored)
}

func decodeScriptRecord(value []byte) (ScriptRecord, error) {
	stored, err := recordcodec.Decode[storedScriptRecord](value, "script")
	if err != nil {
		return ScriptRecord{}, err
	}
	record := ScriptRecord{
		EnvironmentID: stored.EnvironmentID, ServiceID: stored.ServiceID, Origin: stored.Origin,
		ReconciliationKey: stored.ReconciliationKey, ActiveGeneration: stored.ActiveGeneration,
		ActiveReferences: stored.ActiveReferences, ScriptSetGeneration: stored.ScriptSetGeneration,
		Desired: core.Script{
			ID: stored.Desired.ID, Slug: stored.Desired.Slug,
			ServiceName: stored.Desired.ServiceName, When: stored.Desired.When, Order: stored.Desired.Order,
			Execution: stored.Desired.Execution,
		},
	}
	if err := validateScriptMetadataRecord(record); err != nil {
		return ScriptRecord{}, corruptRecord()
	}
	return record, nil
}

func validateScriptSetGeneration(record ScriptSetGenerationRecord) error {
	if validateID(ids.KindEnvironment, record.EnvironmentID) != nil || record.GenerationID == "" ||
		len(record.GenerationID) > 128 || strings.Contains(record.GenerationID, "/") {
		return errs.New(errs.KindValidationFailed, "Script-set generation is invalid")
	}
	return nil
}

func encodeScriptSetGeneration(record ScriptSetGenerationRecord) ([]byte, error) {
	if err := validateScriptSetGeneration(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("script_set_generation", record)
}

func decodeScriptSetGeneration(value []byte) (ScriptSetGenerationRecord, error) {
	record, err := recordcodec.Decode[ScriptSetGenerationRecord](value, "script_set_generation")
	if err != nil || validateScriptSetGeneration(record) != nil {
		return ScriptSetGenerationRecord{}, corruptRecord()
	}
	return record, nil
}

func encodeScriptLocator(record scriptLocatorRecord) ([]byte, error) {
	if validateID(ids.KindScript, record.ScriptID) != nil ||
		validateID(ids.KindEnvironment, record.EnvironmentID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Script locator is invalid")
	}
	return recordcodec.Encode("script_locator", record)
}

func decodeScriptLocator(value []byte) (scriptLocatorRecord, error) {
	record, err := recordcodec.Decode[scriptLocatorRecord](value, "script_locator")
	if err != nil || validateID(ids.KindScript, record.ScriptID) != nil ||
		validateID(ids.KindEnvironment, record.EnvironmentID) != nil {
		return scriptLocatorRecord{}, corruptRecord()
	}
	return record, nil
}

func readActiveScriptSet(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	revision int64,
) (Versioned[ScriptSetGenerationRecord], error) {
	read, err := store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{scriptSetActiveKey(environmentID)}, Revision: revision},
	)
	if err != nil {
		return Versioned[ScriptSetGenerationRecord]{}, err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
		return Versioned[ScriptSetGenerationRecord]{}, errs.New(
			errs.KindInternal,
			"Environment active Script-set generation is missing",
		)
	}
	record, err := decodeScriptSetGeneration(read.Values[0].Value)
	if err != nil || record.EnvironmentID != environmentID {
		return Versioned[ScriptSetGenerationRecord]{}, corruptRecord()
	}
	return Versioned[ScriptSetGenerationRecord]{
		Record: record, Revision: read.Values[0].ModRevision, ReadRevision: read.ReadRevision,
	}, nil
}

type activeScriptStorage struct {
	Script             Versioned[ScriptRecord]
	Active             Versioned[ScriptSetGenerationRecord]
	Locator            Versioned[scriptLocatorRecord]
	EnvironmentLocator Versioned[string]
}

func readActiveScriptStorage(
	ctx context.Context,
	store hierarchyStore,
	scriptID string,
	revision int64,
) (activeScriptStorage, error) {
	locatorRead, err := store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{scriptLocatorKey(scriptID)}, Revision: revision},
	)
	if err != nil {
		return activeScriptStorage{}, err
	}
	if locatorRead == nil || len(locatorRead.Values) != 1 || locatorRead.Values[0] == nil {
		return activeScriptStorage{}, errs.New(errs.KindScriptNotFound, "Script was not found")
	}
	locator, err := decodeScriptLocator(locatorRead.Values[0].Value)
	if err != nil || locator.ScriptID != scriptID {
		return activeScriptStorage{}, corruptRecord()
	}
	environmentLocatorRead, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			scriptEnvironmentLocatorKey(locator.EnvironmentID, scriptID),
		}, Revision: locatorRead.ReadRevision,
	})
	if err != nil {
		return activeScriptStorage{}, err
	}
	if environmentLocatorRead == nil || len(environmentLocatorRead.Values) != 1 ||
		environmentLocatorRead.Values[0] == nil || string(environmentLocatorRead.Values[0].Value) != scriptID {
		return activeScriptStorage{}, errs.New(errs.KindInternal, "Script Environment locator is missing or corrupt")
	}
	active, err := readActiveScriptSet(ctx, store, locator.EnvironmentID, locatorRead.ReadRevision)
	if err != nil {
		return activeScriptStorage{}, err
	}
	primary, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{scriptSetScriptKey(locator.EnvironmentID, active.Record.GenerationID, scriptID)},
		Revision: active.ReadRevision,
	})
	if err != nil {
		return activeScriptStorage{}, err
	}
	if primary == nil || len(primary.Values) != 1 || primary.Values[0] == nil {
		return activeScriptStorage{}, errs.New(errs.KindScriptNotFound, "Script was not found")
	}
	record, err := decodeScriptRecord(primary.Values[0].Value)
	if err != nil || record.Desired.ID != scriptID || record.EnvironmentID != locator.EnvironmentID ||
		record.ScriptSetGeneration != active.Record.GenerationID {
		return activeScriptStorage{}, corruptRecord()
	}
	return activeScriptStorage{
		Script: Versioned[ScriptRecord]{
			Record:       record,
			Revision:     primary.Values[0].ModRevision,
			ReadRevision: active.ReadRevision,
		},
		Active: active,
		Locator: Versioned[scriptLocatorRecord]{
			Record:       locator,
			Revision:     locatorRead.Values[0].ModRevision,
			ReadRevision: active.ReadRevision,
		},
		EnvironmentLocator: Versioned[string]{
			Record:       scriptID,
			Revision:     environmentLocatorRead.Values[0].ModRevision,
			ReadRevision: active.ReadRevision,
		},
	}, nil
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
	return recordcodec.Encode("script_body_generation", generation)
}

func decodeScriptBodyGeneration(value []byte) (ScriptBodyGenerationRecord, error) {
	generation, err := recordcodec.Decode[ScriptBodyGenerationRecord](value, "script_body_generation")
	if err != nil {
		return ScriptBodyGenerationRecord{}, err
	}
	if err := validateScriptBodyGeneration(generation); err != nil {
		return ScriptBodyGenerationRecord{}, corruptRecord()
	}
	return generation, nil
}
