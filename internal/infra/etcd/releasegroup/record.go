package releasegroup

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"io"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/releasegroup"
	infraetcd "github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	recordPrefix               = "/v1/records/release-groups/"
	ownerPrefix                = "/v1/indexes/release-groups/by-owner/environment/"
	namePrefix                 = "/v1/indexes/release-groups/by-name/environment/"
	environmentRecordPrefix    = "/v1/records/environments/"
	projectRecordPrefix        = "/v1/records/projects/"
	tenantRecordPrefix         = "/v1/records/tenants/"
	serviceRecordPrefix        = "/v1/records/services/"
	serviceOwnerPrefix         = "/v1/indexes/services/by-owner/environment/"
	environmentEpochPrefix     = "/v1/runtime/environment-mutation-epochs/"
	environmentOperationPrefix = "/v1/runtime/environment-operation-locks/"
	deletionRootPrefix         = "/v1/runtime/deletions/"
	environmentComposePrefix   = "/v1/records/environment-compose-projections/"
)

type envelope struct {
	Schema int    `json:"schema"`
	Kind   string `json:"kind"`
	Data   record `json:"data"`
}

type record struct {
	ID            string           `json:"id"`
	EnvironmentID string           `json:"environment_id"`
	Name          string           `json:"name"`
	ServiceIDs    []string         `json:"service_ids"`
	Order         []string         `json:"order"`
	DefaultTag    string           `json:"default_tag,omitempty"`
	OnFailure     domain.OnFailure `json:"on_failure"`
}

type cursor struct {
	Version       int    `json:"v"`
	Revision      int64  `json:"revision"`
	EnvironmentID string `json:"environment_id"`
	LastID        string `json:"last_id"`
	Limit         int    `json:"limit"`
}

type epochRecord struct {
	EnvironmentID string `json:"environment_id"`
}

type durableEnvelope[T any] struct {
	Schema int    `json:"schema"`
	Kind   string `json:"kind"`
	Data   T      `json:"data"`
}

func encodeGroup(group domain.Group) ([]byte, error) {
	value, err := json.Marshal(envelope{Schema: 1, Kind: "release_group", Data: record{
		ID: group.ID, EnvironmentID: group.EnvironmentID, Name: group.Name,
		ServiceIDs: group.ServiceIDs, Order: group.Order, DefaultTag: group.DefaultTag,
		OnFailure: group.OnFailure,
	}})
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return value, nil
}

func decodeGroup(value []byte) (domain.Group, error) {
	if err := rejectDuplicateJSONFields(value); err != nil {
		return domain.Group{}, corruptRecord()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var stored envelope
	if err := decoder.Decode(&stored); err != nil {
		return domain.Group{}, corruptRecord()
	}
	if err := requireEOF(decoder); err != nil || stored.Schema != 1 || stored.Kind != "release_group" {
		return domain.Group{}, corruptRecord()
	}
	group, err := domain.New(domain.Input{
		ID: stored.Data.ID, EnvironmentID: stored.Data.EnvironmentID, Name: stored.Data.Name,
		ServiceIDs: stored.Data.ServiceIDs, Order: stored.Data.Order,
		DefaultTag: stored.Data.DefaultTag, OnFailure: stored.Data.OnFailure,
	})
	if err != nil {
		return domain.Group{}, corruptRecord()
	}
	return group, nil
}

func encodeCursor(value cursor) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeCursor(value string) (cursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return cursor{}, invalidCursor()
	}
	if err := rejectDuplicateJSONFields(decoded); err != nil {
		return cursor{}, invalidCursor()
	}
	decoder := json.NewDecoder(bytes.NewReader(decoded))
	decoder.DisallowUnknownFields()
	var result cursor
	if err := decoder.Decode(&result); err != nil {
		return cursor{}, invalidCursor()
	}
	if err := requireEOF(decoder); err != nil || result.Version != 1 || result.Revision <= 0 ||
		ids.Validate(ids.KindEnvironment, result.EnvironmentID) != nil ||
		ids.Validate(ids.KindReleaseGroup, result.LastID) != nil ||
		result.Limit < 1 || result.Limit > maximumPageLimit {
		return cursor{}, invalidCursor()
	}
	return result, nil
}

func requireEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errs.New(errs.KindInternal, "durable release group json has trailing data")
	}
	return nil
}

func decodeDurable[T any](value []byte, kind string) (T, error) {
	var zero T
	if err := rejectDuplicateJSONFields(value); err != nil {
		return zero, corruptRecord()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var stored durableEnvelope[T]
	if err := decoder.Decode(&stored); err != nil || requireEOF(decoder) != nil ||
		stored.Schema != 1 || stored.Kind != kind {
		return zero, corruptRecord()
	}
	return stored.Data, nil
}

func decodeEnvironment(value []byte) (hierarchyrecord.EnvironmentRecord, error) {
	record, err := decodeDurable[hierarchyrecord.EnvironmentRecord](value, "environment")
	if err != nil || ids.Validate(ids.KindEnvironment, record.ID) != nil ||
		ids.Validate(ids.KindProject, record.ProjectID) != nil || record.Name == "" ||
		record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return hierarchyrecord.EnvironmentRecord{}, corruptRecord()
	}
	return record, nil
}

func decodeProject(value []byte) (hierarchyrecord.ProjectRecord, error) {
	record, err := decodeDurable[hierarchyrecord.ProjectRecord](value, "project")
	if err != nil || ids.Validate(ids.KindProject, record.ID) != nil || record.Slug == "" || record.Name == "" {
		return hierarchyrecord.ProjectRecord{}, corruptRecord()
	}
	if record.Kind == hierarchyrecord.ProjectKindTenant {
		if ids.Validate(ids.KindTenant, record.TenantID) != nil {
			return hierarchyrecord.ProjectRecord{}, corruptRecord()
		}
	} else if record.Kind != hierarchyrecord.ProjectKindBacking || record.TenantID != "" {
		return hierarchyrecord.ProjectRecord{}, corruptRecord()
	}
	return record, nil
}

func decodeTenant(value []byte) (hierarchyrecord.TenantRecord, error) {
	record, err := decodeDurable[hierarchyrecord.TenantRecord](value, "tenant")
	if err != nil || ids.Validate(ids.KindTenant, record.ID) != nil || record.Slug == "" || record.Name == "" {
		return hierarchyrecord.TenantRecord{}, corruptRecord()
	}
	return record, nil
}

func decodeService(value []byte) (servicerecord.ServiceRecord, error) {
	record, err := decodeDurable[servicerecord.ServiceRecord](value, "service")
	if err != nil || ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil ||
		ids.Validate(ids.KindService, record.Desired.ID) != nil ||
		record.Runtime.ServiceID != record.Desired.ID || record.Runtime.RuntimeIntent == "absent" {
		return servicerecord.ServiceRecord{}, corruptRecord()
	}
	return record, nil
}

func decodeComposeProjection(value []byte) (infraetcd.EnvironmentComposeProjection, error) {
	projection, err := decodeDurable[infraetcd.EnvironmentComposeProjection](value, "environment-compose-projection")
	if err != nil || ids.Validate(ids.KindEnvironment, projection.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, projection.RevisionID) != nil || projection.RenderGeneration == 0 {
		return infraetcd.EnvironmentComposeProjection{}, corruptRecord()
	}
	return projection, nil
}

func decodeEpoch(value []byte, environmentID string) ([]byte, error) {
	record, err := decodeDurable[epochRecord](value, "environment-mutation-epoch")
	if err != nil || record.EnvironmentID != environmentID {
		return nil, corruptRecord()
	}
	encoded, err := json.Marshal(durableEnvelope[epochRecord]{
		Schema: 1, Kind: "environment-mutation-epoch", Data: record,
	})
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return encoded, nil
}

func rejectDuplicateJSONFields(value []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(value))
	if err := walkJSONValue(decoder); err != nil {
		return err
	}
	return requireEOF(decoder)
}

func walkJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			member, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := member.(string)
			if !ok {
				return fmt.Errorf("json object member is not a string")
			}
			if _, duplicate := seen[name]; duplicate {
				return fmt.Errorf("duplicate JSON member %q", name)
			}
			seen[name] = struct{}{}
			if err := walkJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return io.ErrUnexpectedEOF
		}
	case '[':
		for decoder.More() {
			if err := walkJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return io.ErrUnexpectedEOF
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter")
	}
	return nil
}

func recordKey(id string) string                   { return recordPrefix + id }
func environmentKey(id string) string              { return environmentRecordPrefix + id }
func projectKey(id string) string                  { return projectRecordPrefix + id }
func tenantKey(id string) string                   { return tenantRecordPrefix + id }
func serviceKey(id string) string                  { return serviceRecordPrefix + id }
func environmentMutationEpochKey(id string) string { return environmentEpochPrefix + id }
func environmentOperationLockKey(id string) string { return environmentOperationPrefix + id }
func deletionKey(kind string, id string) string    { return deletionRootPrefix + kind + "/" + id }
func environmentDeletionKey(id string) string      { return deletionKey("environment", id) }
func environmentComposeKey(id string) string       { return environmentComposePrefix + id }

func environmentOwnerKey(projectID string, environmentID string) string {
	return "/v1/indexes/environments/by-owner/project/" + projectID + "/" + environmentID
}

func projectOwnerKey(project hierarchyrecord.ProjectRecord) string {
	if project.Kind == hierarchyrecord.ProjectKindBacking {
		return "/v1/indexes/projects/by-owner/platform/-/" + project.ID
	}
	return "/v1/indexes/projects/by-owner/tenant/" + project.TenantID + "/" + project.ID
}

func ownerScopePrefix(environmentID string) string    { return ownerPrefix + environmentID + "/" }
func ownerKey(environmentID string, id string) string { return ownerScopePrefix(environmentID) + id }
func serviceOwnerKey(environmentID string, id string) string {
	return serviceOwnerPrefix + environmentID + "/" + id
}
func nameKey(environmentID string, name string) string {
	return namePrefix + environmentID + "/~" + base64.RawURLEncoding.EncodeToString([]byte(name))
}

func corruptRecord() error {
	return errs.New(errs.KindInternal, "durable release group record is corrupt")
}

func invalidCursor() error {
	return errs.New(errs.KindMalformedRequest, "release group cursor is invalid")
}
