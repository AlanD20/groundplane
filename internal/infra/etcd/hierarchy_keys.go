package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	tenantPrefix                   = "/v1/records/tenants/"
	projectPrefix                  = "/v1/records/projects/"
	environmentPrefix              = "/v1/records/environments/"
	environmentMutationEpochPrefix = "/v1/runtime/environment-mutation-epochs/"
	environmentOperationLockPrefix = "/v1/runtime/environment-operation-locks/"
	projectPlatformOwnerPrefix     = "/v1/indexes/projects/by-owner/platform/-/"
	maximumEncodedCursorBytes      = 2048
	cursorVersion                  = 1
	cursorOrder                    = "id_asc"
)

type cursorPayload struct {
	Version  int    `json:"v"`
	Revision int64  `json:"revision"`
	LastID   string `json:"last_id"`
	Query    string `json:"query"`
}

type cursorQuery struct {
	Collection string `json:"collection"`
	OwnerKind  string `json:"owner_kind"`
	OwnerID    string `json:"owner_id"`
	Order      string `json:"order"`
	Limit      int    `json:"limit"`
}

func tenantKey(id string) string      { return tenantPrefix + id }
func projectKey(id string) string     { return projectPrefix + id }
func environmentKey(id string) string { return environmentPrefix + id }

func environmentMutationEpochKey(environmentID string) string {
	return environmentMutationEpochPrefix + environmentID
}

func environmentOperationLockKey(environmentID string) string {
	return environmentOperationLockPrefix + environmentID
}

func tenantSlugKey(slug string) string {
	return "/v1/indexes/tenants/by-slug/global/-/" + encodeDynamicSegment(slug)
}

func projectTenantSlugKey(tenantID string, slug string) string {
	return "/v1/indexes/projects/by-slug/tenant/" + tenantID + "/" + encodeDynamicSegment(slug)
}

func projectPlatformSlugKey(slug string) string {
	return "/v1/indexes/projects/by-slug/platform/-/" + encodeDynamicSegment(slug)
}

func projectSlugKey(record ProjectRecord) string {
	if record.Kind == ProjectKindBacking {
		return projectPlatformSlugKey(record.Slug)
	}
	return projectTenantSlugKey(record.TenantID, record.Slug)
}

func environmentNameKey(projectID string, name string) string {
	return "/v1/indexes/environments/by-name/project/" + projectID + "/" + encodeDynamicSegment(name)
}

func projectTenantOwnerPrefix(tenantID string) string {
	return "/v1/indexes/projects/by-owner/tenant/" + tenantID + "/"
}

func projectOwnerKey(record ProjectRecord) string {
	if record.Kind == ProjectKindBacking {
		return projectPlatformOwnerPrefix + record.ID
	}
	return projectTenantOwnerPrefix(record.TenantID) + record.ID
}

func environmentOwnerPrefix(projectID string) string {
	return "/v1/indexes/environments/by-owner/project/" + projectID + "/"
}

func environmentOwnerKey(projectID string, environmentID string) string {
	return environmentOwnerPrefix(projectID) + environmentID
}

func encodeDynamicSegment(value string) string {
	return "~" + base64.RawURLEncoding.EncodeToString([]byte(value))
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "hierarchy context is required")
	}
	return ctx.Err()
}

func validateID(kind ids.Kind, id string) error {
	if err := ids.Validate(kind, id); err != nil {
		return errs.New(errs.KindValidationFailed, err.Error())
	}
	return nil
}

func validateLabel(field string, value string) error {
	if value == "" || !utf8.ValidString(value) {
		return errs.Newf(errs.KindValidationFailed, "%s is required and must be valid UTF-8", field)
	}
	return nil
}

func validateTenant(record TenantRecord) error {
	if err := validateID(ids.KindTenant, record.ID); err != nil {
		return err
	}
	if err := validateLabel("tenant slug", record.Slug); err != nil {
		return err
	}
	if err := validateLabel("tenant name", record.Name); err != nil {
		return err
	}
	if record.Description != "" && !utf8.ValidString(record.Description) {
		return errs.New(errs.KindValidationFailed, "tenant description must be valid UTF-8")
	}
	if record.DeletionTaskID != "" && ids.Validate(ids.KindTask, record.DeletionTaskID) != nil {
		return errs.New(errs.KindValidationFailed, "tenant deletion_task_id is invalid")
	}
	return nil
}

func validateProject(record ProjectRecord) error {
	if err := validateID(ids.KindProject, record.ID); err != nil {
		return err
	}
	if err := validateLabel("project slug", record.Slug); err != nil {
		return err
	}
	if err := validateLabel("project name", record.Name); err != nil {
		return err
	}
	if record.Description != "" && !utf8.ValidString(record.Description) {
		return errs.New(errs.KindValidationFailed, "project description must be valid UTF-8")
	}
	if record.DeletionTaskID != "" && ids.Validate(ids.KindTask, record.DeletionTaskID) != nil {
		return errs.New(errs.KindValidationFailed, "project deletion_task_id is invalid")
	}
	switch record.Kind {
	case ProjectKindTenant:
		return validateID(ids.KindTenant, record.TenantID)
	case ProjectKindBacking:
		if record.TenantID != "" {
			return errs.New(errs.KindValidationFailed, "backing projects must not have a tenant id")
		}
		return nil
	default:
		return errs.New(errs.KindValidationFailed, "project kind must be tenant or backing")
	}
}

func validateEnvironment(record EnvironmentRecord) error {
	if err := validateID(ids.KindEnvironment, record.ID); err != nil {
		return err
	}
	if err := validateID(ids.KindProject, record.ProjectID); err != nil {
		return err
	}
	if err := validateLabel("environment name", record.Name); err != nil {
		return err
	}
	pool, err := ipam.ParseIPv4Prefix(record.NetworkPool)
	if err != nil || pool.String() != record.NetworkPool {
		return errs.New(errs.KindValidationFailed, "environment network_pool must be a canonical IPv4 CIDR")
	}
	if err := validateLabel("environment volume directory", record.VolumeDir); err != nil {
		return err
	}
	if err := validateEnvironmentRecordPath(record); err != nil {
		return err
	}
	if err := validateEnvironmentProvisioning(record); err != nil {
		return err
	}
	if record.DeletionTaskID != "" && ids.Validate(ids.KindTask, record.DeletionTaskID) != nil {
		return errs.New(errs.KindValidationFailed, "environment deletion_task_id is invalid")
	}
	_, offset := record.CreatedAt.Zone()
	if record.CreatedAt.IsZero() || offset != 0 {
		return errs.New(errs.KindValidationFailed, "environment created_at must be a non-zero UTC timestamp")
	}
	return nil
}

func encodeTenant(record TenantRecord) ([]byte, error) { return recordcodec.Encode("tenant", record) }
func encodeProject(record ProjectRecord) ([]byte, error) {
	return recordcodec.Encode("project", record)
}
func decodeTenant(value []byte) (TenantRecord, error) {
	record, err := recordcodec.Decode[TenantRecord](value, "tenant")
	if err != nil {
		return TenantRecord{}, err
	}
	if err := validateTenant(record); err != nil {
		return TenantRecord{}, corruptRecord()
	}
	return record, nil
}

func decodeProject(value []byte) (ProjectRecord, error) {
	record, err := recordcodec.Decode[ProjectRecord](value, "project")
	if err != nil {
		return ProjectRecord{}, err
	}
	if err := validateProject(record); err != nil {
		return ProjectRecord{}, corruptRecord()
	}
	return record, nil
}

func corruptRecord() error {
	return errs.New(errs.KindInternal, "durable record violates its repository schema")
}

func getRecord[T any](
	ctx context.Context,
	store hierarchyStore,
	key string,
	id string,
	notFound errs.Kind,
	decode func([]byte) (T, error),
	identity func(T) string,
) (Versioned[T], error) {
	result, err := store.Get(ctx, key)
	if err != nil {
		return Versioned[T]{}, err
	}
	if result.Entry == nil {
		return Versioned[T]{}, errs.Newf(notFound, "%s was not found", id)
	}
	record, err := decode(result.Entry.Value)
	if err != nil {
		return Versioned[T]{}, err
	}
	if identity(record) != id {
		return Versioned[T]{}, errs.New(errs.KindInternal, "primary key does not match its record id")
	}
	return Versioned[T]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func resolveRecord[T any](
	ctx context.Context,
	store hierarchyStore,
	indexKey string,
	primaryKey func(string) string,
	idKind ids.Kind,
	notFound errs.Kind,
	decode func([]byte) (T, error),
	identity func(T) string,
	matches func(T) bool,
) (Versioned[T], error) {
	index, err := store.Get(ctx, indexKey)
	if err != nil {
		return Versioned[T]{}, err
	}
	if index.Entry == nil {
		return Versioned[T]{}, errs.New(notFound, "resource was not found")
	}
	id := string(index.Entry.Value)
	if ids.Validate(idKind, id) != nil {
		return Versioned[T]{}, errs.New(errs.KindInternal, "slug index contains an invalid stable id")
	}
	result, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{primaryKey(id)}, Revision: index.ReadRevision})
	if err != nil {
		return Versioned[T]{}, err
	}
	if len(result.Values) != 1 || result.Values[0] == nil {
		return Versioned[T]{}, errs.New(errs.KindInternal, "slug index references a missing primary record")
	}
	record, err := decode(result.Values[0].Value)
	if err != nil {
		return Versioned[T]{}, err
	}
	if identity(record) != id {
		return Versioned[T]{}, errs.New(errs.KindInternal, "slug index id does not match its primary record")
	}
	if !matches(record) {
		return Versioned[T]{}, errs.New(errs.KindInternal, "slug index ownership does not match its primary record")
	}
	return Versioned[T]{
		Record: record, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func renameRecord[T any](
	ctx context.Context,
	store hierarchyStore,
	current Versioned[T],
	replacement T,
	primaryKey string,
	oldSlugKey string,
	newSlugKey string,
	membershipKeys []string,
	tombstoneKey string,
	kind string,
	id string,
	notFound errs.Kind,
	encode func(T) ([]byte, error),
) (Versioned[T], error) {
	secondaryKeys := append([]string{oldSlugKey}, membershipKeys...)
	tombstoneOffset := len(secondaryKeys)
	secondaryKeys = append(secondaryKeys, tombstoneKey)
	newSlugOffset := -1
	if newSlugKey != oldSlugKey {
		newSlugOffset = len(secondaryKeys)
		secondaryKeys = append(secondaryKeys, newSlugKey)
	}
	secondary, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: secondaryKeys, Revision: current.ReadRevision})
	if err != nil {
		return Versioned[T]{}, err
	}
	if len(secondary.Values) != len(secondaryKeys) || secondary.Values[0] == nil ||
		string(secondary.Values[0].Value) != id {
		return Versioned[T]{}, errs.New(errs.KindInternal, "slug index is missing or mismatched")
	}
	for index := range membershipKeys {
		value := secondary.Values[index+1]
		if value == nil || string(value.Value) != id {
			return Versioned[T]{}, errs.New(errs.KindInternal, "owner index is missing or mismatched")
		}
	}
	if secondary.Values[tombstoneOffset] != nil {
		return Versioned[T]{}, errs.New(errs.KindResourceInUse, "resource deletion is in progress")
	}
	if newSlugOffset >= 0 && secondary.Values[newSlugOffset] != nil {
		return Versioned[T]{}, errs.New(errs.KindSlugConflict, "slug is already in use")
	}
	if newSlugOffset < 0 {
		if err := validateContext(ctx); err != nil {
			return Versioned[T]{}, err
		}
		return current, nil
	}
	value, err := encode(replacement)
	if err != nil {
		return Versioned[T]{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: primaryKey, ModRevision: current.Revision},
		{Key: oldSlugKey, ModRevision: secondary.Values[0].ModRevision},
	}
	for index, key := range membershipKeys {
		conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: secondary.Values[index+1].ModRevision})
	}
	conditions = append(conditions, etcdstore.Condition{Key: tombstoneKey})
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: primaryKey, Value: value}}
	conditions = append(conditions, etcdstore.Condition{Key: newSlugKey})
	mutations = append(mutations,
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: oldSlugKey},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: newSlugKey, Value: []byte(id)},
	)
	if err := validateContext(ctx); err != nil {
		return Versioned[T]{}, err
	}
	result, err := store.Transact(ctx, conditions, mutations)
	if err != nil {
		return Versioned[T]{}, err
	}
	if !result.Succeeded {
		return Versioned[T]{}, diagnoseRename(
			ctx, store, primaryKey, current.Revision, oldSlugKey, newSlugKey,
			membershipKeys, tombstoneKey, kind, id, notFound,
		)
	}
	return Versioned[T]{Record: replacement, Revision: result.Revision, ReadRevision: result.Revision}, nil
}

func diagnoseRename(
	ctx context.Context,
	store hierarchyStore,
	primaryKey string,
	expectedRevision int64,
	oldSlugKey string,
	newSlugKey string,
	membershipKeys []string,
	tombstoneKey string,
	kind string,
	id string,
	notFound errs.Kind,
) error {
	keys := []string{primaryKey, oldSlugKey}
	membershipOffset := len(keys)
	keys = append(keys, membershipKeys...)
	tombstoneOffset := len(keys)
	keys = append(keys, tombstoneKey)
	newSlugOffset := len(keys)
	keys = append(keys, newSlugKey)
	current, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return err
	}
	if len(current.Values) != len(keys) {
		return errs.New(errs.KindInternal, "rename diagnosis returned an invalid key count")
	}
	if current.Values[0] == nil {
		return errs.Newf(notFound, "%s was not found", id)
	}
	if current.Values[tombstoneOffset] != nil {
		return errs.New(errs.KindResourceInUse, "resource deletion is in progress")
	}
	if current.Values[newSlugOffset] != nil && string(current.Values[newSlugOffset].Value) != id {
		return errs.New(errs.KindSlugConflict, "slug is already in use")
	}
	if current.Values[0].ModRevision != expectedRevision {
		return stateConflict(kind, id)
	}
	if current.Values[1] == nil || string(current.Values[1].Value) != id {
		return errs.New(errs.KindInternal, "slug index is missing or mismatched")
	}
	for index := range membershipKeys {
		value := current.Values[membershipOffset+index]
		if value == nil || string(value.Value) != id {
			return errs.New(errs.KindInternal, "owner index is missing or mismatched")
		}
	}
	return stateConflict(kind, id)
}

func stateConflict(kind string, id string) error {
	return errs.Newf(errs.KindStateConflict, "%s %s changed", kind, id)
}

func normalizePageRequest(
	request PageRequest,
	collection string,
	ownerKind string,
	ownerID string,
	prefix string,
	idKind ids.Kind,
) (int, int64, string, string, error) {
	limit := request.Limit
	if limit == 0 {
		limit = DefaultPageLimit
	}
	if limit < 1 || limit > MaximumPageLimit {
		return 0, 0, "", "", errs.New(
			errs.KindValidationFailed,
			"page limit must be between 1 and 200",
		)
	}
	if request.Revision < 0 {
		return 0, 0, "", "", errs.New(
			errs.KindValidationFailed,
			"page revision must not be negative",
		)
	}
	query, err := queryDigest(collection, ownerKind, ownerID, limit)
	if err != nil {
		return 0, 0, "", "", err
	}
	if request.Cursor == "" {
		return limit, request.Revision, "", query, nil
	}
	cursor, err := decodeCursor(request.Cursor)
	if err != nil {
		return 0, 0, "", "", err
	}
	if cursor.Query != query {
		return 0, 0, "", "", errs.New(errs.KindMalformedRequest, "cursor does not match the list query")
	}
	if request.Revision > 0 && request.Revision != cursor.Revision {
		return 0, 0, "", "", errs.New(errs.KindMalformedRequest, "cursor does not match the fixed revision")
	}
	if ids.Validate(idKind, cursor.LastID) != nil {
		return 0, 0, "", "", errs.New(errs.KindMalformedRequest, "cursor contains an invalid last id")
	}
	return limit, cursor.Revision, prefix + cursor.LastID, query, nil
}

func queryDigest(collection string, ownerKind string, ownerID string, limit int) (string, error) {
	encoded, err := json.Marshal(cursorQuery{
		Collection: collection,
		OwnerKind:  ownerKind,
		OwnerID:    ownerID,
		Order:      cursorOrder,
		Limit:      limit,
	})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(encoded)
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func encodeCursor(cursor cursorPayload) (string, error) {
	encoded, err := json.Marshal(cursor)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeCursor(value string) (cursorPayload, error) {
	if len(value) > maximumEncodedCursorBytes {
		return cursorPayload{}, errs.New(errs.KindMalformedRequest, "cursor is too long")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != value {
		return cursorPayload{}, errs.New(errs.KindMalformedRequest, "cursor is malformed")
	}
	if err := recordcodec.RejectDuplicateFields(decoded); err != nil {
		return cursorPayload{}, errs.New(errs.KindMalformedRequest, "cursor is malformed")
	}
	decoder := json.NewDecoder(bytes.NewReader(decoded))
	decoder.DisallowUnknownFields()
	var cursor cursorPayload
	if err := decoder.Decode(&cursor); err != nil || recordcodec.RequireEOF(decoder) != nil {
		return cursorPayload{}, errs.New(errs.KindMalformedRequest, "cursor is malformed")
	}
	if cursor.Version != cursorVersion || cursor.Revision <= 0 || cursor.LastID == "" || cursor.Query == "" {
		return cursorPayload{}, errs.New(errs.KindMalformedRequest, "cursor is malformed")
	}
	return cursor, nil
}

func validateListKey(prefix string, key string, kind ids.Kind) error {
	if !strings.HasPrefix(key, prefix) {
		return fmt.Errorf("key is outside prefix")
	}
	id := strings.TrimPrefix(key, prefix)
	if strings.Contains(id, "/") {
		return fmt.Errorf("key has nested segments")
	}
	return ids.Validate(kind, id)
}

func listPrimaryPage[T any](
	ctx context.Context,
	store hierarchyStore,
	collection string,
	ownerKind string,
	ownerID string,
	prefix string,
	idKind ids.Kind,
	request PageRequest,
	decode func([]byte) (T, error),
	identity func(T) string,
	matches func(T) bool,
) (Page[T], error) {
	if err := validateContext(ctx); err != nil {
		return Page[T]{}, err
	}
	limit, revision, start, query, err := normalizePageRequest(
		request, collection, ownerKind, ownerID, prefix, idKind,
	)
	if err != nil {
		return Page[T]{}, err
	}
	rangeResult, err := store.Range(ctx, etcdstore.RangeRequest{
		Prefix: prefix, StartExclusive: start, Limit: int64(limit), Revision: revision,
	})
	if err != nil {
		return Page[T]{}, err
	}
	defer clearRangeKeyValues(rangeResult.Values)
	items := make([]Versioned[T], 0, len(rangeResult.Values))
	for _, value := range rangeResult.Values {
		if err := validateListKey(prefix, value.Key, idKind); err != nil {
			return Page[T]{}, errs.New(errs.KindInternal, "primary list contains an invalid key")
		}
		record, err := decode(value.Value)
		if err != nil {
			return Page[T]{}, err
		}
		expectedID := strings.TrimPrefix(value.Key, prefix)
		if identity(record) != expectedID {
			return Page[T]{}, errs.New(errs.KindInternal, "primary list key does not match its record id")
		}
		if !matches(record) {
			return Page[T]{}, errs.New(errs.KindInternal, "primary list contains a record outside its scope")
		}
		items = append(items, Versioned[T]{
			Record: record, Revision: value.ModRevision, ReadRevision: rangeResult.ReadRevision,
		})
	}
	next, err := nextPageCursor(rangeResult, query, idKind, prefix)
	if err != nil {
		return Page[T]{}, err
	}
	return Page[T]{Items: items, NextCursor: next, Revision: rangeResult.ReadRevision}, nil
}

func listFilteredPrimaryPage[T any](
	ctx context.Context,
	store hierarchyStore,
	collection string,
	filterKind string,
	filterID string,
	prefix string,
	idKind ids.Kind,
	request PageRequest,
	decode func([]byte) (T, error),
	identity func(T) string,
	matches func(T) bool,
) (Page[T], error) {
	if err := validateContext(ctx); err != nil {
		return Page[T]{}, err
	}
	limit, revision, start, query, err := normalizePageRequest(
		request, collection, filterKind, filterID, prefix, idKind,
	)
	if err != nil {
		return Page[T]{}, err
	}
	items := make([]Versioned[T], 0, limit)
	continuations := make([]etcdstore.KeyValue, 0, limit)
	readRevision := revision
	for {
		rangeResult, err := store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: int64(MaximumPageLimit), Revision: readRevision,
		})
		if err != nil {
			return Page[T]{}, err
		}
		if rangeResult.ReadRevision <= 0 || (readRevision > 0 && rangeResult.ReadRevision != readRevision) {
			return Page[T]{}, errs.New(errs.KindInternal, "filtered primary list changed revision")
		}
		readRevision = rangeResult.ReadRevision
		for _, value := range rangeResult.Values {
			if err := validateListKey(prefix, value.Key, idKind); err != nil {
				return Page[T]{}, errs.New(errs.KindInternal, "filtered primary list contains an invalid key")
			}
			record, err := decode(value.Value)
			if err != nil {
				return Page[T]{}, err
			}
			expectedID := strings.TrimPrefix(value.Key, prefix)
			if identity(record) != expectedID {
				return Page[T]{}, errs.New(errs.KindInternal, "filtered primary list key does not match its record id")
			}
			if !matches(record) {
				continue
			}
			if len(items) == limit {
				next, err := nextPageCursor(&etcdstore.RangeResult{
					Values: continuations, More: true, ReadRevision: readRevision,
				}, query, idKind, prefix)
				if err != nil {
					return Page[T]{}, err
				}
				return Page[T]{Items: items, NextCursor: next, Revision: readRevision}, nil
			}
			items = append(items, Versioned[T]{
				Record: record, Revision: value.ModRevision, ReadRevision: readRevision,
			})
			continuations = append(continuations, value)
		}
		if !rangeResult.More {
			return Page[T]{Items: items, Revision: readRevision}, nil
		}
		if len(rangeResult.Values) == 0 {
			return Page[T]{}, errs.New(errs.KindInternal, "filtered primary list returned an empty continuation")
		}
		start = rangeResult.Values[len(rangeResult.Values)-1].Key
	}
}

func listIndexPage[T any](
	ctx context.Context,
	store hierarchyStore,
	collection string,
	ownerKind string,
	ownerID string,
	prefix string,
	primaryKey func(string) string,
	idKind ids.Kind,
	request PageRequest,
	decode func([]byte) (T, error),
	identity func(T) string,
	matches func(T) bool,
) (Page[T], error) {
	return listIndexPageAtRevision(
		ctx, store, collection, ownerKind, ownerID, prefix, primaryKey, idKind,
		request, decode, identity, matches, 0,
	)
}

func listIndexPageAtRevision[T any](
	ctx context.Context,
	store hierarchyStore,
	collection string,
	ownerKind string,
	ownerID string,
	prefix string,
	primaryKey func(string) string,
	idKind ids.Kind,
	request PageRequest,
	decode func([]byte) (T, error),
	identity func(T) string,
	matches func(T) bool,
	anchorRevision int64,
) (Page[T], error) {
	if err := validateContext(ctx); err != nil {
		return Page[T]{}, err
	}
	limit, revision, start, query, err := normalizePageRequest(
		request, collection, ownerKind, ownerID, prefix, idKind,
	)
	if err != nil {
		return Page[T]{}, err
	}
	if revision == 0 {
		revision = anchorRevision
	} else if anchorRevision > 0 && revision != anchorRevision {
		return Page[T]{}, errs.New(errs.KindStateConflict, "list cursor Script-set generation changed")
	}
	rangeResult, err := store.Range(ctx, etcdstore.RangeRequest{
		Prefix: prefix, StartExclusive: start, Limit: int64(limit), Revision: revision,
	})
	if err != nil {
		return Page[T]{}, err
	}
	defer clearRangeKeyValues(rangeResult.Values)
	if len(rangeResult.Values) == 0 {
		return Page[T]{Items: []Versioned[T]{}, Revision: rangeResult.ReadRevision}, nil
	}
	primaryKeys := make([]string, len(rangeResult.Values))
	expectedIDs := make([]string, len(rangeResult.Values))
	for index, value := range rangeResult.Values {
		if err := validateListKey(prefix, value.Key, idKind); err != nil {
			return Page[T]{}, errs.New(errs.KindInternal, "owner index contains an invalid key")
		}
		id := strings.TrimPrefix(value.Key, prefix)
		if string(value.Value) != id {
			return Page[T]{}, errs.New(errs.KindInternal, "owner index value does not match its key")
		}
		primaryKeys[index] = primaryKey(id)
		expectedIDs[index] = id
	}
	primaries, err := getManyBatchedAtRevision(ctx, store, primaryKeys, rangeResult.ReadRevision)
	if err != nil {
		return Page[T]{}, err
	}
	defer clearKeyValues(primaries.Values)
	if len(primaries.Values) != len(primaryKeys) {
		return Page[T]{}, errs.New(errs.KindInternal, "owner index read returned an invalid primary count")
	}
	items := make([]Versioned[T], 0, len(primaryKeys))
	for index, value := range primaries.Values {
		if value == nil {
			return Page[T]{}, errs.New(errs.KindInternal, "owner index references a missing primary record")
		}
		record, err := decode(value.Value)
		if err != nil {
			return Page[T]{}, err
		}
		if identity(record) != expectedIDs[index] {
			return Page[T]{}, errs.New(errs.KindInternal, "owner index id does not match its primary record")
		}
		if !matches(record) {
			return Page[T]{}, errs.New(errs.KindInternal, "owner index does not match its primary record")
		}
		items = append(items, Versioned[T]{
			Record: record, Revision: value.ModRevision, ReadRevision: rangeResult.ReadRevision,
		})
	}
	next, err := nextPageCursor(rangeResult, query, idKind, prefix)
	if err != nil {
		return Page[T]{}, err
	}
	return Page[T]{Items: items, NextCursor: next, Revision: rangeResult.ReadRevision}, nil
}

func getManyBatchedAtRevision(
	ctx context.Context,
	store hierarchyStore,
	keys []string,
	revision int64,
) (*etcdstore.GetManyResult, error) {
	if len(keys) == 0 || revision <= 0 {
		return nil, errs.New(errs.KindInternal, "fixed-revision batched read is invalid")
	}
	values := make([]*etcdstore.KeyValue, 0, len(keys))
	responseRevision := int64(0)
	for start := 0; start < len(keys); start += etcdstore.MaximumOperations {
		end := min(start+etcdstore.MaximumOperations, len(keys))
		batch, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys[start:end], Revision: revision})
		if err != nil {
			if batch != nil {
				clearKeyValues(batch.Values)
			}
			clearKeyValues(values)
			return nil, err
		}
		if batch == nil || batch.ReadRevision != revision || len(batch.Values) != end-start {
			if batch != nil {
				clearKeyValues(batch.Values)
			}
			clearKeyValues(values)
			return nil, errs.New(errs.KindInternal, "fixed-revision batched read is incomplete")
		}
		values = append(values, batch.Values...)
		responseRevision = max(responseRevision, batch.ResponseRevision)
	}
	return &etcdstore.GetManyResult{
		Values: values, ReadRevision: revision, ResponseRevision: responseRevision,
	}, nil
}

func clearRangeKeyValues(values []etcdstore.KeyValue) {
	for index := range values {
		clear(values[index].Value)
	}
}

func nextPageCursor(result *etcdstore.RangeResult, query string, kind ids.Kind, prefix string) (string, error) {
	if !result.More {
		return "", nil
	}
	if len(result.Values) == 0 || result.ReadRevision <= 0 {
		return "", errs.New(errs.KindInternal, "paginated range returned an invalid continuation")
	}
	lastKey := result.Values[len(result.Values)-1].Key
	if err := validateListKey(prefix, lastKey, kind); err != nil {
		return "", errs.New(errs.KindInternal, "paginated range returned an invalid continuation key")
	}
	lastID := strings.TrimPrefix(lastKey, prefix)
	return encodeCursor(cursorPayload{
		Version: cursorVersion, Revision: result.ReadRevision, LastID: lastID, Query: query,
	})
}
