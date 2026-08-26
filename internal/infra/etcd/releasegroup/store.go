// Package releasegroup persists the Release Group aggregate and its indexes.
package releasegroup

import (
	"bytes"
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/releasegroup"
	infraetcd "github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	defaultPageLimit = 50
	maximumPageLimit = 200
)

// Versioned binds a domain value to the exact MVCC revisions used to read it.
type Versioned struct {
	Group        domain.Group
	Revision     int64
	ReadRevision int64
}

// PageRequest is a bounded, fixed-revision Release Group list request.
type PageRequest struct {
	Limit  int
	Cursor string
}

// Page is one environment-scoped fixed-revision result page.
type Page struct {
	Items      []Versioned
	NextCursor string
	Revision   int64
}

// Removal reports whether this call deleted the record or replayed an already
// completed deletion.
type Removal struct {
	ID            string
	Revision      int64
	AlreadyAbsent bool
}

// Store is the concrete etcd Release Group adapter.
type Store struct {
	backend infraetcd.Store
}

func New(backend infraetcd.Store) (*Store, error) {
	if backend == nil {
		return nil, errs.New(errs.KindInternal, "release group etcd store is required")
	}
	return &Store{backend: backend}, nil
}

func (store *Store) createDirectFixture(ctx context.Context, group domain.Group) (Versioned, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned{}, err
	}
	if err := domain.Validate(group); err != nil {
		return Versioned{}, err
	}

	existing, found, err := store.find(ctx, group.ID)
	if err != nil {
		return Versioned{}, err
	}
	if found {
		if domain.Equal(existing.Group, group) {
			return existing, nil
		}
		return Versioned{}, errs.New(errs.KindStateConflict, "release group id already identifies different desired state")
	}

	evidence, err := store.loadMutationEvidence(ctx, group, "", 0)
	if err != nil {
		return Versioned{}, err
	}
	value, err := encodeGroup(group)
	if err != nil {
		return Versioned{}, err
	}
	result, err := store.backend.Transact(ctx, evidence.conditions, []infraetcd.Mutation{
		{Type: infraetcd.MutationPut, Key: recordKey(group.ID), Value: value},
		{Type: infraetcd.MutationPut, Key: ownerKey(group.EnvironmentID, group.ID), Value: []byte(group.ID)},
		{Type: infraetcd.MutationPut, Key: nameKey(group.EnvironmentID, group.Name), Value: []byte(group.ID)},
		{Type: infraetcd.MutationPut, Key: environmentMutationEpochKey(group.EnvironmentID), Value: evidence.epochValue},
	})
	clear(value)
	if err != nil {
		return Versioned{}, err
	}
	if !result.Succeeded {
		return Versioned{}, store.classifyConflict(ctx, group, 0)
	}
	return Versioned{Group: domain.Clone(group), Revision: result.Revision, ReadRevision: result.Revision}, nil
}

// PrepareCreate seals the exact Release Group and hierarchy transaction for
// atomic publication with a protected Controller Task.
func (store *Store) PrepareCreate(ctx context.Context, group domain.Group) (infraetcd.ReleaseGroupPreparedMutation, error) {
	if err := validateContext(ctx); err != nil {
		return infraetcd.ReleaseGroupPreparedMutation{}, err
	}
	if err := domain.Validate(group); err != nil {
		return infraetcd.ReleaseGroupPreparedMutation{}, err
	}
	if _, found, err := store.find(ctx, group.ID); err != nil {
		return infraetcd.ReleaseGroupPreparedMutation{}, err
	} else if found {
		return infraetcd.ReleaseGroupPreparedMutation{}, errs.New(errs.KindStateConflict, "release group id already exists")
	}
	evidence, err := store.loadMutationEvidence(ctx, group, "", 0)
	if err != nil {
		return infraetcd.ReleaseGroupPreparedMutation{}, err
	}
	value, err := encodeGroup(group)
	if err != nil {
		return infraetcd.ReleaseGroupPreparedMutation{}, err
	}
	return infraetcd.ReleaseGroupPreparedMutation{
		EnvironmentID: group.EnvironmentID, GroupID: group.ID, Type: infraetcd.TaskCreate,
		Conditions: append([]infraetcd.Condition(nil), evidence.conditions...),
		Mutations: []infraetcd.Mutation{
			{Type: infraetcd.MutationPut, Key: recordKey(group.ID), Value: value},
			{Type: infraetcd.MutationPut, Key: ownerKey(group.EnvironmentID, group.ID), Value: []byte(group.ID)},
			{Type: infraetcd.MutationPut, Key: nameKey(group.EnvironmentID, group.Name), Value: []byte(group.ID)},
			{Type: infraetcd.MutationPut, Key: environmentMutationEpochKey(group.EnvironmentID), Value: evidence.epochValue},
		},
	}, nil
}

func (store *Store) Get(ctx context.Context, id string) (Versioned, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned{}, err
	}
	if ids.Validate(ids.KindReleaseGroup, id) != nil {
		return Versioned{}, errs.New(errs.KindValidationFailed, "release group id is invalid")
	}
	stored, found, err := store.find(ctx, id)
	if err != nil {
		return Versioned{}, err
	}
	if !found {
		return Versioned{}, errs.New(errs.KindReleaseGroupNotFound, "release group was not found")
	}
	return stored, nil
}

// GetAtRevision resolves the primary and owner index from one caller-selected
// planning snapshot. Release publication uses this to freeze group membership
// without observing a newer metadata edit.
func (store *Store) GetAtRevision(ctx context.Context, id string, revision int64) (Versioned, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned{}, err
	}
	if ids.Validate(ids.KindReleaseGroup, id) != nil || revision <= 0 {
		return Versioned{}, errs.New(errs.KindValidationFailed, "release group revision read is invalid")
	}
	primary, err := store.backend.GetMany(ctx, infraetcd.GetManyRequest{
		Keys: []string{recordKey(id)}, Revision: revision,
	})
	if err != nil {
		return Versioned{}, err
	}
	if primary == nil || primary.ReadRevision != revision || len(primary.Values) != 1 {
		return Versioned{}, corruptRecord()
	}
	if primary.Values[0] == nil {
		return Versioned{}, errs.New(errs.KindReleaseGroupNotFound, "release group was not found")
	}
	group, err := decodeGroup(primary.Values[0].Value)
	if err != nil || group.ID != id {
		return Versioned{}, corruptRecord()
	}
	owner, err := store.backend.GetMany(ctx, infraetcd.GetManyRequest{
		Keys: []string{ownerKey(group.EnvironmentID, id)}, Revision: revision,
	})
	if err != nil {
		return Versioned{}, err
	}
	if owner == nil || owner.ReadRevision != revision || len(owner.Values) != 1 || owner.Values[0] == nil ||
		!bytes.Equal(owner.Values[0].Value, []byte(id)) {
		return Versioned{}, corruptRecord()
	}
	return Versioned{
		Group: domain.Clone(group), Revision: primary.Values[0].ModRevision, ReadRevision: revision,
	}, nil
}

func (store *Store) Resolve(ctx context.Context, environmentID string, name string) (Versioned, error) {
	if err := validateScope(environmentID, name); err != nil {
		return Versioned{}, err
	}
	index, err := store.backend.Get(ctx, nameKey(environmentID, strings.TrimSpace(name)))
	if err != nil {
		return Versioned{}, err
	}
	if index == nil || index.Entry == nil {
		return Versioned{}, errs.New(errs.KindReleaseGroupNotFound, "release group was not found")
	}
	id := string(index.Entry.Value)
	if ids.Validate(ids.KindReleaseGroup, id) != nil {
		return Versioned{}, corruptRecord()
	}
	result, err := store.backend.GetMany(ctx, infraetcd.GetManyRequest{
		Keys: []string{recordKey(id), ownerKey(environmentID, id)}, Revision: index.ReadRevision,
	})
	if err != nil {
		return Versioned{}, err
	}
	if result == nil || result.ReadRevision != index.ReadRevision || len(result.Values) != 2 ||
		result.Values[0] == nil || result.Values[1] == nil {
		return Versioned{}, errs.New(errs.KindStateConflict, "release group name index changed")
	}
	group, err := decodeGroup(result.Values[0].Value)
	if err != nil {
		return Versioned{}, err
	}
	if group.ID != id || group.EnvironmentID != environmentID || group.Name != name ||
		!bytes.Equal(result.Values[1].Value, []byte(id)) {
		return Versioned{}, corruptRecord()
	}
	return Versioned{Group: group, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision}, nil
}

func (store *Store) List(ctx context.Context, environmentID string, request PageRequest) (Page, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return Page{}, errs.New(errs.KindValidationFailed, "release group environment id is invalid")
	}
	limit := request.Limit
	if limit == 0 {
		limit = defaultPageLimit
	}
	if limit < 1 || limit > maximumPageLimit {
		return Page{}, errs.New(errs.KindMalformedRequest, "release group page limit must be between 1 and 200")
	}

	revision := int64(0)
	start := ""
	if request.Cursor != "" {
		decoded, err := decodeCursor(request.Cursor)
		if err != nil {
			return Page{}, err
		}
		if decoded.EnvironmentID != environmentID || decoded.Limit != limit {
			return Page{}, invalidCursor()
		}
		revision = decoded.Revision
		start = ownerKey(environmentID, decoded.LastID)
		anchor, err := store.backend.GetMany(ctx, infraetcd.GetManyRequest{
			Keys: []string{start}, Revision: revision,
		})
		if err != nil {
			return Page{}, err
		}
		if anchor == nil || anchor.ReadRevision != revision || len(anchor.Values) != 1 ||
			anchor.Values[0] == nil || !bytes.Equal(anchor.Values[0].Value, []byte(decoded.LastID)) {
			return Page{}, invalidCursor()
		}
	}
	ranged, err := store.backend.Range(ctx, infraetcd.RangeRequest{
		Prefix: ownerScopePrefix(environmentID), StartExclusive: start,
		Limit: int64(limit + 1), Revision: revision,
	})
	if err != nil {
		return Page{}, err
	}
	if ranged == nil || ranged.ReadRevision <= 0 {
		return Page{}, errs.New(errs.KindInternal, "release group owner index page is empty")
	}
	if revision > 0 && ranged.ReadRevision != revision {
		return Page{}, errs.New(errs.KindInternal, "release group cursor revision changed")
	}
	values := ranged.Values
	more := len(values) > limit
	if more {
		values = values[:limit]
	}
	keys := make([]string, 0, len(values)*2)
	idsInPage := make([]string, len(values))
	for index, value := range values {
		id := strings.TrimPrefix(value.Key, ownerScopePrefix(environmentID))
		if ids.Validate(ids.KindReleaseGroup, id) != nil || !bytes.Equal(value.Value, []byte(id)) {
			return Page{}, corruptRecord()
		}
		idsInPage[index] = id
		keys = append(keys, recordKey(id))
	}
	items := make([]Versioned, 0, len(keys))
	if len(keys) > 0 {
		records, err := store.backend.GetMany(ctx, infraetcd.GetManyRequest{Keys: keys, Revision: ranged.ReadRevision})
		if err != nil {
			return Page{}, err
		}
		if records == nil || records.ReadRevision != ranged.ReadRevision || len(records.Values) != len(keys) {
			return Page{}, errs.New(errs.KindInternal, "release group list evidence is incomplete")
		}
		for index, value := range records.Values {
			if value == nil {
				return Page{}, corruptRecord()
			}
			group, err := decodeGroup(value.Value)
			if err != nil || group.ID != idsInPage[index] || group.EnvironmentID != environmentID {
				return Page{}, corruptRecord()
			}
			nameIndex, indexErr := store.backend.GetMany(ctx, infraetcd.GetManyRequest{
				Keys: []string{nameKey(environmentID, group.Name)}, Revision: ranged.ReadRevision,
			})
			if indexErr != nil {
				return Page{}, indexErr
			}
			if nameIndex == nil || nameIndex.ReadRevision != ranged.ReadRevision || len(nameIndex.Values) != 1 ||
				nameIndex.Values[0] == nil || !bytes.Equal(nameIndex.Values[0].Value, []byte(group.ID)) {
				return Page{}, corruptRecord()
			}
			items = append(items, Versioned{Group: group, Revision: value.ModRevision, ReadRevision: records.ReadRevision})
		}
	}
	page := Page{Items: items, Revision: ranged.ReadRevision}
	if more {
		page.NextCursor, err = encodeCursor(cursor{
			Version: 1, Revision: ranged.ReadRevision, EnvironmentID: environmentID,
			LastID: idsInPage[len(idsInPage)-1], Limit: limit,
		})
		if err != nil {
			return Page{}, err
		}
	}
	return page, nil
}

func (store *Store) updateDirectFixture(ctx context.Context, current Versioned, replacement domain.Group) (Versioned, error) {
	if err := validateVersion(current); err != nil {
		return Versioned{}, err
	}
	if err := domain.Validate(replacement); err != nil {
		return Versioned{}, err
	}
	if current.Group.ID != replacement.ID || current.Group.EnvironmentID != replacement.EnvironmentID {
		return Versioned{}, errs.New(errs.KindValidationFailed, "release group update changed stable identity or ownership")
	}
	stored, found, err := store.find(ctx, current.Group.ID)
	if err != nil {
		return Versioned{}, err
	}
	if !found {
		return Versioned{}, errs.New(errs.KindReleaseGroupNotFound, "release group was not found")
	}
	if domain.Equal(stored.Group, replacement) && stored.Revision >= current.Revision {
		return stored, nil
	}
	if stored.Revision != current.Revision || !domain.Equal(stored.Group, current.Group) {
		return Versioned{}, errs.New(errs.KindStateConflict, "release group changed before update")
	}
	evidence, err := store.loadMutationEvidence(ctx, replacement, current.Group.Name, current.Revision)
	if err != nil {
		return Versioned{}, err
	}
	value, err := encodeGroup(replacement)
	if err != nil {
		return Versioned{}, err
	}
	mutations := []infraetcd.Mutation{{Type: infraetcd.MutationPut, Key: recordKey(replacement.ID), Value: value}}
	mutations = append(mutations, infraetcd.Mutation{
		Type: infraetcd.MutationPut, Key: environmentMutationEpochKey(replacement.EnvironmentID), Value: evidence.epochValue,
	})
	if replacement.Name != current.Group.Name {
		mutations = append(mutations,
			infraetcd.Mutation{Type: infraetcd.MutationDelete, Key: nameKey(replacement.EnvironmentID, current.Group.Name)},
			infraetcd.Mutation{Type: infraetcd.MutationPut, Key: nameKey(replacement.EnvironmentID, replacement.Name), Value: []byte(replacement.ID)},
		)
	}
	result, err := store.backend.Transact(ctx, evidence.conditions, mutations)
	clear(value)
	if err != nil {
		return Versioned{}, err
	}
	if !result.Succeeded {
		return Versioned{}, store.classifyConflict(ctx, replacement, current.Revision)
	}
	return Versioned{Group: domain.Clone(replacement), Revision: result.Revision, ReadRevision: result.Revision}, nil
}

func (store *Store) PrepareUpdate(ctx context.Context, current Versioned, replacement domain.Group) (infraetcd.ReleaseGroupPreparedMutation, error) {
	if err := validateVersion(current); err != nil {
		return infraetcd.ReleaseGroupPreparedMutation{}, err
	}
	if err := domain.Validate(replacement); err != nil {
		return infraetcd.ReleaseGroupPreparedMutation{}, err
	}
	if current.Group.ID != replacement.ID || current.Group.EnvironmentID != replacement.EnvironmentID {
		return infraetcd.ReleaseGroupPreparedMutation{}, errs.New(errs.KindValidationFailed, "release group update changed stable identity or ownership")
	}
	evidence, err := store.loadMutationEvidence(ctx, replacement, current.Group.Name, current.Revision)
	if err != nil {
		return infraetcd.ReleaseGroupPreparedMutation{}, err
	}
	value, err := encodeGroup(replacement)
	if err != nil {
		return infraetcd.ReleaseGroupPreparedMutation{}, err
	}
	mutations := []infraetcd.Mutation{
		{Type: infraetcd.MutationPut, Key: recordKey(replacement.ID), Value: value},
		{Type: infraetcd.MutationPut, Key: environmentMutationEpochKey(replacement.EnvironmentID), Value: evidence.epochValue},
	}
	if replacement.Name != current.Group.Name {
		mutations = append(mutations,
			infraetcd.Mutation{Type: infraetcd.MutationDelete, Key: nameKey(replacement.EnvironmentID, current.Group.Name)},
			infraetcd.Mutation{Type: infraetcd.MutationPut, Key: nameKey(replacement.EnvironmentID, replacement.Name), Value: []byte(replacement.ID)},
		)
	}
	return infraetcd.ReleaseGroupPreparedMutation{
		EnvironmentID: replacement.EnvironmentID, GroupID: replacement.ID, GroupRevision: current.Revision,
		Type: infraetcd.TaskUpdate, Conditions: append([]infraetcd.Condition(nil), evidence.conditions...), Mutations: mutations,
	}, nil
}

func (store *Store) PrepareRemove(ctx context.Context, current Versioned) (infraetcd.ReleaseGroupPreparedMutation, error) {
	if err := validateVersion(current); err != nil {
		return infraetcd.ReleaseGroupPreparedMutation{}, err
	}
	evidence, err := store.loadMutationEvidence(ctx, current.Group, current.Group.Name, current.Revision)
	if err != nil {
		return infraetcd.ReleaseGroupPreparedMutation{}, err
	}
	return infraetcd.ReleaseGroupPreparedMutation{
		EnvironmentID: current.Group.EnvironmentID, GroupID: current.Group.ID, GroupRevision: current.Revision,
		Type: infraetcd.TaskRemove, Conditions: append([]infraetcd.Condition(nil), evidence.conditions...),
		Mutations: []infraetcd.Mutation{{
			Type: infraetcd.MutationPut, Key: environmentMutationEpochKey(current.Group.EnvironmentID), Value: evidence.epochValue,
		}},
	}, nil
}

func (store *Store) removeDirectForbidden(ctx context.Context, current Versioned) (Removal, error) {
	if err := validateVersion(current); err != nil {
		return Removal{}, err
	}
	return Removal{}, errs.New(
		errs.KindInternal,
		"release group removal requires the protected task and tombstone finalizer",
	)
}

type mutationEvidence struct {
	conditions []infraetcd.Condition
	epochValue []byte
}

func (store *Store) loadMutationEvidence(ctx context.Context, group domain.Group, oldName string, revision int64) (mutationEvidence, error) {
	baseKeys := []string{
		environmentKey(group.EnvironmentID), environmentMutationEpochKey(group.EnvironmentID),
		environmentOperationLockKey(group.EnvironmentID), environmentDeletionKey(group.EnvironmentID),
		recordKey(group.ID), ownerKey(group.EnvironmentID, group.ID), nameKey(group.EnvironmentID, group.Name),
		environmentComposeKey(group.EnvironmentID), deletionKey("release_group", group.ID),
	}
	if oldName != "" && oldName != group.Name {
		baseKeys = append(baseKeys, nameKey(group.EnvironmentID, oldName))
	}
	result, err := store.backend.GetMany(ctx, infraetcd.GetManyRequest{Keys: baseKeys})
	if err != nil {
		return mutationEvidence{}, err
	}
	if result == nil || len(result.Values) != len(baseKeys) || result.Values[0] == nil {
		return mutationEvidence{}, errs.New(errs.KindEnvironmentNotFound, "release group environment was not found")
	}
	environment, err := decodeEnvironment(result.Values[0].Value)
	if err != nil || environment.ID != group.EnvironmentID {
		return mutationEvidence{}, corruptRecord()
	}
	if result.Values[1] == nil {
		return mutationEvidence{}, corruptRecord()
	}
	epochValue, err := decodeEpoch(result.Values[1].Value, group.EnvironmentID)
	if err != nil {
		return mutationEvidence{}, err
	}
	defer func() {
		if err != nil {
			clear(epochValue)
		}
	}()
	if result.Values[2] != nil || result.Values[3] != nil {
		return mutationEvidence{}, errs.New(errs.KindResourceInUse, "release group environment has an active operation or deletion")
	}
	if result.Values[7] == nil {
		return mutationEvidence{}, errs.New(errs.KindResourceInUse, "release group environment has no enabled compose project")
	}
	projection, err := decodeComposeProjection(result.Values[7].Value)
	if err != nil || projection.EnvironmentID != group.EnvironmentID {
		return mutationEvidence{}, corruptRecord()
	}
	if result.Values[8] != nil {
		return mutationEvidence{}, errs.New(errs.KindResourceInUse, "release group deletion is already in progress")
	}
	conditions := []infraetcd.Condition{
		{Key: baseKeys[0], ModRevision: result.Values[0].ModRevision},
		{Key: baseKeys[1], ModRevision: result.Values[1].ModRevision},
		{Key: baseKeys[2]}, {Key: baseKeys[3]},
		{Key: baseKeys[7], ModRevision: result.Values[7].ModRevision}, {Key: baseKeys[8]},
	}
	if revision == 0 {
		if result.Values[4] != nil || result.Values[5] != nil {
			return mutationEvidence{}, errs.New(errs.KindStateConflict, "release group stable id already exists")
		}
		if result.Values[6] != nil {
			return mutationEvidence{}, errs.New(errs.KindNameConflict, "release group name already exists in the environment")
		}
		conditions = append(conditions, infraetcd.Condition{Key: baseKeys[4]}, infraetcd.Condition{Key: baseKeys[5]}, infraetcd.Condition{Key: baseKeys[6]})
	} else {
		if result.Values[4] == nil || result.Values[4].ModRevision != revision || result.Values[5] == nil {
			return mutationEvidence{}, errs.New(errs.KindStateConflict, "release group changed before mutation")
		}
		stored, decodeErr := decodeGroup(result.Values[4].Value)
		if decodeErr != nil || stored.ID != group.ID || stored.EnvironmentID != group.EnvironmentID ||
			stored.Name != oldName || !bytes.Equal(result.Values[5].Value, []byte(group.ID)) {
			return mutationEvidence{}, corruptRecord()
		}
		conditions = append(conditions,
			infraetcd.Condition{Key: baseKeys[4], ModRevision: revision},
			infraetcd.Condition{Key: baseKeys[5], ModRevision: result.Values[5].ModRevision},
		)
		if oldName != group.Name {
			if result.Values[6] != nil {
				return mutationEvidence{}, errs.New(errs.KindNameConflict, "release group name already exists in the environment")
			}
			oldNameIndex := result.Values[9]
			if oldNameIndex == nil || !bytes.Equal(oldNameIndex.Value, []byte(group.ID)) {
				return mutationEvidence{}, corruptRecord()
			}
			conditions = append(conditions,
				infraetcd.Condition{Key: baseKeys[6]},
				infraetcd.Condition{Key: baseKeys[9], ModRevision: oldNameIndex.ModRevision},
			)
		} else {
			if result.Values[6] == nil || !bytes.Equal(result.Values[6].Value, []byte(group.ID)) {
				return mutationEvidence{}, corruptRecord()
			}
			conditions = append(conditions, infraetcd.Condition{Key: baseKeys[6], ModRevision: result.Values[6].ModRevision})
		}
	}

	projectResult, err := store.backend.GetMany(ctx, infraetcd.GetManyRequest{Keys: []string{
		projectKey(environment.ProjectID), environmentOwnerKey(environment.ProjectID, environment.ID),
		deletionKey("project", environment.ProjectID),
	}})
	if err != nil {
		return mutationEvidence{}, err
	}
	if projectResult == nil || len(projectResult.Values) != 3 || projectResult.Values[0] == nil ||
		projectResult.Values[1] == nil {
		return mutationEvidence{}, corruptRecord()
	}
	project, err := decodeProject(projectResult.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID ||
		!bytes.Equal(projectResult.Values[1].Value, []byte(environment.ID)) {
		return mutationEvidence{}, corruptRecord()
	}
	if projectResult.Values[2] != nil {
		return mutationEvidence{}, errs.New(errs.KindResourceInUse, "release group project deletion is in progress")
	}
	conditions = append(conditions,
		infraetcd.Condition{Key: projectKey(project.ID), ModRevision: projectResult.Values[0].ModRevision},
		infraetcd.Condition{Key: environmentOwnerKey(project.ID, environment.ID), ModRevision: projectResult.Values[1].ModRevision},
		infraetcd.Condition{Key: deletionKey("project", project.ID)},
	)
	ownerIndex := projectOwnerKey(project)
	ownerKeys := []string{ownerIndex}
	if project.Kind == infraetcd.ProjectKindTenant {
		ownerKeys = append(ownerKeys, tenantKey(project.TenantID), deletionKey("tenant", project.TenantID))
	}
	ownerResult, err := store.backend.GetMany(ctx, infraetcd.GetManyRequest{Keys: ownerKeys})
	if err != nil {
		return mutationEvidence{}, err
	}
	if ownerResult == nil || len(ownerResult.Values) != len(ownerKeys) || ownerResult.Values[0] == nil ||
		!bytes.Equal(ownerResult.Values[0].Value, []byte(project.ID)) {
		return mutationEvidence{}, corruptRecord()
	}
	conditions = append(conditions, infraetcd.Condition{Key: ownerIndex, ModRevision: ownerResult.Values[0].ModRevision})
	if project.Kind == infraetcd.ProjectKindTenant {
		if ownerResult.Values[1] == nil {
			return mutationEvidence{}, corruptRecord()
		}
		tenant, tenantErr := decodeTenant(ownerResult.Values[1].Value)
		if tenantErr != nil || tenant.ID != project.TenantID {
			return mutationEvidence{}, corruptRecord()
		}
		if ownerResult.Values[2] != nil {
			return mutationEvidence{}, errs.New(errs.KindResourceInUse, "release group tenant deletion is in progress")
		}
		conditions = append(conditions,
			infraetcd.Condition{Key: tenantKey(project.TenantID), ModRevision: ownerResult.Values[1].ModRevision},
			infraetcd.Condition{Key: deletionKey("tenant", project.TenantID)},
		)
	}

	projectionMembers := make(map[string]struct{}, len(projection.Services))
	for _, identity := range projection.Services {
		if ids.Validate(ids.KindService, identity.ID) != nil {
			return mutationEvidence{}, corruptRecord()
		}
		if _, duplicate := projectionMembers[identity.ID]; duplicate {
			return mutationEvidence{}, corruptRecord()
		}
		projectionMembers[identity.ID] = struct{}{}
	}
	serviceKeys := make([]string, 0, len(group.ServiceIDs)*3)
	for _, serviceID := range group.ServiceIDs {
		serviceKeys = append(serviceKeys, serviceKey(serviceID), serviceOwnerKey(group.EnvironmentID, serviceID), deletionKey("service", serviceID))
	}
	serviceResult, err := store.backend.GetMany(ctx, infraetcd.GetManyRequest{Keys: serviceKeys})
	if err != nil {
		return mutationEvidence{}, err
	}
	if serviceResult == nil || len(serviceResult.Values) != len(serviceKeys) {
		return mutationEvidence{}, corruptRecord()
	}
	for index, serviceID := range group.ServiceIDs {
		recordValue := serviceResult.Values[index*3]
		ownerValue := serviceResult.Values[index*3+1]
		deletionValue := serviceResult.Values[index*3+2]
		if recordValue == nil || ownerValue == nil {
			return mutationEvidence{}, errs.New(errs.KindServiceNotFound, "release group member service was not found in the environment")
		}
		service, decodeErr := decodeService(recordValue.Value)
		_, enabled := projectionMembers[serviceID]
		if decodeErr != nil || service.Desired.ID != serviceID || service.EnvironmentID != group.EnvironmentID ||
			!bytes.Equal(ownerValue.Value, []byte(serviceID)) {
			return mutationEvidence{}, corruptRecord()
		}
		if !enabled {
			return mutationEvidence{}, errs.New(errs.KindResourceInUse, "release group member service is not enabled in the environment compose project")
		}
		if deletionValue != nil {
			return mutationEvidence{}, errs.New(errs.KindResourceInUse, "release group member service deletion is in progress")
		}
		conditions = append(conditions,
			infraetcd.Condition{Key: serviceKey(serviceID), ModRevision: recordValue.ModRevision},
			infraetcd.Condition{Key: serviceOwnerKey(group.EnvironmentID, serviceID), ModRevision: ownerValue.ModRevision},
			infraetcd.Condition{Key: deletionKey("service", serviceID)},
		)
	}
	return mutationEvidence{conditions: conditions, epochValue: epochValue}, nil
}

func (store *Store) find(ctx context.Context, id string) (Versioned, bool, error) {
	result, err := store.backend.Get(ctx, recordKey(id))
	if err != nil {
		return Versioned{}, false, err
	}
	if result == nil {
		return Versioned{}, false, errs.New(errs.KindInternal, "release group read result is empty")
	}
	if result.Entry == nil {
		return Versioned{ReadRevision: result.ReadRevision}, false, nil
	}
	group, err := decodeGroup(result.Entry.Value)
	if err != nil {
		return Versioned{}, false, err
	}
	if group.ID != id {
		return Versioned{}, false, corruptRecord()
	}
	indexes, err := store.backend.GetMany(ctx, infraetcd.GetManyRequest{
		Keys:     []string{ownerKey(group.EnvironmentID, id), nameKey(group.EnvironmentID, group.Name)},
		Revision: result.ReadRevision,
	})
	if err != nil {
		return Versioned{}, false, err
	}
	if indexes == nil || indexes.ReadRevision != result.ReadRevision || len(indexes.Values) != 2 ||
		indexes.Values[0] == nil || indexes.Values[1] == nil ||
		!bytes.Equal(indexes.Values[0].Value, []byte(id)) ||
		!bytes.Equal(indexes.Values[1].Value, []byte(id)) {
		return Versioned{}, false, corruptRecord()
	}
	return Versioned{Group: group, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision}, true, nil
}

func (store *Store) classifyConflict(ctx context.Context, desired domain.Group, expectedRevision int64) error {
	lock, err := store.backend.GetMany(ctx, infraetcd.GetManyRequest{Keys: []string{
		environmentOperationLockKey(desired.EnvironmentID), environmentDeletionKey(desired.EnvironmentID),
		nameKey(desired.EnvironmentID, desired.Name), recordKey(desired.ID),
	}})
	if err != nil {
		return err
	}
	if lock != nil && len(lock.Values) == 4 {
		if lock.Values[0] != nil || lock.Values[1] != nil {
			return errs.New(errs.KindResourceInUse, "release group environment has an active operation or deletion")
		}
		if lock.Values[2] != nil && !bytes.Equal(lock.Values[2].Value, []byte(desired.ID)) {
			return errs.New(errs.KindNameConflict, "release group name already exists in the environment")
		}
		if expectedRevision > 0 && lock.Values[3] == nil {
			return errs.New(errs.KindReleaseGroupNotFound, "release group was not found")
		}
	}
	return errs.New(errs.KindStateConflict, "release group durable state changed")
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "release group context is required")
	}
	return ctx.Err()
}

func validateScope(environmentID string, name string) error {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || strings.TrimSpace(name) == "" ||
		name != strings.TrimSpace(name) {
		return errs.New(errs.KindValidationFailed, "release group environment id and name are required")
	}
	return nil
}

func validateVersion(value Versioned) error {
	if err := domain.Validate(value.Group); err != nil {
		return err
	}
	if value.Revision <= 0 || value.ReadRevision < value.Revision {
		return errs.New(errs.KindValidationFailed, "release group revision is invalid")
	}
	return nil
}
