// Package releasegroup persists the Release Group aggregate and its indexes.
package releasegroup

import (
	"bytes"
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/releasegroup"

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
	backend etcdstore.Store
}

func New(backend etcdstore.Store) (*Store, error) {
	if backend == nil {
		return nil, errs.New(errs.KindInternal, "release group etcd store is required")
	}
	return &Store{backend: backend}, nil
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
	primary, err := store.backend.GetMany(ctx, etcdstore.GetManyRequest{
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
	owner, err := store.backend.GetMany(ctx, etcdstore.GetManyRequest{
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
	index, err := store.backend.Get(ctx, nameKey(environmentID, name))
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
	result, err := store.backend.GetMany(ctx, etcdstore.GetManyRequest{
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
		anchor, err := store.backend.GetMany(ctx, etcdstore.GetManyRequest{
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
	ranged, err := store.backend.Range(ctx, etcdstore.RangeRequest{
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
		records, err := store.backend.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: ranged.ReadRevision})
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
			nameIndex, indexErr := store.backend.GetMany(ctx, etcdstore.GetManyRequest{
				Keys: []string{nameKey(environmentID, group.Name)}, Revision: ranged.ReadRevision,
			})
			if indexErr != nil {
				return Page{}, indexErr
			}
			if nameIndex == nil || nameIndex.ReadRevision != ranged.ReadRevision || len(nameIndex.Values) != 1 ||
				nameIndex.Values[0] == nil || !bytes.Equal(nameIndex.Values[0].Value, []byte(group.ID)) {
				return Page{}, corruptRecord()
			}
			items = append(
				items,
				Versioned{Group: group, Revision: value.ModRevision, ReadRevision: records.ReadRevision},
			)
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
