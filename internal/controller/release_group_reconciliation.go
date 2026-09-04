package controller

import (
	"context"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/releasegroup"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	etcdreleasegroup "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroup"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type releaseGroupBlueprintStore interface {
	List(context.Context, string, etcdreleasegroup.PageRequest) (etcdreleasegroup.Page, error)
}

type releaseGroupBlueprintMutationPreparer interface {
	PrepareReleaseGroupBlueprintMutation(
		context.Context,
		string,
		int64,
		[]etcd.ReleaseGroupSnapshotEntry,
		[]domain.Group,
	) (etcd.ReleaseGroupBlueprintPreparedMutation, error)
}

// ReleaseGroupBlueprintPlanner owns Release Group desired-state orchestration.
// The app composition root only wires this capability into Blueprint apply.
type ReleaseGroupBlueprintPlanner struct {
	groups    releaseGroupBlueprintStore
	mutations releaseGroupBlueprintMutationPreparer
}

// NewReleaseGroupBlueprintPlanner constructs the controller-owned planner.
func NewReleaseGroupBlueprintPlanner(
	groups releaseGroupBlueprintStore,
	mutations releaseGroupBlueprintMutationPreparer,
) (*ReleaseGroupBlueprintPlanner, error) {
	if groups == nil || mutations == nil {
		return nil, errs.New(errs.KindInternal, "Release Group Blueprint planner is not configured")
	}
	return &ReleaseGroupBlueprintPlanner{groups: groups, mutations: mutations}, nil
}

// Prepare returns one opaque persistence fragment for the parent Blueprint
// publication transaction.
func (planner *ReleaseGroupBlueprintPlanner) Prepare(
	ctx context.Context,
	environmentID string,
	specs map[string]core.ReleaseGroupSpec,
	services []core.Service,
	allocate ReleaseGroupIDAllocator,
) (etcd.ReleaseGroupBlueprintPreparedMutation, error) {
	current, readRevision, err := planner.snapshot(ctx, environmentID)
	if err != nil {
		return etcd.ReleaseGroupBlueprintPreparedMutation{}, err
	}
	previous := make([]ReleaseGroupIdentity, len(current))
	snapshot := make([]etcd.ReleaseGroupSnapshotEntry, len(current))
	for index, versioned := range current {
		previous[index] = ReleaseGroupIdentity{
			ID:   versioned.Group.ID,
			Name: versioned.Group.Name,
		}
		snapshot[index] = etcd.ReleaseGroupSnapshotEntry{
			Group:        versioned.Group,
			Revision:     versioned.Revision,
			ReadRevision: versioned.ReadRevision,
		}
	}
	reconciled, err := ReconcileBlueprintReleaseGroups(
		environmentID,
		specs,
		services,
		previous,
		allocate,
	)
	if err != nil {
		return etcd.ReleaseGroupBlueprintPreparedMutation{}, err
	}
	return planner.mutations.PrepareReleaseGroupBlueprintMutation(
		ctx,
		environmentID,
		readRevision,
		snapshot,
		reconciled.Desired,
	)
}

func (planner *ReleaseGroupBlueprintPlanner) snapshot(
	ctx context.Context,
	environmentID string,
) ([]etcdreleasegroup.Versioned, int64, error) {
	var groups []etcdreleasegroup.Versioned
	cursor := ""
	var readRevision int64
	for {
		page, err := planner.groups.List(ctx, environmentID, etcdreleasegroup.PageRequest{
			Limit:  200,
			Cursor: cursor,
		})
		if err != nil {
			return nil, 0, err
		}
		if readRevision == 0 {
			readRevision = page.Revision
		} else if page.Revision != readRevision {
			return nil, 0, errs.New(
				errs.KindInternal,
				"Blueprint Release Group list changed revision",
			)
		}
		groups = append(groups, page.Items...)
		if page.NextCursor == "" {
			return groups, readRevision, nil
		}
		cursor = page.NextCursor
	}
}

// AuthoringSpecs projects current Release Groups back to portable service
// labels. Stable ids remain outside the authored Blueprint.
func (planner *ReleaseGroupBlueprintPlanner) AuthoringSpecs(
	ctx context.Context,
	environmentID string,
	serviceNames map[string]string,
) (map[string]core.ReleaseGroupSpec, error) {
	groups, _, err := planner.snapshot(ctx, environmentID)
	if err != nil {
		return nil, err
	}
	specs := make(map[string]core.ReleaseGroupSpec, len(groups))
	for _, versioned := range groups {
		group := versioned.Group
		services := make([]string, len(group.ServiceIDs))
		for index, id := range group.ServiceIDs {
			name, found := serviceNames[id]
			if !found {
				return nil, errs.New(errs.KindInternal, "Release Group Blueprint service identity is missing")
			}
			services[index] = name
		}
		order := make([]string, len(group.Order))
		for index, id := range group.Order {
			name, found := serviceNames[id]
			if !found {
				return nil, errs.New(errs.KindInternal, "Release Group Blueprint order identity is missing")
			}
			order[index] = name
		}
		specs[group.Name] = core.ReleaseGroupSpec{
			Services:  services,
			Order:     order,
			Tag:       group.DefaultTag,
			OnFailure: core.OnFailure(group.OnFailure),
		}
	}
	return specs, nil
}

// ReleaseGroupIdentity is the stable identity retained from the previously
// applied Blueprint. Names are labels and may be changed by an operator;
// matching a current name is what permits ID reuse during reconciliation.
type ReleaseGroupIdentity struct {
	ID   string
	Name string
}

// ReleaseGroupBlueprintReconciliation is the deterministic desired-state
// result. Desired groups are ready for the Release Group repository; removed
// identities are finalized through the normal protected deletion task path.
type ReleaseGroupBlueprintReconciliation struct {
	Desired []domain.Group
	Removed []ReleaseGroupIdentity
}

// ReleaseGroupIDAllocator allocates one fresh durable Release Group ID. The
// allocator must never return an ID already present in previous.
type ReleaseGroupIDAllocator func() string

// ReconcileBlueprintReleaseGroups resolves authored service names to stable
// Service IDs, preserves Release Group IDs by canonical Blueprint name, and
// reports omitted groups for protected removal. It does not mutate storage.
func ReconcileBlueprintReleaseGroups(
	environmentID string,
	specs map[string]core.ReleaseGroupSpec,
	services []core.Service,
	previous []ReleaseGroupIdentity,
	allocate ReleaseGroupIDAllocator,
) (ReleaseGroupBlueprintReconciliation, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return ReleaseGroupBlueprintReconciliation{}, errs.New(errs.KindValidationFailed, "release group environment id is invalid")
	}

	serviceIDsByName := make(map[string]string, len(services))
	serviceNamesByID := make(map[string]string, len(services))
	for _, service := range services {
		if ids.Validate(ids.KindService, service.ID) != nil || strings.TrimSpace(service.Name) == "" {
			return ReleaseGroupBlueprintReconciliation{}, errs.New(errs.KindInternal, "release group reconciliation received an invalid service")
		}
		if _, exists := serviceIDsByName[service.Name]; exists {
			return ReleaseGroupBlueprintReconciliation{}, errs.Newf(errs.KindInternal, "service name %q is not unique in the environment", service.Name)
		}
		if existingName, exists := serviceNamesByID[service.ID]; exists {
			return ReleaseGroupBlueprintReconciliation{}, errs.Newf(errs.KindInternal, "service id %q is shared by %q and %q", service.ID, existingName, service.Name)
		}
		serviceIDsByName[service.Name] = service.ID
		serviceNamesByID[service.ID] = service.Name
	}

	previousByName := make(map[string]ReleaseGroupIdentity, len(previous))
	previousIDs := make(map[string]struct{}, len(previous))
	for _, identity := range previous {
		if ids.Validate(ids.KindReleaseGroup, identity.ID) != nil || invalidReleaseGroupName(identity.Name) {
			return ReleaseGroupBlueprintReconciliation{}, errs.New(errs.KindInternal, "release group previous identity is invalid")
		}
		if _, exists := previousByName[identity.Name]; exists {
			return ReleaseGroupBlueprintReconciliation{}, errs.Newf(errs.KindInternal, "release group name %q is not unique", identity.Name)
		}
		if _, exists := previousIDs[identity.ID]; exists {
			return ReleaseGroupBlueprintReconciliation{}, errs.Newf(errs.KindInternal, "release group id %q is not unique", identity.ID)
		}
		previousByName[identity.Name] = identity
		previousIDs[identity.ID] = struct{}{}
	}

	groupNames := make([]string, 0, len(specs))
	for name := range specs {
		groupNames = append(groupNames, name)
	}
	sort.Strings(groupNames)

	result := ReleaseGroupBlueprintReconciliation{
		Desired: make([]domain.Group, 0, len(groupNames)),
		Removed: make([]ReleaseGroupIdentity, 0, len(previous)),
	}
	seenPrevious := make(map[string]struct{}, len(groupNames))
	allocatedIDs := make(map[string]struct{}, len(previous))
	for id := range previousIDs {
		allocatedIDs[id] = struct{}{}
	}

	for _, name := range groupNames {
		spec := specs[name]
		if err := spec.Validate(name); err != nil {
			return ReleaseGroupBlueprintReconciliation{}, errs.Wrap(errs.KindValidationFailed, err)
		}

		serviceIDs := make([]string, len(spec.Services))
		serviceIDByName := make(map[string]string, len(spec.Services))
		for index, serviceName := range spec.Services {
			serviceID, exists := serviceIDsByName[serviceName]
			if !exists {
				return ReleaseGroupBlueprintReconciliation{}, errs.Newf(errs.KindServiceNotFound, "release group %q references unknown service %q", name, serviceName)
			}
			serviceIDs[index] = serviceID
			serviceIDByName[serviceName] = serviceID
		}
		orderNames := spec.Order
		if len(orderNames) == 0 {
			orderNames = spec.Services
		}
		order := make([]string, len(orderNames))
		for index, serviceName := range orderNames {
			order[index] = serviceIDByName[serviceName]
		}

		identity, exists := previousByName[name]
		if exists {
			seenPrevious[name] = struct{}{}
		} else {
			if allocate == nil {
				return ReleaseGroupBlueprintReconciliation{}, errs.Newf(errs.KindInternal, "release group %q requires an id allocator", name)
			}
			identity = ReleaseGroupIdentity{ID: allocate(), Name: name}
			if ids.Validate(ids.KindReleaseGroup, identity.ID) != nil {
				return ReleaseGroupBlueprintReconciliation{}, errs.New(errs.KindInternal, "release group allocator returned an invalid id")
			}
			if _, used := allocatedIDs[identity.ID]; used {
				return ReleaseGroupBlueprintReconciliation{}, errs.Newf(errs.KindInternal, "release group allocator reused id %q", identity.ID)
			}
			allocatedIDs[identity.ID] = struct{}{}
		}

		group, err := domain.New(domain.Input{
			ID: identity.ID, EnvironmentID: environmentID, Name: name,
			ServiceIDs: serviceIDs, Order: order, DefaultTag: spec.Tag,
			OnFailure: domain.OnFailure(spec.OnFailure.WithDefault()),
		})
		if err != nil {
			return ReleaseGroupBlueprintReconciliation{}, err
		}
		result.Desired = append(result.Desired, group)
	}

	removedNames := make([]string, 0, len(previous)-len(seenPrevious))
	for name := range previousByName {
		if _, retained := seenPrevious[name]; !retained {
			removedNames = append(removedNames, name)
		}
	}
	sort.Strings(removedNames)
	for _, name := range removedNames {
		result.Removed = append(result.Removed, previousByName[name])
	}
	return result, nil
}

func invalidReleaseGroupName(name string) bool {
	if !utf8.ValidString(name) || strings.TrimSpace(name) == "" {
		return true
	}
	return strings.IndexFunc(name, unicode.IsControl) >= 0
}
