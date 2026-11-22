package monitoring

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	MaximumInventoryPage = 512
	inventoryReadBudget  = 2048
	inventoryPageTimeout = 5 * time.Second
	inventoryCursorLimit = 8 * 1024
)

type HierarchyInventory interface {
	ListProjects(
		context.Context,
		hierarchyrecord.ProjectFilter,
		etcdstore.PageRequest,
	) (etcdstore.Page[hierarchyrecord.ProjectRecord], error)
	ListEnvironments(
		context.Context,
		string,
		etcdstore.PageRequest,
	) (etcdstore.Page[hierarchyrecord.EnvironmentRecord], error)
}

type ServiceInventory interface {
	ListServices(
		context.Context,
		string,
		etcdstore.PageRequest,
	) (etcdstore.Page[servicerecord.ServiceRecord], error)
}

type InventoryTarget struct {
	Service   etcdstore.Versioned[servicerecord.ServiceRecord]
	Ownership metrics.Ownership
}

type InventoryPage struct {
	Items    []InventoryTarget
	Next     string
	Revision int64
}

type Inventory struct {
	hierarchy   HierarchyInventory
	services    ServiceInventory
	pageContext func(context.Context) (context.Context, func())
}

func NewInventory(hierarchy HierarchyInventory, services ServiceInventory) (*Inventory, error) {
	if hierarchy == nil || services == nil {
		return nil, errs.New(errs.KindInternal, "monitoring inventory readers are required")
	}
	return &Inventory{
		hierarchy: hierarchy,
		services:  services,
		pageContext: func(parent context.Context) (context.Context, func()) {
			return context.WithTimeout(parent, inventoryPageTimeout)
		},
	}, nil
}

// Page walks one fixed MVCC snapshot in stable Project, Environment and Service
// id order. It performs at most inventoryReadBudget nested reads within five
// seconds. Budget exhaustion returns the advanced continuation, even when only
// empty branches were visited, rather than materializing a global inventory.
// A compacted cursor fails with KindCursorExpired; callers restart a new cycle
// from an empty continuation and never combine revisions in one page.
func (inventory *Inventory) Page(ctx context.Context, after string, limit int) (InventoryPage, error) {
	if ctx == nil || inventory == nil || inventory.hierarchy == nil || inventory.services == nil ||
		inventory.pageContext == nil {
		return InventoryPage{}, errs.New(errs.KindInternal, "monitoring inventory is unavailable")
	}
	if limit < 1 || limit > MaximumInventoryPage {
		return InventoryPage{}, errs.New(
			errs.KindValidationFailed,
			"monitoring inventory limit must be between 1 and 512",
		)
	}
	state, err := decodeInventoryCursor(after)
	if err != nil {
		return InventoryPage{}, err
	}
	bounded, cancel := inventory.pageContext(ctx)
	defer cancel()
	result := InventoryPage{Items: make([]InventoryTarget, 0, limit), Revision: state.Revision}
	reads := 0
	for len(result.Items) < limit {
		cost := inventoryLevelReadCost(state.Level)
		if reads+cost > inventoryReadBudget {
			result.Next, err = encodeInventoryCursor(state)
			return result, err
		}
		switch state.Level {
		case inventoryLevelProject:
			project, found, page, readErr := inventory.readProject(bounded, state)
			reads++
			if readErr != nil {
				if page, deadlineErr, ok := finishInventoryDeadline(ctx, bounded, state, result, readErr); ok {
					return page, deadlineErr
				}
				return InventoryPage{}, readErr
			}
			if state.Revision == 0 {
				state.Revision, result.Revision = page.Revision, page.Revision
			}
			if !found {
				return result, nil
			}
			_ = project
			state.Level = inventoryLevelEnvironment
			state.EnvironmentCursor, state.ServiceCursor, state.ServiceLimit = "", "", 0
		case inventoryLevelEnvironment:
			project, found, projectPage, readErr := inventory.readProject(bounded, state)
			reads++
			if readErr != nil || !found {
				if readErr != nil {
					if page, deadlineErr, ok := finishInventoryDeadline(ctx, bounded, state, result, readErr); ok {
						return page, deadlineErr
					}
				}
				if readErr == nil {
					readErr = errs.New(errs.KindStateConflict, "monitoring inventory Project disappeared")
				}
				return InventoryPage{}, readErr
			}
			environment, exists, environmentPage, readErr := inventory.readEnvironment(bounded, state, project)
			reads++
			if readErr != nil {
				if page, deadlineErr, ok := finishInventoryDeadline(ctx, bounded, state, result, readErr); ok {
					return page, deadlineErr
				}
				return InventoryPage{}, readErr
			}
			if !exists {
				if projectPage.NextCursor == "" {
					return result, nil
				}
				state = inventoryCursor{Version: inventoryCursorVersion, Revision: state.Revision,
					Level: inventoryLevelProject, ProjectCursor: projectPage.NextCursor}
				continue
			}
			_ = environment
			_ = environmentPage
			state.Level, state.ServiceCursor = inventoryLevelService, ""
		case inventoryLevelService:
			project, found, projectPage, readErr := inventory.readProject(bounded, state)
			reads++
			if readErr != nil || !found {
				if readErr != nil {
					if page, deadlineErr, ok := finishInventoryDeadline(ctx, bounded, state, result, readErr); ok {
						return page, deadlineErr
					}
				}
				if readErr == nil {
					readErr = errs.New(errs.KindStateConflict, "monitoring inventory Project disappeared")
				}
				return InventoryPage{}, readErr
			}
			environment, exists, environmentPage, readErr := inventory.readEnvironment(bounded, state, project)
			reads++
			if readErr != nil || !exists {
				if readErr != nil {
					if page, deadlineErr, ok := finishInventoryDeadline(ctx, bounded, state, result, readErr); ok {
						return page, deadlineErr
					}
				}
				if readErr == nil {
					readErr = errs.New(errs.KindStateConflict, "monitoring inventory Environment disappeared")
				}
				return InventoryPage{}, readErr
			}
			remaining := limit - len(result.Items)
			serviceLimit := state.ServiceLimit
			if state.ServiceCursor == "" {
				serviceLimit = min(remaining, etcdstore.MaximumPageLimit)
			} else if remaining < serviceLimit {
				if len(result.Items) == 0 {
					return InventoryPage{}, errs.New(
						errs.KindMalformedRequest,
						"monitoring inventory limit is smaller than its continuation",
					)
				}
				result.Next, err = encodeInventoryCursor(state)
				return result, err
			}
			services, readErr := inventory.services.ListServices(bounded, environment.Record.ID, etcdstore.PageRequest{
				Limit: serviceLimit, Cursor: state.ServiceCursor, Revision: state.Revision,
			})
			reads++
			if readErr != nil {
				if page, deadlineErr, ok := finishInventoryDeadline(ctx, bounded, state, result, readErr); ok {
					return page, deadlineErr
				}
				return InventoryPage{}, readErr
			}
			if err := validateServiceInventoryPage(
				services,
				state.Revision,
				environment.Record.ID,
				serviceLimit,
			); err != nil {
				return InventoryPage{}, err
			}
			class, tenantID, err := inventoryOwnership(project.Record)
			if err != nil {
				return InventoryPage{}, err
			}
			for _, service := range services.Items {
				result.Items = append(result.Items, InventoryTarget{Service: service, Ownership: metrics.Ownership{
					TenantID: tenantID, ProjectID: project.Record.ID, EnvironmentID: environment.Record.ID,
					ServiceID: service.Record.Desired.ID, Class: class,
				}})
			}
			if services.NextCursor != "" {
				state.ServiceCursor, state.ServiceLimit = services.NextCursor, serviceLimit
				continue
			}
			if environmentPage.NextCursor != "" {
				state.Level, state.EnvironmentCursor = inventoryLevelEnvironment, environmentPage.NextCursor
				state.ServiceCursor, state.ServiceLimit = "", 0
				continue
			}
			if projectPage.NextCursor != "" {
				state = inventoryCursor{Version: inventoryCursorVersion, Revision: state.Revision,
					Level: inventoryLevelProject, ProjectCursor: projectPage.NextCursor}
				continue
			}
			return result, nil
		default:
			return InventoryPage{}, errs.New(errs.KindMalformedRequest, "monitoring inventory cursor is malformed")
		}
	}
	result.Next, err = encodeInventoryCursor(state)
	return result, err
}

func finishInventoryDeadline(
	parent context.Context,
	bounded context.Context,
	state inventoryCursor,
	result InventoryPage,
	readErr error,
) (InventoryPage, error, bool) {
	if parent.Err() != nil || !errors.Is(context.Cause(bounded), context.DeadlineExceeded) ||
		(!errors.Is(readErr, bounded.Err()) && !errors.Is(readErr, context.DeadlineExceeded)) || state.Revision <= 0 {
		return InventoryPage{}, nil, false
	}
	next, err := encodeInventoryCursor(state)
	if err != nil {
		return InventoryPage{}, err, true
	}
	result.Next = next
	return result, nil, true
}

func (inventory *Inventory) readProject(
	ctx context.Context,
	state inventoryCursor,
) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], bool, etcdstore.Page[hierarchyrecord.ProjectRecord], error) {
	page, err := inventory.hierarchy.ListProjects(ctx, hierarchyrecord.ProjectFilter{}, etcdstore.PageRequest{
		Limit: 1, Cursor: state.ProjectCursor, Revision: state.Revision,
	})
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, false, page, err
	}
	revision := state.Revision
	if revision == 0 {
		revision = page.Revision
	}
	if page.Revision <= 0 || page.Revision != revision || len(page.Items) > 1 ||
		len(page.Items) == 0 && page.NextCursor != "" {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, false, page,
			errs.New(errs.KindStateConflict, "monitoring Project inventory changed revision or shape")
	}
	if len(page.Items) == 0 {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, false, page, nil
	}
	project := page.Items[0]
	if project.ReadRevision != revision || ids.Validate(ids.KindProject, project.Record.ID) != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, false, page,
			errs.New(errs.KindStateConflict, "monitoring Project inventory ownership is invalid")
	}
	if _, _, err := inventoryOwnership(project.Record); err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, false, page, err
	}
	return project, true, page, nil
}

func (inventory *Inventory) readEnvironment(
	ctx context.Context,
	state inventoryCursor,
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], bool,
	etcdstore.Page[hierarchyrecord.EnvironmentRecord], error,
) {
	page, err := inventory.hierarchy.ListEnvironments(ctx, project.Record.ID, etcdstore.PageRequest{
		Limit: 1, Cursor: state.EnvironmentCursor, Revision: state.Revision,
	})
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, false, page, err
	}
	if page.Revision != state.Revision || len(page.Items) > 1 || len(page.Items) == 0 && page.NextCursor != "" {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, false, page,
			errs.New(errs.KindStateConflict, "monitoring Environment inventory changed revision or shape")
	}
	if len(page.Items) == 0 {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, false, page, nil
	}
	environment := page.Items[0]
	if environment.ReadRevision != state.Revision || environment.Record.ProjectID != project.Record.ID ||
		ids.Validate(ids.KindEnvironment, environment.Record.ID) != nil {
		return etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, false, page,
			errs.New(errs.KindStateConflict, "monitoring Environment inventory ownership is invalid")
	}
	return environment, true, page, nil
}

func validateServiceInventoryPage(
	page etcdstore.Page[servicerecord.ServiceRecord],
	revision int64,
	environmentID string,
	limit int,
) error {
	if page.Revision != revision || len(page.Items) > limit || len(page.Items) == 0 && page.NextCursor != "" {
		return errs.New(errs.KindStateConflict, "monitoring Service inventory changed revision or shape")
	}
	for _, service := range page.Items {
		if service.ReadRevision != revision || service.Record.EnvironmentID != environmentID ||
			ids.Validate(ids.KindService, service.Record.Desired.ID) != nil {
			return errs.New(errs.KindStateConflict, "monitoring Service inventory ownership is invalid")
		}
	}
	return nil
}

func inventoryOwnership(project hierarchyrecord.ProjectRecord) (metrics.OwnerClass, string, error) {
	switch project.Kind {
	case hierarchyrecord.ProjectKindTenant:
		if ids.Validate(ids.KindTenant, project.TenantID) != nil {
			return "", "", errs.New(errs.KindStateConflict, "monitoring workload Project owner is invalid")
		}
		return metrics.OwnerWorkload, project.TenantID, nil
	case hierarchyrecord.ProjectKindBacking:
		if project.TenantID != "" {
			return "", "", errs.New(errs.KindStateConflict, "monitoring Backing Project cannot have a Tenant")
		}
		return metrics.OwnerBacking, "", nil
	default:
		return "", "", errs.New(errs.KindStateConflict, "monitoring Project kind is invalid")
	}
}

const (
	inventoryCursorVersion    = 1
	inventoryLevelProject     = "project"
	inventoryLevelEnvironment = "environment"
	inventoryLevelService     = "service"
)

type inventoryCursor struct {
	Version           int    `json:"v"`
	Revision          int64  `json:"revision"`
	Level             string `json:"level"`
	ProjectCursor     string `json:"project_cursor,omitempty"`
	EnvironmentCursor string `json:"environment_cursor,omitempty"`
	ServiceCursor     string `json:"service_cursor,omitempty"`
	ServiceLimit      int    `json:"service_limit,omitempty"`
}

func decodeInventoryCursor(value string) (inventoryCursor, error) {
	if value == "" {
		return inventoryCursor{Version: inventoryCursorVersion, Level: inventoryLevelProject}, nil
	}
	if len(value) > inventoryCursorLimit {
		return inventoryCursor{}, errs.New(errs.KindMalformedRequest, "monitoring inventory cursor is too long")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != value ||
		recordcodec.RejectDuplicateFields(decoded) != nil {
		return inventoryCursor{}, errs.New(errs.KindMalformedRequest, "monitoring inventory cursor is malformed")
	}
	decoder := json.NewDecoder(bytes.NewReader(decoded))
	decoder.DisallowUnknownFields()
	var state inventoryCursor
	if decoder.Decode(&state) != nil || recordcodec.RequireEOF(decoder) != nil ||
		validateInventoryCursor(state) != nil {
		return inventoryCursor{}, errs.New(errs.KindMalformedRequest, "monitoring inventory cursor is malformed")
	}
	return state, nil
}

func encodeInventoryCursor(state inventoryCursor) (string, error) {
	if err := validateInventoryCursor(state); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	result := base64.RawURLEncoding.EncodeToString(encoded)
	if len(result) > inventoryCursorLimit {
		return "", errs.New(errs.KindInternal, "monitoring inventory cursor exceeds its bound")
	}
	return result, nil
}

func validateInventoryCursor(state inventoryCursor) error {
	if state.Version != inventoryCursorVersion || state.Revision <= 0 ||
		len(state.ProjectCursor) > 2048 || len(state.EnvironmentCursor) > 2048 || len(state.ServiceCursor) > 2048 ||
		state.ServiceLimit < 0 || state.ServiceLimit > etcdstore.MaximumPageLimit {
		return errs.New(errs.KindMalformedRequest, "monitoring inventory cursor is malformed")
	}
	switch state.Level {
	case inventoryLevelProject:
		if state.EnvironmentCursor != "" || state.ServiceCursor != "" || state.ServiceLimit != 0 {
			return errs.New(errs.KindMalformedRequest, "monitoring inventory cursor is malformed")
		}
	case inventoryLevelEnvironment:
		if state.ServiceCursor != "" || state.ServiceLimit != 0 {
			return errs.New(errs.KindMalformedRequest, "monitoring inventory cursor is malformed")
		}
	case inventoryLevelService:
		if state.ServiceCursor == "" && state.ServiceLimit != 0 ||
			state.ServiceCursor != "" && state.ServiceLimit == 0 {
			return errs.New(errs.KindMalformedRequest, "monitoring inventory cursor is malformed")
		}
	default:
		return errs.New(errs.KindMalformedRequest, "monitoring inventory cursor is malformed")
	}
	return nil
}

func inventoryLevelReadCost(level string) int {
	switch level {
	case inventoryLevelProject:
		return 1
	case inventoryLevelEnvironment:
		return 2
	case inventoryLevelService:
		return 3
	default:
		return inventoryReadBudget + 1
	}
}
