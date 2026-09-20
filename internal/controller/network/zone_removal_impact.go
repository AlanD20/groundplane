package network

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type zoneRemovalImpactZones interface {
	GetZone(context.Context, string) (etcd.Versioned[zonerecord.Record], error)
}

type zoneRemovalImpactAttaches interface {
	ListAttachesByBackingNetworkAtRevision(
		context.Context,
		string,
		string,
		int64,
	) ([]etcd.Versioned[etcd.AttachRecord], error)
}

type zoneRemovalImpactServices interface {
	GetService(context.Context, string) (etcd.Versioned[etcd.ServiceRecord], error)
	ListServices(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ServiceRecord], error)
}

type zoneRemovalImpactFacts interface {
	ResolveRemovalDatabase(context.Context, etcd.Versioned[etcd.AttachRecord], func(string) error) error
}

type zoneRemovalImpactService struct {
	zones    zoneRemovalImpactZones
	attaches zoneRemovalImpactAttaches
	services zoneRemovalImpactServices
	facts    zoneRemovalImpactFacts
}

func newZoneRemovalImpactService(
	zones zoneRemovalImpactZones,
	attaches zoneRemovalImpactAttaches,
	services zoneRemovalImpactServices,
	facts zoneRemovalImpactFacts,
) (*zoneRemovalImpactService, error) {
	if zones == nil || attaches == nil || services == nil || facts == nil {
		return nil, errs.New(errs.KindInternal, "Zone removal impact service is not configured")
	}
	return &zoneRemovalImpactService{zones: zones, attaches: attaches, services: services, facts: facts}, nil
}

func (service *zoneRemovalImpactService) GetZoneRemovalImpact(
	ctx context.Context,
	zoneID string,
) (apiTypes.ZoneRemovalImpact, error) {
	if ctx == nil {
		return apiTypes.ZoneRemovalImpact{}, errs.New(errs.KindInternal, "Zone removal impact context is required")
	}
	if ids.Validate(ids.KindNetwork, zoneID) != nil {
		return apiTypes.ZoneRemovalImpact{}, errs.New(errs.KindValidationFailed, "Zone id is invalid")
	}
	zone, err := service.zones.GetZone(ctx, zoneID)
	if err != nil {
		return apiTypes.ZoneRemovalImpact{}, err
	}
	impact := apiTypes.ZoneRemovalImpact{
		ZoneID: zone.Record.Desired.ID, ZoneName: zone.Record.Desired.Name,
		Mode: apiTypes.ZoneRemovalImpactOrdinary, Attaches: []apiTypes.ZoneRemovalImpactAttach{},
		Services: []apiTypes.ZoneRemovalImpactService{}, Databases: []apiTypes.ZoneRemovalImpactDatabase{},
	}
	canonical := zoneRemovalImpactCanonical{ZoneID: zoneID, ZoneRevision: zone.Revision}
	if zone.Record.Desired.OwnerKind == core.ZoneOwnerBackingProject {
		impact.Mode = apiTypes.ZoneRemovalImpactCascade
		if err := service.populateBackingImpact(ctx, zone, &impact, &canonical); err != nil {
			return apiTypes.ZoneRemovalImpact{}, err
		}
	} else if err := service.populateOrdinaryImpact(ctx, zone, &impact, &canonical); err != nil {
		return apiTypes.ZoneRemovalImpact{}, err
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return apiTypes.ZoneRemovalImpact{}, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(encoded)
	clear(encoded)
	impact.ImpactToken = hex.EncodeToString(digest[:])
	return impact, nil
}

func (service *zoneRemovalImpactService) populateBackingImpact(
	ctx context.Context,
	zone etcd.Versioned[zonerecord.Record],
	impact *apiTypes.ZoneRemovalImpact,
	canonical *zoneRemovalImpactCanonical,
) error {
	attaches, err := service.attaches.ListAttachesByBackingNetworkAtRevision(
		ctx, zone.Record.Desired.OwnerID, zone.Record.Desired.ID, zone.ReadRevision,
	)
	if err != nil {
		return err
	}
	serviceByID := make(map[string]apiTypes.ZoneRemovalImpactService, len(attaches))
	serviceRevision := make(map[string]int64, len(attaches))
	for _, attach := range attaches {
		consumer, err := service.services.GetService(ctx, attach.Record.ServiceID)
		if err != nil {
			return err
		}
		database := ""
		if err := service.facts.ResolveRemovalDatabase(ctx, attach, func(value string) error {
			database = value
			return nil
		}); err != nil {
			return err
		}
		impact.Attaches = append(impact.Attaches, apiTypes.ZoneRemovalImpactAttach{
			ID: attach.Record.ID, Name: attach.Record.Name, EnvironmentID: attach.Record.EnvironmentID,
			ServiceID: consumer.Record.Desired.ID, Database: database, Status: string(attach.Record.Status),
		})
		serviceByID[consumer.Record.Desired.ID] = apiTypes.ZoneRemovalImpactService{
			ID: consumer.Record.Desired.ID, Name: consumer.Record.Desired.Name,
			EnvironmentID: consumer.Record.EnvironmentID,
		}
		serviceRevision[consumer.Record.Desired.ID] = consumer.Revision
		canonical.Attaches = append(canonical.Attaches, zoneRemovalImpactAttachCanonical{
			ID: attach.Record.ID, Revision: attach.Revision, ServiceID: consumer.Record.Desired.ID,
			Database: database, Status: string(attach.Record.Status),
		})
		if database != "" {
			impact.Databases = append(impact.Databases, apiTypes.ZoneRemovalImpactDatabase{
				AttachID: attach.Record.ID, Name: database,
			})
		}
	}
	for _, item := range serviceByID {
		impact.Services = append(impact.Services, item)
		canonical.Services = append(canonical.Services, zoneRemovalImpactServiceCanonical{
			ID: item.ID, Revision: serviceRevision[item.ID], Name: item.Name,
		})
	}
	sortZoneRemovalImpact(impact, canonical)
	return nil
}

func (service *zoneRemovalImpactService) populateOrdinaryImpact(
	ctx context.Context,
	zone etcd.Versioned[zonerecord.Record],
	impact *apiTypes.ZoneRemovalImpact,
	canonical *zoneRemovalImpactCanonical,
) error {
	cursor := ""
	for {
		page, err := service.services.ListServices(ctx, zone.Record.EnvironmentID, etcd.PageRequest{
			Limit: 200, Cursor: cursor,
		})
		if err != nil {
			return err
		}
		for _, current := range page.Items {
			if !slices.Contains(current.Record.Desired.Zones, zone.Record.Desired.Name) {
				continue
			}
			item := apiTypes.ZoneRemovalImpactService{
				ID: current.Record.Desired.ID, Name: current.Record.Desired.Name,
				EnvironmentID: current.Record.EnvironmentID,
			}
			impact.Services = append(impact.Services, item)
			canonical.Services = append(canonical.Services, zoneRemovalImpactServiceCanonical{
				ID: item.ID, Revision: current.Revision, Name: item.Name,
			})
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	sortZoneRemovalImpact(impact, canonical)
	return nil
}

type zoneRemovalImpactCanonical struct {
	ZoneID       string                              `json:"zone_id"`
	ZoneRevision int64                               `json:"zone_revision"`
	Attaches     []zoneRemovalImpactAttachCanonical  `json:"attaches"`
	Services     []zoneRemovalImpactServiceCanonical `json:"services"`
}

type zoneRemovalImpactAttachCanonical struct {
	ID        string `json:"id"`
	Revision  int64  `json:"revision"`
	ServiceID string `json:"service_id"`
	Database  string `json:"database"`
	Status    string `json:"status"`
}

type zoneRemovalImpactServiceCanonical struct {
	ID       string `json:"id"`
	Revision int64  `json:"revision"`
	Name     string `json:"name"`
}

func sortZoneRemovalImpact(impact *apiTypes.ZoneRemovalImpact, canonical *zoneRemovalImpactCanonical) {
	slices.SortFunc(impact.Attaches, func(left, right apiTypes.ZoneRemovalImpactAttach) int {
		return strings.Compare(left.ID, right.ID)
	})
	slices.SortFunc(impact.Services, func(left, right apiTypes.ZoneRemovalImpactService) int {
		return strings.Compare(left.ID, right.ID)
	})
	slices.SortFunc(impact.Databases, func(left, right apiTypes.ZoneRemovalImpactDatabase) int {
		return strings.Compare(left.AttachID, right.AttachID)
	})
	slices.SortFunc(canonical.Attaches, func(left, right zoneRemovalImpactAttachCanonical) int {
		return strings.Compare(left.ID, right.ID)
	})
	slices.SortFunc(canonical.Services, func(left, right zoneRemovalImpactServiceCanonical) int {
		return strings.Compare(left.ID, right.ID)
	})
}

func (service *zoneCreationService) GetZoneRemovalImpact(
	ctx context.Context,
	zoneID string,
) (apiTypes.ZoneRemovalImpact, error) {
	if service == nil || service.impacts == nil {
		return apiTypes.ZoneRemovalImpact{}, errs.New(
			errs.KindInternal,
			"Zone removal impact service is not configured",
		)
	}
	return service.impacts.GetZoneRemovalImpact(ctx, zoneID)
}
