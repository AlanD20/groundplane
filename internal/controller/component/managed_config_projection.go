package component

import (
	"context"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"path"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type managedConfigTopology interface {
	ListServices(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ServiceRecord], error)
	ListZones(context.Context, string, etcd.PageRequest) (etcd.Page[zonerecord.Record], error)
	ListRoutes(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.RouteRecord], error)
}

// ManagedConfigRegistration projects a compiled plan's one activated file.
// Plan and SourcePath are the same authorities used by mutation planning.
type ManagedConfigRegistration struct {
	Kind       core.ComponentKind
	SourcePath string
	Plan       func(core.Environment, core.Component) (componentsdk.EnvironmentPlan, error)
}

type ConfigProjector struct {
	topology managedConfigTopology
	platform ManagedConfigProjector
	catalog  []ManagedConfigRegistration
}

func NewConfigProjector(
	topology managedConfigTopology,
	platform ManagedConfigProjector,
	catalog []ManagedConfigRegistration,
) (*ConfigProjector, error) {
	if topology == nil || platform == nil {
		return nil, errs.New(errs.KindInternal, "managed-config preview dependencies are required")
	}
	seen := make(map[core.ComponentKind]bool, len(catalog))
	for _, registration := range catalog {
		if registration.Kind == "" || registration.Plan == nil || seen[registration.Kind] ||
			registration.SourcePath == "" || path.IsAbs(registration.SourcePath) ||
			path.Clean(registration.SourcePath) != registration.SourcePath {
			return nil, errs.New(errs.KindInternal, "managed-config preview registration is invalid")
		}
		seen[registration.Kind] = true
	}
	return &ConfigProjector{
		topology: topology, platform: platform, catalog: append([]ManagedConfigRegistration(nil), catalog...),
	}, nil
}

func (projector *ConfigProjector) ProjectManagedConfigFiles(
	ctx context.Context,
	component core.Component,
	revision int64,
) ([]apiTypes.ManagedConfigFile, error) {
	if !component.Enabled {
		return []apiTypes.ManagedConfigFile{}, nil
	}
	if component.Owner == core.ComponentOwnerPlatform {
		return projector.platform.ProjectManagedConfigFiles(ctx, component, revision)
	}
	for _, registration := range projector.catalog {
		if registration.Kind != component.Kind {
			continue
		}
		if revision <= 0 || component.Owner != core.ComponentOwnerEnvironment || component.Config.Caddy == nil {
			return nil, errs.New(errs.KindInternal, "managed-config preview authority is invalid")
		}
		environment, err := projector.environment(ctx, component.OwnerID, revision)
		if err != nil {
			return nil, err
		}
		plan, err := registration.Plan(environment, component)
		if err != nil {
			return nil, err
		}
		files := []apiTypes.ManagedConfigFile{}
		for _, file := range plan.Files {
			if file.Path == registration.SourcePath {
				files = append(files, apiTypes.ManagedConfigFile{
					Path: file.Path, Template: component.Config.Caddy.CaddyfileTemplate, Rendered: string(file.Content),
				})
			}
		}
		if len(files) != 1 {
			return nil, errs.New(errs.KindInternal, "managed-config preview requires one activated file")
		}
		return files, nil
	}
	return []apiTypes.ManagedConfigFile{}, nil
}

func (projector *ConfigProjector) environment(
	ctx context.Context,
	id string,
	revision int64,
) (core.Environment, error) {
	environment := core.Environment{ID: id, Services: map[string]core.Service{}, Zones: map[string]core.Zone{}}
	services, err := readPreviewCollection(ctx, id, revision, projector.topology.ListServices)
	if err != nil {
		return core.Environment{}, err
	}
	for _, record := range services {
		if _, duplicate := environment.Services[record.Desired.Name]; duplicate || record.EnvironmentID != id {
			return core.Environment{}, errs.New(errs.KindInternal, "managed-config preview Service scope is invalid")
		}
		environment.Services[record.Desired.Name] = record.Desired
	}
	zones, err := readPreviewCollection(ctx, id, revision, projector.topology.ListZones)
	if err != nil {
		return core.Environment{}, err
	}
	for _, record := range zones {
		if _, duplicate := environment.Zones[record.Desired.Name]; duplicate || record.EnvironmentID != id {
			return core.Environment{}, errs.New(errs.KindInternal, "managed-config preview Zone scope is invalid")
		}
		environment.Zones[record.Desired.Name] = record.Desired
	}
	routes, err := readPreviewCollection(ctx, id, revision, projector.topology.ListRoutes)
	if err != nil {
		return core.Environment{}, err
	}
	for _, record := range routes {
		if record.EnvironmentID != id {
			return core.Environment{}, errs.New(errs.KindInternal, "managed-config preview Route scope is invalid")
		}
		environment.Routes = append(environment.Routes, record.Desired)
	}
	return environment, nil
}

func readPreviewCollection[T any](
	ctx context.Context,
	id string,
	revision int64,
	list func(context.Context, string, etcd.PageRequest) (etcd.Page[T], error),
) ([]T, error) {
	var result []T
	request := etcd.PageRequest{Limit: etcd.MaximumPageLimit, Revision: revision}
	seen := map[string]bool{"": true}
	for {
		page, err := list(ctx, id, request)
		if err != nil {
			return nil, err
		}
		if page.Revision != revision || len(page.Items) > request.Limit {
			return nil, errs.New(errs.KindInternal, "managed-config preview page is inconsistent")
		}
		for _, item := range page.Items {
			if item.ReadRevision != revision {
				return nil, errs.New(errs.KindInternal, "managed-config preview item revision is inconsistent")
			}
			result = append(result, item.Record)
		}
		if page.NextCursor == "" {
			return result, nil
		}
		if seen[page.NextCursor] || len(page.Items) == 0 {
			return nil, errs.New(errs.KindInternal, "managed-config preview pagination did not advance")
		}
		seen[page.NextCursor] = true
		request.Cursor = page.NextCursor
	}
}
