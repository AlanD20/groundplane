package component

import (
	"context"
	"errors"
	"fmt"
	"testing"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type previewTopology struct {
	services func(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ServiceRecord], error)
	zones    func(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ZoneRecord], error)
	routes   func(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.RouteRecord], error)
}

func (topology previewTopology) ListServices(
	ctx context.Context, id string, request etcd.PageRequest,
) (etcd.Page[etcd.ServiceRecord], error) {
	return topology.services(ctx, id, request)
}

func (topology previewTopology) ListZones(
	ctx context.Context, id string, request etcd.PageRequest,
) (etcd.Page[etcd.ZoneRecord], error) {
	return topology.zones(ctx, id, request)
}

func (topology previewTopology) ListRoutes(
	ctx context.Context, id string, request etcd.PageRequest,
) (etcd.Page[etcd.RouteRecord], error) {
	return topology.routes(ctx, id, request)
}

func previewComponent() core.Component {
	return core.Component{
		ID: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAX", OwnerID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAX",
		Owner: core.ComponentOwnerEnvironment, Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{CaddyfileTemplate: "complete template"}},
	}
}

func emptyPreviewTopology() previewTopology {
	return previewTopology{
		services: func(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ServiceRecord], error) {
			return etcd.Page[etcd.ServiceRecord]{Revision: 42}, nil
		},
		zones: func(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ZoneRecord], error) {
			return etcd.Page[etcd.ZoneRecord]{Revision: 42}, nil
		},
		routes: func(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.RouteRecord], error) {
			return etcd.Page[etcd.RouteRecord]{Revision: 42}, nil
		},
	}
}

// Rationale: current preview must resolve every page and collection at the
// Component read revision even if newer desired state appears during the read.
func TestConfigProjectorPinsCompleteDesiredView(t *testing.T) {
	t.Parallel()
	component := previewComponent()
	topology := emptyPreviewTopology()
	reads, plans := 0, 0
	check := func(id string, request etcd.PageRequest) {
		t.Helper()
		reads++
		if id != component.OwnerID || request.Revision != 42 || request.Limit != etcd.MaximumPageLimit {
			t.Fatalf("preview read %q %#v", id, request)
		}
	}
	topology.services = func(_ context.Context, id string, request etcd.PageRequest) (etcd.Page[etcd.ServiceRecord], error) {
		check(id, request)
		start, end, next := 0, 200, "next"
		if request.Cursor == "next" {
			start, end, next = 200, 201, ""
		}
		page := etcd.Page[etcd.ServiceRecord]{Revision: 42, NextCursor: next}
		for index := start; index < end; index++ {
			page.Items = append(page.Items, etcd.Versioned[etcd.ServiceRecord]{ReadRevision: 42,
				Record: etcd.ServiceRecord{EnvironmentID: id, Desired: core.Service{Name: fmt.Sprint("app-", index)}}})
		}
		return page, nil
	}
	topology.zones = func(_ context.Context, id string, request etcd.PageRequest) (etcd.Page[etcd.ZoneRecord], error) {
		check(id, request)
		return etcd.Page[etcd.ZoneRecord]{Revision: 42, Items: []etcd.Versioned[etcd.ZoneRecord]{{ReadRevision: 42,
			Record: etcd.ZoneRecord{EnvironmentID: id, Desired: core.Zone{Name: "edge"}},
		}}}, nil
	}
	topology.routes = func(_ context.Context, id string, request etcd.PageRequest) (etcd.Page[etcd.RouteRecord], error) {
		check(id, request)
		return etcd.Page[etcd.RouteRecord]{Revision: 42, Items: []etcd.Versioned[etcd.RouteRecord]{{ReadRevision: 42,
			Record: etcd.RouteRecord{EnvironmentID: id, Desired: core.Route{Host: "app.example.com"}},
		}}}, nil
	}
	registration := ManagedConfigRegistration{Kind: component.Kind, SourcePath: "caddy/Caddyfile",
		Plan: func(environment core.Environment, input core.Component) (componentsdk.EnvironmentPlan, error) {
			plans++
			if environment.ID != component.OwnerID || input.ID != component.ID || len(environment.Services) != 201 ||
				len(
					environment.Zones,
				) != 1 || len(environment.Routes) != 1 || environment.Routes[0].Host != "app.example.com" {
				t.Fatalf("incomplete preview view: %#v", environment)
			}
			return componentsdk.EnvironmentPlan{Files: []componentsdk.ManagedFile{
				{Path: "other", Content: []byte("not activated")},
				{Path: "caddy/Caddyfile", Content: []byte("resolved policy")},
			}}, nil
		},
	}
	projector, err := NewConfigProjector(topology, &managedConfigProjector{}, []ManagedConfigRegistration{registration})
	if err != nil {
		t.Fatal(err)
	}
	files, err := projector.ProjectManagedConfigFiles(context.Background(), component, 42)
	if err != nil || len(files) != 1 || reads != 4 || plans != 1 {
		t.Fatalf("preview = %#v, %v; reads=%d plans=%d", files, err, reads, plans)
	}
	if files[0].Path != registration.SourcePath || files[0].Template != "complete template" ||
		files[0].Rendered != "resolved policy" {
		t.Fatalf("managed file = %#v", files[0])
	}
}

// Rationale: missing revisions, corrupt views and read failures cannot fall back
// to latest data or call the planner with a partial Environment.
func TestConfigProjectorFailsClosedBeforePlanning(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"no revision", "page revision", "item revision", "owner", "read error", "cursor cycle"} {
		t.Run(name, func(t *testing.T) {
			component := previewComponent()
			topology := emptyPreviewTopology()
			revision := int64(42)
			if name == "no revision" {
				revision = 0
			}
			topology.routes = func(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.RouteRecord], error) {
				page := etcd.Page[etcd.RouteRecord]{Revision: 42, Items: []etcd.Versioned[etcd.RouteRecord]{{
					ReadRevision: 42, Record: etcd.RouteRecord{EnvironmentID: component.OwnerID},
				}}}
				switch name {
				case "page revision":
					page.Revision = 43
				case "item revision":
					page.Items[0].ReadRevision = 43
				case "owner":
					page.Items[0].Record.EnvironmentID = "other"
				case "read error":
					return page, errs.New(errs.KindStateConflict, "compacted")
				case "cursor cycle":
					page.NextCursor = "same"
				}
				return page, nil
			}
			registration := ManagedConfigRegistration{Kind: component.Kind, SourcePath: "config",
				Plan: func(core.Environment, core.Component) (componentsdk.EnvironmentPlan, error) {
					t.Fatal("planner called after invalid read")
					return componentsdk.EnvironmentPlan{}, nil
				},
			}
			projector, err := NewConfigProjector(
				topology,
				&managedConfigProjector{},
				[]ManagedConfigRegistration{registration},
			)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := projector.ProjectManagedConfigFiles(context.Background(), component, revision); err == nil {
				t.Fatal("invalid preview succeeded")
			}
		})
	}
}

// Rationale: empty Environments still need an authoritative revision, a
// registered planner error must survive, and only one activated file is valid.
func TestConfigProjectorHandlesEmptyViewAndPlanFailures(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"empty view", "planner error", "missing file", "duplicate file"} {
		t.Run(name, func(t *testing.T) {
			registration := ManagedConfigRegistration{Kind: core.ComponentKindIngressCaddy, SourcePath: "config",
				Plan: func(environment core.Environment, _ core.Component) (componentsdk.EnvironmentPlan, error) {
					if len(environment.Services)+len(environment.Zones)+len(environment.Routes) != 0 {
						t.Fatal("expected empty desired view")
					}
					if name == "planner error" {
						return componentsdk.EnvironmentPlan{}, errs.New(errs.KindValidationFailed, "invalid references")
					}
					files := []componentsdk.ManagedFile{{Path: "config", Content: []byte("empty Route policy")}}
					if name == "missing file" {
						files = nil
					}
					if name == "duplicate file" {
						files = append(files, files[0])
					}
					return componentsdk.EnvironmentPlan{Files: files}, nil
				},
			}
			projector, err := NewConfigProjector(
				emptyPreviewTopology(), &managedConfigProjector{}, []ManagedConfigRegistration{registration},
			)
			if err != nil {
				t.Fatal(err)
			}
			files, err := projector.ProjectManagedConfigFiles(context.Background(), previewComponent(), 42)
			if name == "empty view" && (err != nil || len(files) != 1) || name != "empty view" && err == nil {
				t.Fatalf("preview = %#v, %v", files, err)
			}
			if name == "planner error" && !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("planner error kind changed: %v", err)
			}
		})
	}
}

// Rationale: disabled and unmanaged Components cause no topology reads, while
// platform preview still uses its existing durable resolver projector.
func TestConfigProjectorDelegatesPlatformAndSkipsUnmanaged(t *testing.T) {
	t.Parallel()
	platform := &managedConfigProjector{}
	projector, err := NewConfigProjector(previewTopology{}, platform, nil)
	if err != nil {
		t.Fatal(err)
	}
	component := previewComponent()
	for _, enabled := range []bool{false, true} {
		component.Enabled = enabled
		files, err := projector.ProjectManagedConfigFiles(context.Background(), component, 42)
		if err != nil || len(files) != 0 || platform.componentID != "" {
			t.Fatalf("unmanaged preview = %#v, %v", files, err)
		}
	}
	component.Owner = core.ComponentOwnerPlatform
	if _, err := projector.ProjectManagedConfigFiles(context.Background(), component, 42); err != nil ||
		platform.componentID != component.ID || platform.revision != 42 {
		t.Fatalf("platform delegation = %#v, %v", platform, err)
	}
}
