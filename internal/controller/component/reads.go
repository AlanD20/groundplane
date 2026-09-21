package component

import (
	"context"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"net/netip"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type readRepository interface {
	GetComponent(context.Context, string) (etcdstore.Versioned[componentrecord.Record], error)
	ListEnvironmentComponents(
		context.Context,
		string,
		etcdstore.PageRequest,
	) (etcdstore.Page[componentrecord.Record], error)
	ListPlatformComponents(context.Context, etcdstore.PageRequest) (etcdstore.Page[componentrecord.Record], error)
}

// ManagedConfigProjector is the consumer-owned port for deriving generic
// managed-file previews without teaching Component reads about implementations.
type ManagedConfigProjector interface {
	ProjectManagedConfigFiles(context.Context, core.Component, int64) ([]apiTypes.ManagedConfigFile, error)
}

type ReadService struct {
	repository             readRepository
	managedConfigProjector ManagedConfigProjector
}

func NewReadService(repository readRepository, managedConfigProjector ManagedConfigProjector) (*ReadService, error) {
	if repository == nil || managedConfigProjector == nil {
		return nil, errs.New(errs.KindInternal, "Component read dependencies are required")
	}
	return &ReadService{repository: repository, managedConfigProjector: managedConfigProjector}, nil
}

func (service *ReadService) ListComponents(
	ctx context.Context,
	environmentID string,
	platform bool,
	kind string,
) ([]apiTypes.Component, error) {
	var records etcdstore.Page[componentrecord.Record]
	var err error
	if platform {
		records, err = service.repository.ListPlatformComponents(
			ctx,
			etcdstore.PageRequest{Limit: etcdstore.MaximumPageLimit},
		)
	} else {
		if ids.Validate(ids.KindEnvironment, environmentID) != nil {
			return nil, errs.New(errs.KindValidationFailed, "Component Environment id is invalid")
		}
		records, err = service.repository.ListEnvironmentComponents(
			ctx,
			environmentID,
			etcdstore.PageRequest{Limit: etcdstore.MaximumPageLimit},
		)
	}
	if err != nil {
		return nil, err
	}
	items := make([]apiTypes.Component, 0, len(records.Items))
	for _, record := range records.Items {
		component, projectErr := projectComponent(record.Record)
		if projectErr != nil {
			return nil, projectErr
		}
		if kind == "" || component.Kind == kind {
			items = append(items, component)
		}
	}
	return items, nil
}

func (service *ReadService) GetComponent(ctx context.Context, id string) (apiTypes.Component, error) {
	record, err := service.repository.GetComponent(ctx, id)
	if err != nil {
		return apiTypes.Component{}, err
	}
	return projectComponent(record.Record)
}

func (service *ReadService) GetComponentConfig(
	ctx context.Context,
	id string,
) (apiTypes.ComponentConfigResponse, error) {
	record, err := service.repository.GetComponent(ctx, id)
	if err != nil {
		return apiTypes.ComponentConfigResponse{}, err
	}
	component, err := componentrecord.ProjectRecord(record.Record)
	if err != nil {
		return apiTypes.ComponentConfigResponse{}, err
	}
	managedFiles, err := service.managedConfigProjector.ProjectManagedConfigFiles(ctx, component, record.ReadRevision)
	if err != nil {
		return apiTypes.ComponentConfigResponse{}, err
	}
	if managedFiles == nil {
		managedFiles = []apiTypes.ManagedConfigFile{}
	}
	return apiTypes.ComponentConfigResponse{
		Config: projectComponentConfig(component), ManagedFiles: managedFiles,
	}, nil
}

func (service *ReadService) GetRouter(ctx context.Context, environmentID string) (apiTypes.Router, error) {
	components, err := service.ListComponents(ctx, environmentID, false, "")
	if err != nil {
		return apiTypes.Router{}, err
	}
	var router apiTypes.Router
	for _, component := range components {
		projection := &apiTypes.ComponentProjection{
			ComponentID: component.ID,
			Enabled:     component.Enabled,
			PinnedIPv4:  component.PinnedIPv4,
		}
		switch core.ComponentKind(component.Kind) {
		case core.ComponentKindIngressCaddy:
			router.Caddy = projection
		case core.ComponentKindEdgeCloudflare:
			router.Tunnel = projection
		}
	}
	return router, nil
}

func projectComponent(record componentrecord.Record) (apiTypes.Component, error) {
	component, err := componentrecord.ProjectRecord(record)
	if err != nil {
		return apiTypes.Component{}, err
	}
	environmentID := ""
	if component.Owner == core.ComponentOwnerEnvironment {
		environmentID = component.OwnerID
	}
	return apiTypes.Component{
		ID: component.ID, Owner: string(component.Owner), OwnerID: component.OwnerID,
		EnvironmentID: environmentID, Kind: string(component.Kind), Enabled: component.Enabled,
		Config: projectComponentConfig(
			component,
		), GeneratedServices: append([]string(nil), component.GeneratedServices...),
		PinnedIPv4: component.PinnedIPv4, Healthy: component.Healthy, Status: componentStatus(component),
	}, nil
}

func componentStatus(component core.Component) string {
	if !component.Enabled {
		return "disabled"
	}
	if component.Healthy {
		return "healthy"
	}
	return "unknown"
}

func projectComponentConfig(component core.Component) *apiTypes.ComponentConfig {
	if !component.Enabled {
		return nil
	}
	config := component.Config
	if config.Caddy != nil {
		result := &apiTypes.ComponentConfig{
			Caddy: &apiTypes.CaddyComponentConfig{
				ZoneIDs: append(
					[]string(nil),
					config.Caddy.ZoneIDs...), CaddyfileTemplate: config.Caddy.CaddyfileTemplate, Alias: config.Caddy.Alias,
			},
		}
		return result
	}
	if config.CloudflareTunnel != nil {
		return &apiTypes.ComponentConfig{CloudflareTunnel: &apiTypes.CloudflareTunnelComponentConfig{
			ZoneIDs:  append([]string(nil), config.CloudflareTunnel.ZoneIDs...),
			SecretID: config.CloudflareTunnel.SecretID,
		}}
	}
	if config.CoreDNS == nil {
		return nil
	}
	upstreamAuto := config.CoreDNS.UpstreamAuto
	tailnetDelegation := config.CoreDNS.TailnetDelegation
	result := &apiTypes.ComponentConfig{
		CoreDNS: &apiTypes.CoreDNSComponentConfig{
			CorefileTemplate:  config.CoreDNS.CorefileTemplate,
			UpstreamAuto:      upstreamAuto,
			UpstreamResolvers: projectResolverEndpoints(config.CoreDNS.UpstreamResolvers),
			Forwarders:        make([]apiTypes.ComponentDNSForwarder, len(config.CoreDNS.Forwarders)),
			TailnetDelegation: tailnetDelegation,
		},
	}
	for index, forwarder := range config.CoreDNS.Forwarders {
		result.CoreDNS.Forwarders[index] = apiTypes.ComponentDNSForwarder{
			Domain:    forwarder.Domain,
			Resolvers: projectResolverEndpoints(forwarder.Resolvers),
		}
	}
	return result
}

func projectResolverEndpoints(values []core.DNSResolverEndpoint) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = value.Address
		if value.Port != 0 {
			address, err := netip.ParseAddr(value.Address)
			if err == nil {
				result[index] = netip.AddrPortFrom(address, value.Port).String()
			}
		}
	}
	return result
}
