package component

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type readRepository interface {
	GetComponent(context.Context, string) (etcd.Versioned[etcd.ComponentRecord], error)
	ListEnvironmentComponents(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ComponentRecord], error)
	ListPlatformComponents(context.Context, etcd.PageRequest) (etcd.Page[etcd.ComponentRecord], error)
}

type ReadService struct{ repository readRepository }

func NewReadService(repository readRepository) (*ReadService, error) {
	if repository == nil {
		return nil, errs.New(errs.KindInternal, "Component read repository is required")
	}
	return &ReadService{repository: repository}, nil
}

func (service *ReadService) ListComponents(
	ctx context.Context,
	environmentID string,
	platform bool,
	kind string,
) ([]apiTypes.Component, error) {
	var records etcd.Page[etcd.ComponentRecord]
	var err error
	if platform {
		records, err = service.repository.ListPlatformComponents(ctx, etcd.PageRequest{Limit: etcd.MaximumPageLimit})
	} else {
		if ids.Validate(ids.KindEnvironment, environmentID) != nil {
			return nil, errs.New(errs.KindValidationFailed, "Component Environment id is invalid")
		}
		records, err = service.repository.ListEnvironmentComponents(
			ctx,
			environmentID,
			etcd.PageRequest{Limit: etcd.MaximumPageLimit},
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

func (service *ReadService) GetComponentConfig(ctx context.Context, id string) (apiTypes.ComponentConfig, error) {
	component, err := service.GetComponent(ctx, id)
	if err != nil {
		return apiTypes.ComponentConfig{}, err
	}
	return apiTypes.ComponentConfig{Config: cloneConfig(component.Config)}, nil
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

func projectComponent(record etcd.ComponentRecord) (apiTypes.Component, error) {
	component, err := etcd.ProjectComponentRecord(record)
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
		Config: cloneConfig(component.Config), GeneratedServices: append([]string(nil), component.GeneratedServices...),
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

func cloneConfig(source map[string]any) map[string]any {
	if len(source) == 0 {
		return nil
	}
	clone := make(map[string]any, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}
