package serviceread

import (
	"context"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type Environments interface {
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error)
}

type Services interface {
	GetService(context.Context, string) (etcdstore.Versioned[servicerecord.ServiceRecord], error)
	ListServices(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[servicerecord.ServiceRecord], error)
}

type Reader struct {
	environments Environments
	services     Services
}

func (service *Reader) GetService(
	ctx context.Context,
	serviceID string,
) (etcdstore.Versioned[servicerecord.ServiceRecord], error) {
	if ctx == nil {
		return etcdstore.Versioned[servicerecord.ServiceRecord]{}, errs.New(
			errs.KindInternal,
			"Service read context is required",
		)
	}
	if ids.Validate(ids.KindService, serviceID) != nil {
		return etcdstore.Versioned[servicerecord.ServiceRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Service read requires a stable Service id",
		)
	}
	return service.services.GetService(ctx, serviceID)
}

func New(environments Environments, services Services) (*Reader, error) {
	if environments == nil || services == nil {
		return nil, errs.New(errs.KindInternal, "Service read repository is not configured")
	}
	return &Reader{environments: environments, services: services}, nil
}

func (service *Reader) ListServices(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[servicerecord.ServiceRecord], error) {
	if ctx == nil {
		return etcdstore.Page[servicerecord.ServiceRecord]{}, errs.New(
			errs.KindInternal,
			"Service list context is required",
		)
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcdstore.Page[servicerecord.ServiceRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Service list requires a stable Environment id",
		)
	}
	if request.Limit < 0 {
		return etcdstore.Page[servicerecord.ServiceRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Service list limit must be a positive integer",
		)
	}
	if _, err := service.environments.GetEnvironment(ctx, environmentID); err != nil {
		return etcdstore.Page[servicerecord.ServiceRecord]{}, err
	}
	return service.services.ListServices(ctx, environmentID, request)
}

func (service *Reader) GetServiceNativeCompose(
	ctx context.Context,
	environmentID string,
	serviceName string,
) (string, error) {
	if ctx == nil {
		return "", errs.New(errs.KindInternal, "Service detail context is required")
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || strings.TrimSpace(serviceName) == "" {
		return "", errs.New(errs.KindValidationFailed, "Service detail identity is invalid")
	}
	environment, err := service.environments.GetEnvironment(ctx, environmentID)
	if err != nil {
		return "", err
	}
	projection, found, err := service.environments.GetEnvironmentComposeProjection(ctx, environmentID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", errs.New(errs.KindInternal, "Service desired Compose projection is missing")
	}
	if projection.Record.EnvironmentID != environment.Record.ID {
		return "", errs.New(errs.KindInternal, "Service desired Compose projection owner is invalid")
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(
		projection.Record.ComposeArtifact,
		artifact,
	); err != nil {
		return "", errs.New(errs.KindInternal, "Service desired Compose artifact is corrupt")
	}
	return composerender.ProjectEnvironmentServiceNativeCompose(artifact, serviceName)
}
