package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// OpenProxyImplementation decodes captured machine authority. Missing metadata
// is not a default: no executor may select an implementation after publication.
func OpenProxyImplementation(value agentpb.ServiceProxyImplementation) (serviceproxy.Implementation, error) {
	switch value {
	case agentpb.ServiceProxyImplementation_SERVICE_PROXY_IMPLEMENTATION_CADDY:
		return serviceproxy.Caddy, nil
	case agentpb.ServiceProxyImplementation_SERVICE_PROXY_IMPLEMENTATION_ENVOY:
		return serviceproxy.Envoy, nil
	default:
		return "", errs.New(errs.KindValidationFailed, "Service proxy implementation is missing or unsupported")
	}
}

func SealProxyImplementation(value serviceproxy.Implementation) (agentpb.ServiceProxyImplementation, error) {
	switch value {
	case serviceproxy.Caddy:
		return agentpb.ServiceProxyImplementation_SERVICE_PROXY_IMPLEMENTATION_CADDY, nil
	case serviceproxy.Envoy:
		return agentpb.ServiceProxyImplementation_SERVICE_PROXY_IMPLEMENTATION_ENVOY, nil
	default:
		return agentpb.ServiceProxyImplementation_SERVICE_PROXY_IMPLEMENTATION_UNSPECIFIED,
			errs.New(errs.KindValidationFailed, "Service proxy implementation must be captured before publication")
	}
}

func OpenProxyConfiguration(proxy *agentpb.ComposeService) (serviceproxy.Configuration, error) {
	if proxy == nil || proxy.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
		return serviceproxy.Configuration{}, errs.New(
			errs.KindValidationFailed,
			"captured stable Service proxy is missing",
		)
	}
	implementation, err := OpenProxyImplementation(proxy.GetProxyImplementation())
	if err != nil {
		return serviceproxy.Configuration{}, err
	}
	return serviceproxy.Read(implementation, proxy.GetProxyConfigJson())
}
