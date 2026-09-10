package component

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

type managedConfigReadRepository struct{ record etcd.ComponentRecord }

func (repository managedConfigReadRepository) GetComponent(
	context.Context,
	string,
) (etcd.Versioned[etcd.ComponentRecord], error) {
	return etcd.Versioned[etcd.ComponentRecord]{Record: repository.record, ReadRevision: 42}, nil
}

func (managedConfigReadRepository) ListEnvironmentComponents(
	context.Context,
	string,
	etcd.PageRequest,
) (etcd.Page[etcd.ComponentRecord], error) {
	return etcd.Page[etcd.ComponentRecord]{}, nil
}

func (managedConfigReadRepository) ListPlatformComponents(
	context.Context,
	etcd.PageRequest,
) (etcd.Page[etcd.ComponentRecord], error) {
	return etcd.Page[etcd.ComponentRecord]{}, nil
}

type managedConfigProjector struct {
	componentID string
	revision    int64
	files       []apiTypes.ManagedConfigFile
}

func (projector *managedConfigProjector) ProjectManagedConfigFiles(
	_ context.Context,
	component core.Component,
	revision int64,
) ([]apiTypes.ManagedConfigFile, error) {
	projector.componentID = component.ID
	projector.revision = revision
	return projector.files, nil
}

// Rationale: disabled Components retain dormant desired state internally but
// must expose a null active config until they are enabled.
func TestProjectComponentConfigHidesDisabledConfiguration(t *testing.T) {
	t.Parallel()
	component := core.Component{
		Enabled: false,
		Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
			ZoneIDs: []string{"net_01ARZ3NDEKTSV4RRFFQ69G5FAX"},
		}},
	}
	if config := projectComponentConfig(component); config != nil {
		t.Fatalf("projectComponentConfig(disabled) = %#v, want nil", config)
	}
	component.Enabled = true
	if config := projectComponentConfig(component); config == nil || config.Caddy == nil {
		t.Fatalf("projectComponentConfig(enabled) = %#v, want Caddy config", config)
	}
}

// Rationale: the generic Component read service must delegate managed-file
// derivation through its consumer-owned port without knowing the Component
// kind, and it must preserve the projector's typed result.
func TestGetComponentConfigDelegatesManagedFileProjection(t *testing.T) {
	t.Parallel()
	component := core.Component{
		ID: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAX", Owner: core.ComponentOwnerPlatform,
		Kind: core.ComponentKindCoreDNS, Enabled: true,
		Config: core.ComponentConfig{CoreDNS: &core.CoreDNSComponentConfig{
			CorefileTemplate: ". {\n    {groundplane}\n}\n", UpstreamAuto: true,
			UpstreamResolvers: []core.DNSResolverEndpoint{}, Forwarders: []core.DNSForwarder{},
		}},
	}
	record, err := etcd.NewComponentRecord(component)
	if err != nil {
		t.Fatalf("NewComponentRecord() error = %v", err)
	}
	projector := &managedConfigProjector{files: []apiTypes.ManagedConfigFile{{
		Path: "/etc/groundplane/coredns/Corefile", Template: component.Config.CoreDNS.CorefileTemplate,
		Rendered: ". {\n    bind 127.0.0.1\n}\n",
	}}}
	service, err := NewReadService(managedConfigReadRepository{record: record}, projector)
	if err != nil {
		t.Fatalf("NewReadService() error = %v", err)
	}
	response, err := service.GetComponentConfig(context.Background(), component.ID)
	if err != nil {
		t.Fatalf("GetComponentConfig() error = %v", err)
	}
	if projector.componentID != component.ID || projector.revision != 42 || len(response.ManagedFiles) != 1 ||
		response.ManagedFiles[0].Rendered != projector.files[0].Rendered {
		t.Fatalf("GetComponentConfig() = %#v, projector component = %q", response, projector.componentID)
	}
}
