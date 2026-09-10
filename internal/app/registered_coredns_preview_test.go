package app

import (
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type registeredCoreDNSPreviewRecords struct {
	projection etcd.HostResolutionProjectionRecord
}

func (records registeredCoreDNSPreviewRecords) GetHostResolutionProjection(
	context.Context,
) (etcd.Versioned[etcd.HostResolutionProjectionRecord], bool, error) {
	return etcd.Versioned[etcd.HostResolutionProjectionRecord]{Record: records.projection}, true, nil
}

func (registeredCoreDNSPreviewRecords) GetHostResolverBaseline(
	context.Context,
) (etcd.Versioned[etcd.HostResolverBaselineRecord], bool, error) {
	return etcd.Versioned[etcd.HostResolverBaselineRecord]{Record: etcd.HostResolverBaselineRecord{
		Generation: 1, Content: []byte("nameserver 1.1.1.1\n"),
	}}, true, nil
}

// Rationale: production composition must derive the public preview with the
// registered CoreDNS renderer and publish the exact Agent-managed target path.
func TestRegisteredCoreDNSManagedConfigProjectorUsesCompiledRenderer(t *testing.T) {
	t.Parallel()
	projection, err := etcd.NewHostResolutionProjectionRecord(1, nil)
	if err != nil {
		t.Fatalf("NewHostResolutionProjectionRecord() error = %v", err)
	}
	records := registeredCoreDNSPreviewRecords{projection: projection}
	projector, err := newRegisteredCoreDNSManagedConfigProjector(records, records)
	if err != nil {
		t.Fatalf("newRegisteredCoreDNSManagedConfigProjector() error = %v", err)
	}
	files, err := projector.ProjectManagedConfigFiles(context.Background(), core.Component{
		Enabled: true,
		Config: core.ComponentConfig{CoreDNS: &core.CoreDNSComponentConfig{
			CorefileTemplate: ". {\n    {groundplane}\n}\n", UpstreamAuto: true,
			UpstreamResolvers: []core.DNSResolverEndpoint{}, Forwarders: []core.DNSForwarder{},
		}},
	}, 42)
	if err != nil {
		t.Fatalf("ProjectManagedConfigFiles() error = %v", err)
	}
	if len(files) != 1 || files[0].Path != "/etc/groundplane/coredns/Corefile" ||
		!strings.Contains(files[0].Rendered, "bind 127.0.0.1") ||
		!strings.Contains(files[0].Rendered, "forward . 1.1.1.1") {
		t.Fatalf("ProjectManagedConfigFiles() = %#v", files)
	}
}
