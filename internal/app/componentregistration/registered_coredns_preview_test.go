package componentregistration

import (
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	testhostresolution "github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testresolverbaseline "github.com/AlanD20/groundplane/internal/infra/etcd/resolverbaseline"
)

type registeredCoreDNSPreviewRecords struct {
	projection testhostresolution.HostResolutionProjectionRecord
}

func (records registeredCoreDNSPreviewRecords) GetHostResolutionProjection(
	context.Context,
) (testkeyvalue.Versioned[testhostresolution.HostResolutionProjectionRecord], bool, error) {
	return testkeyvalue.Versioned[testhostresolution.HostResolutionProjectionRecord]{
		Record: records.projection,
	}, true, nil
}

func (registeredCoreDNSPreviewRecords) GetHostResolverBaseline(
	context.Context,
) (testkeyvalue.Versioned[testresolverbaseline.Record], bool, error) {
	return testkeyvalue.Versioned[testresolverbaseline.Record]{Record: testresolverbaseline.Record{
		Generation: 1, Content: []byte("nameserver 1.1.1.1\n"),
	}}, true, nil
}

// Rationale: production composition must derive the public preview with the
// registered CoreDNS renderer and publish the exact Agent-managed target path.
func TestRegisteredCoreDNSManagedConfigProjectorUsesCompiledRenderer(t *testing.T) {
	t.Parallel()
	projection, err := testhostresolution.NewHostResolutionProjectionRecord(1, nil)
	if err != nil {
		t.Fatalf("NewHostResolutionProjectionRecord() error = %v", err)
	}
	records := registeredCoreDNSPreviewRecords{projection: projection}
	projector, err := NewDNSManagedConfigProjector(records, records)
	if err != nil {
		t.Fatalf("NewDNSManagedConfigProjector() error = %v", err)
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
