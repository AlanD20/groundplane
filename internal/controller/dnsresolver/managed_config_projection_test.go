package dnsresolver

import (
	"context"
	"crypto/sha256"
	"testing"

	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type previewProjectionReader struct {
	record etcd.HostResolutionProjectionRecord
}

func (reader previewProjectionReader) GetHostResolutionProjection(
	context.Context,
) (etcd.Versioned[etcd.HostResolutionProjectionRecord], bool, error) {
	return etcd.Versioned[etcd.HostResolutionProjectionRecord]{Record: reader.record}, true, nil
}

type previewBaselineReader struct {
	found bool
	reads int
}

func (reader *previewBaselineReader) GetHostResolverBaseline(
	context.Context,
) (etcd.Versioned[etcd.HostResolverBaselineRecord], bool, error) {
	reader.reads++
	return etcd.Versioned[etcd.HostResolverBaselineRecord]{Record: etcd.HostResolverBaselineRecord{
		Generation: 7,
		Content:    []byte("nameserver 1.1.1.1\n"),
	}}, reader.found, nil
}

type previewRenderer struct{ input componentdns.RenderInput }

func (renderer *previewRenderer) Render(input componentdns.RenderInput) ([]byte, error) {
	renderer.input = componentdns.CloneRenderInput(input)
	return []byte("controller-rendered Corefile\n"), nil
}

func (*previewRenderer) Digest(componentdns.RenderInput) ([sha256.Size]byte, error) {
	return [sha256.Size]byte{1}, nil
}

// Rationale: preview reads must derive bytes from the already-persisted host
// resolver baseline and current host-resolution projection without capturing
// or publishing baseline state as a GET side effect.
func TestManagedConfigProjectorUsesOnlyDurableReadInputs(t *testing.T) {
	t.Parallel()
	projection, err := etcd.NewHostResolutionProjectionRecord(11, nil)
	if err != nil {
		t.Fatalf("NewHostResolutionProjectionRecord() error = %v", err)
	}
	baselines := &previewBaselineReader{found: true}
	renderer := &previewRenderer{}
	projector, err := NewManagedConfigProjector(
		previewProjectionReader{record: projection}, baselines, renderer,
		"/etc/groundplane/coredns/Corefile",
	)
	if err != nil {
		t.Fatalf("NewManagedConfigProjector() error = %v", err)
	}
	template := ". {\n    {groundplane}\n}\n"
	files, err := projector.ProjectManagedConfigFiles(context.Background(), core.Component{
		Enabled: true,
		Config: core.ComponentConfig{CoreDNS: &core.CoreDNSComponentConfig{
			CorefileTemplate: template, UpstreamAuto: true,
			UpstreamResolvers: []core.DNSResolverEndpoint{}, Forwarders: []core.DNSForwarder{},
		}},
	}, 42)
	if err != nil {
		t.Fatalf("ProjectManagedConfigFiles() error = %v", err)
	}
	if baselines.reads != 1 || len(files) != 1 || files[0].Path != "/etc/groundplane/coredns/Corefile" ||
		files[0].Template != template || files[0].Rendered != "controller-rendered Corefile\n" {
		t.Fatalf("ProjectManagedConfigFiles() = %#v, baseline reads = %d", files, baselines.reads)
	}
	if len(renderer.input.CatchAll) != 1 || renderer.input.CatchAll[0].Address.String() != "1.1.1.1" {
		t.Fatalf("renderer input catch-all = %#v", renderer.input.CatchAll)
	}
}

// Rationale: an absent durable baseline must fail the preview read rather than
// causing GET to capture and persist mutable host state.
func TestManagedConfigProjectorDoesNotCreateMissingBaseline(t *testing.T) {
	t.Parallel()
	projection, err := etcd.NewHostResolutionProjectionRecord(11, nil)
	if err != nil {
		t.Fatalf("NewHostResolutionProjectionRecord() error = %v", err)
	}
	baselines := &previewBaselineReader{}
	projector, err := NewManagedConfigProjector(
		previewProjectionReader{record: projection}, baselines, &previewRenderer{},
		"/etc/groundplane/coredns/Corefile",
	)
	if err != nil {
		t.Fatalf("NewManagedConfigProjector() error = %v", err)
	}
	_, err = projector.ProjectManagedConfigFiles(context.Background(), core.Component{
		Enabled: true,
		Config: core.ComponentConfig{CoreDNS: &core.CoreDNSComponentConfig{
			CorefileTemplate: ". {\n    {groundplane}\n}\n", UpstreamAuto: true,
			UpstreamResolvers: []core.DNSResolverEndpoint{}, Forwarders: []core.DNSForwarder{},
		}},
	}, 42)
	if err == nil || baselines.reads != 1 {
		t.Fatalf("ProjectManagedConfigFiles() error = %v, baseline reads = %d", err, baselines.reads)
	}
}
