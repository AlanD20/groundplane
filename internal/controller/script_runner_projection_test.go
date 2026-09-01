package controller

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

func TestValidateScriptRunnerReleaseSourceAllowsStagedPinnedCandidate(t *testing.T) {
	// Rationale: lifecycle hooks are authorized from the staged candidate
	// before it can become the current successful Release.
	image := "registry.example.invalid/app@sha256:" + strings.Repeat("a", 64)
	sources := etcd.ScriptExecutionSources{
		Release: etcd.CurrentSuccessfulRelease{},
		RenderInput: etcd.Versioned[etcd.ReleaseRenderInput]{Record: etcd.ReleaseRenderInput{
			Image: image,
			Projection: etcd.EnvironmentComposeProjection{
				RevisionID: "task-staged", RenderGeneration: 1,
			},
		}},
	}
	sources.Release.Intent.Image = image

	if err := validateScriptRunnerReleaseSource(sources); err != nil {
		t.Fatalf("validateScriptRunnerReleaseSource() error = %v", err)
	}
}

// Rationale: absent unsupported Compose fields are valid because they carry no
// authored intent; a zero-value service and an empty deploy placement must pass.
func TestValidateScriptServiceDispositionAllowsMissingFields(t *testing.T) {
	cases := []struct {
		name    string
		service composetypes.ServiceConfig
	}{
		{name: "zero service", service: composetypes.ServiceConfig{}},
		{name: "empty deploy", service: composetypes.ServiceConfig{Deploy: &composetypes.DeployConfig{}}},
		{
			name: "empty placement",
			service: composetypes.ServiceConfig{Deploy: &composetypes.DeployConfig{
				Placement: composetypes.Placement{},
			}},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if err := validateScriptServiceDisposition(test.service); err != nil {
				t.Fatalf("validateScriptServiceDisposition() error = %v", err)
			}
		})
	}
}

func TestStripControllerServiceExtensionsConsumesRecognizedMetadataOnly(t *testing.T) {
	// Rationale: compose-go retains projected Groundplane metadata in its
	// extension map; typed resource/release fields are already captured, while
	// every unknown extension must remain visible to closed-runner validation.
	service := composetypes.ServiceConfig{Extensions: composetypes.Extensions{
		composeResourceExtension: map[string]any{"id": "svc_exact"},
		"x-gp-release":          map[string]any{"default_strategy": "recreate"},
	}}
	service.Extensions = stripControllerServiceExtensions(service.Extensions)
	if service.Extensions != nil {
		t.Fatalf("normalized extensions = %#v, want nil", service.Extensions)
	}
	if err := validateScriptServiceDisposition(service); err != nil {
		t.Fatalf("validateScriptServiceDisposition() error = %v", err)
	}

	withUnknown := composetypes.ServiceConfig{Extensions: composetypes.Extensions{
		"x-gp-release": map[string]any{"default_strategy": "recreate"},
		"x-authored":   map[string]any{},
	}}
	withUnknown.Extensions = stripControllerServiceExtensions(withUnknown.Extensions)
	if withUnknown.Extensions == nil || len(withUnknown.Extensions) != 1 {
		t.Fatalf("unknown extension normalized away: %#v", withUnknown.Extensions)
	}
	if err := validateScriptServiceDisposition(withUnknown); err == nil {
		t.Fatal("validateScriptServiceDisposition() accepted unknown extension")
	}
}

func TestProjectScriptNetworksOmitsServiceEndpointIdentity(t *testing.T) {
	// Rationale: one-off runners join the Service's networks but must not claim
	// its aliases, static addresses, link-local addresses, MAC, or gateway role.
	const networkID = "net_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	sources := etcd.ScriptExecutionSources{
		DesiredProjection: etcd.Versioned[etcd.EnvironmentComposeProjection]{
			Record: etcd.EnvironmentComposeProjection{DesiredZones: []etcd.EnvironmentZoneProjection{{
				Desired: core.Zone{ID: networkID, Name: "backend"},
			}}},
		},
		Networks: []etcd.Versioned[etcd.ZoneRecord]{{
			Record: etcd.ZoneRecord{Desired: core.Zone{ID: networkID, Name: "backend"}},
			Revision: 7,
		}},
	}
	config := &composetypes.ServiceNetworkConfig{
		Aliases:         []string{"app-api"},
		GatewayPriority: 100,
		Ipv4Address:     "10.20.0.10",
		Ipv6Address:     "fd00::10",
		LinkLocalIPs:    []string{"169.254.10.10"},
		MacAddress:      "02:42:ac:11:00:02",
		DriverOpts:      map[string]string{"com.example.safe": "value"},
		InterfaceName:   "eth9",
		Priority:        25,
	}
	service := composetypes.ServiceConfig{Networks: map[string]*composetypes.ServiceNetworkConfig{
		"backend": config,
	}}

	projected, err := projectScriptNetworks(service, sources)
	if err != nil {
		t.Fatalf("projectScriptNetworks() error = %v", err)
	}
	if len(projected) != 1 || projected[0].NetworkId != networkID ||
		projected[0].RenderedAttachment.InterfaceName != "eth9" ||
		projected[0].RenderedAttachment.Priority != 25 ||
		len(projected[0].RenderedAttachment.DriverOptions) != 1 {
		t.Fatalf("projected runner network = %#v", projected)
	}

	config.Extensions = composetypes.Extensions{"x-unsafe": map[string]any{}}
	if _, err := projectScriptNetworks(service, sources); err == nil {
		t.Fatal("projectScriptNetworks() accepted unknown endpoint extension")
	}
}

// Rationale: explicit empty maps and slices are authored values, so they must
// remain rejected just like non-empty unsupported Compose fields.
func TestValidateScriptServiceDispositionRejectsPresentEmptyFields(t *testing.T) {
	cases := []struct {
		name    string
		service composetypes.ServiceConfig
	}{
		{name: "annotations", service: composetypes.ServiceConfig{Annotations: composetypes.Mapping{}}},
		{name: "cap add", service: composetypes.ServiceConfig{CapAdd: []string{}}},
		{name: "configs", service: composetypes.ServiceConfig{Configs: []composetypes.ServiceConfigObjConfig{}}},
		{name: "device cgroup rules", service: composetypes.ServiceConfig{DeviceCgroupRules: []string{}}},
		{name: "devices", service: composetypes.ServiceConfig{Devices: []composetypes.DeviceMapping{}}},
		{name: "env files", service: composetypes.ServiceConfig{EnvFiles: []composetypes.EnvFile{}}},
		{name: "external links", service: composetypes.ServiceConfig{ExternalLinks: []string{}}},
		{name: "gpus", service: composetypes.ServiceConfig{Gpus: []composetypes.DeviceRequest{}}},
		{name: "label files", service: composetypes.ServiceConfig{LabelFiles: []string{}}},
		{name: "links", service: composetypes.ServiceConfig{Links: []string{}}},
		{
			name:    "models",
			service: composetypes.ServiceConfig{Models: map[string]*composetypes.ServiceModelConfig{}},
		},
		{name: "secrets", service: composetypes.ServiceConfig{Secrets: []composetypes.ServiceSecretConfig{}}},
		{name: "volumes from", service: composetypes.ServiceConfig{VolumesFrom: []string{}}},
		{name: "pre start", service: composetypes.ServiceConfig{PreStart: []composetypes.ServiceHook{}}},
		{name: "post start", service: composetypes.ServiceConfig{PostStart: []composetypes.ServiceHook{}}},
		{name: "pre stop", service: composetypes.ServiceConfig{PreStop: []composetypes.ServiceHook{}}},
		{name: "service extensions", service: composetypes.ServiceConfig{Extensions: composetypes.Extensions{}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if err := validateScriptServiceDisposition(test.service); err == nil {
				t.Fatal("validateScriptServiceDisposition() succeeded for present empty unsupported field")
			}
		})
	}
}

// Rationale: deploy validation has the same authored-presence rule as service
// validation, including nested placement and resource extension maps/slices.
func TestScriptDeployResourcesAllowsMissingFields(t *testing.T) {
	cases := []struct {
		name   string
		deploy *composetypes.DeployConfig
	}{
		{name: "nil deploy", deploy: nil},
		{name: "empty deploy", deploy: &composetypes.DeployConfig{}},
		{name: "empty placement", deploy: &composetypes.DeployConfig{Placement: composetypes.Placement{}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := scriptDeployResources(test.deploy); err != nil {
				t.Fatalf("scriptDeployResources() error = %v", err)
			}
		})
	}
}

// Rationale: explicit empty deploy maps and slices are authored unsupported
// values and must not be treated as absent by validation.
func TestScriptDeployResourcesRejectsPresentEmptyFields(t *testing.T) {
	cases := []struct {
		name   string
		deploy *composetypes.DeployConfig
	}{
		{name: "deploy labels", deploy: &composetypes.DeployConfig{Labels: composetypes.Labels{}}},
		{
			name:   "placement constraints",
			deploy: &composetypes.DeployConfig{Placement: composetypes.Placement{Constraints: []string{}}},
		},
		{
			name:   "placement preferences",
			deploy: &composetypes.DeployConfig{Placement: composetypes.Placement{Preferences: []composetypes.PlacementPreferences{}}},
		},
		{
			name:   "placement extensions",
			deploy: &composetypes.DeployConfig{Placement: composetypes.Placement{Extensions: composetypes.Extensions{}}},
		},
		{
			name:   "resource extensions",
			deploy: &composetypes.DeployConfig{Resources: composetypes.Resources{Extensions: composetypes.Extensions{}}},
		},
		{name: "deploy extensions", deploy: &composetypes.DeployConfig{Extensions: composetypes.Extensions{}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := scriptDeployResources(test.deploy); err == nil {
				t.Fatal("scriptDeployResources() succeeded for present empty unsupported field")
			}
		})
	}
}
