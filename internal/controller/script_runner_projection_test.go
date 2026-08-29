package controller

import (
	"testing"

	composetypes "github.com/compose-spec/compose-go/v2/types"
)

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
