package app

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/compose-spec/compose-go/v2/loader"
	composetypes "github.com/compose-spec/compose-go/v2/types"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: compiled bootstrap commands must survive the real Compose parser,
// without substituting Controller/helper environment values into shell code.
func TestBackingServiceComposePreservesRuntimeShell(t *testing.T) {
	registerAdapters()
	for _, input := range []struct {
		adapter string
		mode    core.BackingAuthentication
	}{
		{"postgres:16", ""},
		{"valkey:9", core.BackingAuthenticationUsernamePassword},
		{"valkey:9", core.BackingAuthenticationPassword},
		{"valkey:9", core.BackingAuthenticationNone},
	} {
		t.Run(input.adapter+"/"+string(input.mode), func(t *testing.T) {
			spec, err := adapters.BackingCreationSpec(input.adapter, input.mode)
			if err != nil {
				t.Fatal(err)
			}
			const suffix = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
			environment := etcd.EnvironmentRecord{
				ID:        "env_" + suffix,
				VolumeDir: "/var/lib/groundplane/vol/backing/prj_" + suffix + "/env_" + suffix,
			}
			zone := core.Zone{ID: "net_" + suffix, Name: "data", Subnet: "10.92.1.0/24"}
			volume := core.Volume{ID: "vol_" + suffix, Key: spec.VolumeKey, Slug: spec.VolumeSlug}
			project := backingComposeProject(spec, "valkey:9", zone, volume, environment)
			artifact, err := controller.RenderCompose(controller.ComposeRenderInput{
				Project: project, ProjectOwnerKind: controller.ComposeProjectOwnerBacking,
				ProjectID: "prj_" + suffix, EnvironmentID: environment.ID,
				ArtifactID: "cfg_" + suffix, PlanID: "plan_" + suffix,
				RenderGeneration: 1, AuthorizedVolumeDir: environment.VolumeDir,
				Identities: controller.ComposeIdentitySnapshot{
					Services: []controller.ComposeResourceIdentity{{ID: "svc_" + suffix, Name: spec.ServiceName}},
					Networks: []controller.ComposeResourceIdentity{{ID: zone.ID, Name: zone.Name}},
					Volumes:  []controller.ComposeResourceIdentity{{ID: volume.ID, Name: volume.Key}},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(artifact.CanonicalYaml), "${") {
				t.Error("bootstrap contains forbidden unresolved interpolation syntax")
			}
			parsed, err := loader.LoadWithContext(context.Background(), composetypes.ConfigDetails{
				WorkingDir:  environment.VolumeDir,
				ConfigFiles: []composetypes.ConfigFile{{Filename: "compose.yaml", Content: artifact.CanonicalYaml}},
				Environment: composetypes.Mapping{
					"VALKEY_PASSWORD": "must-not-be-substituted", "VALKEY_AUTHENTICATION": "wrong",
					"POSTGRES_USER": "wrong", "POSTGRES_DB": "wrong",
				},
			}, func(options *loader.Options) { options.SkipResolveEnvironment = true })
			if err != nil {
				t.Fatal(err)
			}
			service := parsed.Services[spec.ServiceName]
			if !reflect.DeepEqual([]string(service.Command), spec.Command) {
				t.Error("Compose changed the runtime command")
			}
			if !reflect.DeepEqual([]string(service.HealthCheck.Test), spec.HealthCommand) {
				t.Error("Compose changed the runtime health command")
			}
		})
	}
}
