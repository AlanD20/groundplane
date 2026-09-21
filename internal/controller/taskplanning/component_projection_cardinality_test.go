package taskplanning

import (
	"testing"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
)

// Rationale: generated Services are not desired Services. Replaying two
// enabled Components must work with zero or one native Service, never panic
// while allocating a slice with a negative derived capacity.
func TestPinnedComponentProjectionAllowsMoreManagedThanNativeServices(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(map[bool]string{false: "zero-native", true: "one-native"}[native], func(t *testing.T) {
			project, identity, projection, routes, catalog := componentPlanProjectionInput(t)
			if !native {
				delete(project.Services, "api")
				projection.DesiredServices = nil
				projection.DesiredRoutes = nil
				routes = nil
			}
			tunnelID := ids.New(ids.KindService)
			tunnel, err := testcomponents.NewRecord(core.Component{
				ID: projection.Components[1].Desired.ID, Owner: core.ComponentOwnerEnvironment,
				OwnerID: identity.EnvironmentID, Kind: core.ComponentKindEdgeCloudflare, Enabled: true,
				GeneratedServices: []string{tunnelID},
				Config: core.ComponentConfig{CloudflareTunnel: &core.CloudflareTunnelComponentConfig{
					SecretID: ids.New(ids.KindSecret), ZoneIDs: []string{projection.DesiredZones[0].Desired.ID},
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			projection.Components[1] = tunnel
			catalog[1] = componentTestRegistration(t, core.ComponentKindEdgeCloudflare,
				func(environment core.Environment, instance core.Component) (componentsdk.EnvironmentPlan, error) {
					plan, err := (componentPlanProjectionRenderer{serviceName: "cloudflare-tunnel"}).Plan(
						environment,
						instance,
					)
					plan.Files = nil
					return plan, err
				})
			result, err := projectPinnedEnvironmentComponents(
				project,
				nil,
				identity,
				projection,
				routes,
				nil,
				nil,
				catalog,
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Project.Services) != len(project.Services)+2 || len(result.PlainFiles) != 1 {
				t.Fatalf("managed replay lost native or generated Services/files: %#v", result)
			}
			if len(projection.DesiredServices) != len(project.Services) {
				t.Fatal("managed replay mutated the native desired projection")
			}
		})
	}
}
