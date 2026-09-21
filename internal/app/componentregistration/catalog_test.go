package componentregistration

import (
	testing "testing"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	registeredcoredns "github.com/AlanD20/groundplane-registered-components/coredns"
)

func TestRegisteredPlannerRejectsUnregisteredValidImage(t *testing.T) {
	catalog, err := NewCatalog()
	if err != nil {
		t.Fatal(err)
	}
	unknown := registeredcoredns.Image
	unknown.Platforms = append([]componentsdk.OCIPlatform(nil), unknown.Platforms...)
	unknown.Repository = "example/unknown-resolver"
	catalog.planners["coredns"] = func(string, componentdns.RenderInput) (componentsdk.EnvironmentPlan, error) {
		return componentsdk.EnvironmentPlan{Services: []componentsdk.ManagedService{{Image: unknown}}}, nil
	}
	if _, err := catalog.Plan("coredns", "svc_test", componentdns.RenderInput{}); err == nil {
		t.Fatal("Plan() accepted an unregistered valid managed image")
	}
}
