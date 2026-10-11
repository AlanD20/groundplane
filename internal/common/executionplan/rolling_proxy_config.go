package executionplan

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Compare complete routing to the captured replica set, not just its byte
// digest. Every replacement retains the initial listener port/protocol set.
func validateRollingProxyConfigs(
	plan rollingrelease.Plan,
	procedure *agentpb.RollingReleaseProcedure,
	proxy *agentpb.ComposeService,
) error {
	if proxy == nil {
		if procedure.GetHasProxy() {
			return errs.New(errs.KindValidationFailed, "rolling proxy artifact is missing")
		}
		return nil
	}
	if procedure.GetProxyImplementation() != proxy.GetProxyImplementation() {
		return errs.New(errs.KindValidationFailed, "rolling procedure changes its captured proxy implementation")
	}
	initial, err := OpenProxyConfiguration(proxy)
	if err != nil {
		return err
	}
	if err := serviceproxy.ValidateTrafficSwitchPorts(initial.Implementation, initial.Ports); err != nil {
		return err
	}
	if err := validateRollingProxyTargets(initial.Upstreams, plan.Prior); err != nil {
		return err
	}
	changes, err := plan.Changes()
	if err != nil {
		return err
	}
	if len(procedure.GetChanges()) != len(changes) {
		return errs.New(errs.KindValidationFailed, "rolling proxy changes do not cover the captured rollout")
	}
	for index, change := range procedure.GetChanges() {
		configuration, err := serviceproxy.Read(initial.Implementation, change.GetProxyConfigJson())
		if err != nil {
			return err
		}
		if !slices.Equal(configuration.Ports, initial.Ports) || configuration.ReleaseID != plan.ReleaseID ||
			configuration.Generation != change.GetProxyGeneration() {
			return errs.New(
				errs.KindValidationFailed,
				"rolling switch changes its captured listeners, Release or generation",
			)
		}
		if err := validateRollingProxyTargets(configuration.Upstreams, changes[index].After); err != nil {
			return err
		}
	}
	return nil
}

func validateRollingProxyTargets(actual []string, replicas []rollingrelease.Replica) error {
	expected := make([]string, len(replicas))
	for index, replica := range replicas {
		expected[index] = replica.RoutingName()
	}
	slices.Sort(expected)
	if !slices.Equal(actual, expected) {
		return errs.New(errs.KindValidationFailed, "rolling proxy upstreams differ from the captured replica set")
	}
	return nil
}
