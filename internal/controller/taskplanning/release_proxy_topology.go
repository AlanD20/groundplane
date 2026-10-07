package taskplanning

import (
	"bytes"
	"encoding/json"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"gopkg.in/yaml.v3"
)

// A blue-green switch updates the proxy's destination, not its Docker network
// attachments or published sockets. Reject changes that require recreating that
// shared proxy before constructing any host-executable rollout.
func validateReleaseProxyTopology(
	strategy domain.Strategy,
	candidate, prior *agentpb.ComposeArtifact,
	serviceID string,
) error {
	if strategy != domain.StrategyBlueGreen || prior == nil {
		return nil
	}
	shape := func(artifact *agentpb.ComposeArtifact) ([]byte, error) {
		var name string
		for _, metadata := range artifact.GetServices() {
			if metadata.GetServiceId() == serviceID &&
				metadata.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
				name = metadata.GetComposeName()
			}
		}
		if name == "" {
			return nil, errs.New(errs.KindStateConflict, "serving Service proxy topology is unavailable")
		}
		// Only the stable proxy's network/socket shape participates in this
		// comparison. Workload fields use Compose's loader-specific decoding
		// (including durations), not yaml.v3 unmarshalling into Project.
		type proxyShape struct {
			Networks map[string]*composetypes.ServiceNetworkConfig `yaml:"networks"`
			Ports    []composetypes.ServicePortConfig              `yaml:"ports"`
			Expose   composetypes.StringOrNumberList               `yaml:"expose"`
			Restart  string                                        `yaml:"restart"`
		}
		var project struct {
			Services map[string]proxyShape `yaml:"services"`
		}
		if err := yaml.Unmarshal(artifact.GetCanonicalYaml(), &project); err != nil {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
		proxy, found := project.Services[name]
		if !found {
			return nil, errs.New(errs.KindStateConflict, "Service proxy is absent from its captured configuration")
		}
		return json.Marshal(proxy)
	}
	before, err := shape(prior)
	if err != nil {
		return err
	}
	after, err := shape(candidate)
	if err != nil {
		return err
	}
	if !bytes.Equal(before, after) {
		return errs.New(
			errs.KindValidationFailed,
			"blue-green cannot change the serving proxy's Zones, aliases, ports or restart policy; keep those settings or explicitly select recreate",
		)
	}
	return nil
}
