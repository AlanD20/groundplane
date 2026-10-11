package componentplanning

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Disabled Controller selection binds the same captured rollouts and value
// references without constructing an Agent Intent or a no-op Task.
func (authority FrozenProjectionAuthority) WithDisabledProxyRollouts(
	input keyvalue.Versioned[ProxyCandidateInputReference], rollouts []ProxyServiceRolloutReference,
	hold *ProxySourceHold, fragment scriptsourcepublication.ScriptSourcePublicationFragment,
) (FrozenProjectionAuthority, error) {
	if authority.proxyServicePreflight == nil || authority.unitRuntimeCondition == nil ||
		input.Record.Validate() != nil || input.Revision <= 0 || input.ReadRevision < input.Revision ||
		input.Record.SourceOwner != ProxyInputControllerSettlement || input.Record.EnvironmentID != authority.environmentID ||
		input.Record.ParentTaskID != authority.desiredRevisionID || authority.unitRuntimeCondition.Key != RuntimeKey(input.Record.ComponentID) {
		return FrozenProjectionAuthority{}, errs.New(
			errs.KindValidationFailed,
			"disabled proxy rollout input differs from its frozen authority",
		)
	}
	preparation := ComponentTaskPreparation{
		proxyServicePreflight:  cloneProxyServicePreflight(authority.proxyServicePreflight),
		proxyServiceConditions: slices.Clone(authority.proxyServiceConditions),
	}
	owned := input.Record
	preparation.proxyServicePreflight.CandidateInput = &owned
	preparation.proxyServiceConditions = append(preparation.proxyServiceConditions,
		keyvalue.Condition{Key: ProxyCandidateInputKey(owned), ModRevision: input.Revision})
	preparation, err := preparation.WithProxyServiceRollouts(rollouts)
	if err != nil {
		return FrozenProjectionAuthority{}, err
	}
	if hold != nil {
		preparation, err = preparation.WithProxySourceHold(*hold, fragment)
		if err != nil {
			return FrozenProjectionAuthority{}, err
		}
	} else if len(fragment.Conditions()) != 0 || len(fragment.Mutations()) != 0 {
		return FrozenProjectionAuthority{}, errs.New(errs.KindValidationFailed, "disabled proxy source fragment has no owner")
	}
	authority.proxyServicePreflight = preparation.proxyServicePreflight
	authority.proxyServiceConditions = preparation.proxyServiceConditions
	authority.proxySourceMutations = preparation.proxySourceMutations
	return authority, nil
}

func (authority FrozenProjectionAuthority) ControllerProxyOperationID() (string, bool) {
	if authority.proxyServicePreflight == nil || authority.proxyServicePreflight.CandidateInput == nil ||
		authority.proxyServicePreflight.CandidateInput.SourceOwner != ProxyInputControllerSettlement {
		return "", false
	}
	return authority.proxyServicePreflight.CandidateInput.SourceID, true
}
