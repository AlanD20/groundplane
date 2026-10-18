package composerender

import (
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// ManagedAttachExternalNetworks preserves the resolved Attach network identities
// when rendering captured runtime. It does not select or create new Attaches.
func ManagedAttachExternalNetworks(project *composetypes.Project) ([]composeidentity.Resource, error) {
	if project == nil {
		return nil, errs.New(errs.KindInternal, "Attach network rendering requires a captured project")
	}
	external := make([]composeidentity.Resource, 0)
	for name, network := range project.Networks {
		networkID, managed := ManagedAttachNetworkID(name)
		if !managed {
			continue
		}
		if !network.External {
			return nil, errs.New(errs.KindInternal, "managed Attach network is not external")
		}
		external = append(external, composeidentity.Resource{ID: networkID, Name: name})
	}
	sort.Slice(external, func(left, right int) bool { return external[left].Name < external[right].Name })
	return external, nil
}

func ManagedAttachNetworkID(composeName string) (string, bool) {
	const prefix = "gp_attach_net_"
	if !strings.HasPrefix(composeName, prefix) {
		return "", false
	}
	networkID := "net_" + strings.ToUpper(strings.TrimPrefix(composeName, prefix))
	return networkID, ids.Validate(ids.KindNetwork, networkID) == nil
}
