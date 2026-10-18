package attachprovision

import (
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
)

func ProvisionClaims(capture blueprintattachinputs.Generation, hostID string) []workexecution.Claim {
	resources := []string{capture.AttachID, capture.EnvironmentID, capture.ConsumerServiceID,
		capture.BackingServiceID, capture.BackingEnvironmentID, capture.BackingNetworkID, capture.CredentialOwnerID}
	if capture.Provision != nil {
		for _, grant := range capture.Provision.Grants {
			resources = append(resources, grant.Record.ID)
		}
	}
	claims := make([]workexecution.Claim, 0, len(resources)*2)
	for _, id := range resources {
		claims = append(claims, workexecution.Claim{ScopeID: id}, workexecution.Claim{HostID: hostID, ScopeID: id})
	}
	slices.SortFunc(claims, func(left, right workexecution.Claim) int {
		if order := strings.Compare(left.HostID, right.HostID); order != 0 {
			return order
		}
		return strings.Compare(left.ScopeID, right.ScopeID)
	})
	return slices.Compact(claims)
}
