package blueprint

import (
	"github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
)

// DNS depends on the selected Zone and named Service, never the whole Project
// or Tenant whose labels appear in its namespace. A missing Service remains a
// real waiting record and must still settle the absence of its DNS answer.
func (planner *authoredUnitPlanner) planDNSUnits() (bool, error) {
	complete := true
	for _, label := range sortedAuthoredMapKeys(planner.input.desired.Input.DNS) {
		spec := planner.input.desired.Input.DNS[label]
		identity, found := planner.identities[ids.KindDNSRecord][label]
		if !found {
			return false, planner.missingInput("DNS", label)
		}
		var reads []blueprintunits.ResourceKey
		var zoneID, serviceID string
		if spec.Service != "" {
			zone, found := planner.identities[ids.KindNetwork][spec.Zone]
			if !found {
				return false, planner.missingInput("DNS Zone", spec.Zone)
			}
			zoneID = zone.ID
			reads = append(reads, authoredResourceKey(ids.KindNetwork, zoneID))
			if service, exists := planner.identities[ids.KindService][spec.Service]; exists {
				serviceID = service.ID
				reads = append(reads, authoredResourceKey(ids.KindService, serviceID))
			}
		}
		fingerprint, err := authoredUnitFingerprint("dns", struct {
			Label              string
			Spec               dnsrecords.EnvironmentSpec
			ZoneID             string
			ServiceID          string
			ZoneFingerprint    string
			ServiceFingerprint string
		}{label, spec, zoneID, serviceID,
			planner.units[authoredResourceKey(ids.KindNetwork, zoneID)].Fingerprint,
			planner.units[authoredResourceKey(ids.KindService, serviceID)].Fingerprint})
		if err != nil {
			return false, err
		}
		planner.addUnit(identity, fingerprint, reads, reads)
	}
	return complete, nil
}
