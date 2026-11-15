package blueprint

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
)

// A Service consumes more than its own Compose declaration. Bind its release
// intent to the Entries, Attaches, Networks, Volumes and Scripts it reads so an
// already-applied acknowledgement cannot skip newly changed configuration.
// Service startup ordering is not a consumed configuration input.
func (planner *authoredUnitPlanner) fingerprintServiceInputs() (bool, error) {
	type input struct {
		Target      blueprintunits.ResourceKey
		Fingerprint string
	}
	for target, unit := range planner.units {
		if target.Kind != ids.KindService {
			continue
		}
		var inputs []input
		for _, read := range unit.Reads {
			if read.Kind == ids.KindService {
				continue
			}
			dependency, found := planner.units[read]
			if !found {
				return false, nil
			}
			inputs = append(inputs, input{read, dependency.Fingerprint})
		}
		fingerprint, err := authoredUnitFingerprint("service-inputs", struct {
			Definition string
			Inputs     []input
		}{unit.Fingerprint, inputs})
		if err != nil {
			return false, err
		}
		unit.Fingerprint = fingerprint
		planner.units[target] = unit
	}
	return true, nil
}
