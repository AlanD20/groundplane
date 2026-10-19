package environmentexecution

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func ValidateHelperResponse(input Input, inspection bool, response *agentpb.EnvironmentProvisionHelperResponse) error {
	if input.Validate() != nil || response == nil || executionplan.RejectUnknown(response) != nil ||
		response.EnvironmentId != input.Creation.EnvironmentId || response.Inspection != inspection ||
		response.ObservedAt == nil || response.ObservedAt.CheckValid() != nil ||
		response.ObservedAt.AsTime().
			Before(input.Attempt.AssignedAt) ||
		response.ObservedAt.AsTime().After(time.Now().Add(30*time.Second)) {
		return invalid("environment provisioning helper observation is invalid")
	}
	reference, err := DecodeReference(response.Input)
	if err != nil || reference != input.Attempt.Input {
		return invalid("environment provisioning helper observed a different captured input")
	}
	if !inspection && !response.DirectoryReady {
		return invalid("environment provisioning helper did not prove directory readiness")
	}
	return nil
}
