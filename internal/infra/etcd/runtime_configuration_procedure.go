package etcd

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// The durable descriptor and Task must name the same retained predecessor and
// complete set of forward file writes before publication or claim.
func validateTaskConfigurationProcedure(task TaskRecord, procedure *agentpb.CandidateReleaseProcedure,
	taskSteps map[string]struct{}) error {
	configuration := procedure.GetConfigurationRestoration()
	if configuration == nil {
		if len(task.Materializations) != 0 {
			return corruptReleaseRecord()
		}
		return nil
	}
	if _, present, err := taskConfigurationCondition(task); err != nil || !present ||
		len(configuration.GetFiles()) != len(task.Materializations) {
		return corruptReleaseRecord()
	}
	id := ""
	var digest []byte
	if task.Configuration.Prior != nil {
		id = task.Configuration.Prior.ID
		var err error
		digest, err = hex.DecodeString(task.Configuration.Prior.SHA256)
		if err != nil {
			return corruptReleaseRecord()
		}
	}
	if !executionplan.ConfigurationSnapshotMatches(configuration, id, digest) {
		return corruptReleaseRecord()
	}
	forward := make(map[string]bool, len(task.Materializations))
	for _, file := range task.Materializations {
		forward[file.StepID] = true
	}
	for _, file := range configuration.GetFiles() {
		if !forward[file.GetForwardStepId()] {
			return corruptReleaseRecord()
		}
		delete(forward, file.GetForwardStepId())
		for _, id := range []string{file.GetForwardStepId(), file.GetProbeStepId(), file.GetCompensateStepId()} {
			if _, exists := taskSteps[id]; !exists {
				return corruptReleaseRecord()
			}
		}
	}
	if len(forward) != 0 {
		return corruptReleaseRecord()
	}
	return nil
}
