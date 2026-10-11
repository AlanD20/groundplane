package agentchannel

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func durableRollingEvidence(value *agentpb.RollingReleaseEvidence) rollingrelease.Completion {
	return executionplan.RollingCompletion(value)
}

func validateAgentRollingEvidence(result *agentpb.ComposeTaskResult) error {
	if len(result.GetRollingEvidence()) > 32 {
		return errs.New(errs.KindValidationFailed, "Agent Task has too many rolling completions")
	}
	previous := ""
	for _, value := range result.GetRollingEvidence() {
		if err := rollingrelease.ValidateCompletion(durableRollingEvidence(value)); err != nil {
			return err
		}
		if value.GetServiceId() <= previous {
			return errs.New(errs.KindValidationFailed, "Agent rolling completions are not uniquely sorted")
		}
		previous = value.GetServiceId()
		for _, proxy := range result.GetProxyEvidence() {
			if proxy.GetServiceId() == previous {
				return errs.New(
					errs.KindValidationFailed,
					"Agent rolling completion also carries single-target proxy evidence",
				)
			}
		}
		for _, recreate := range result.GetRecreateEvidence() {
			if recreate.GetServiceId() == previous {
				return errs.New(errs.KindValidationFailed, "Agent rolling completion also carries recreate evidence")
			}
		}
	}
	return nil
}
