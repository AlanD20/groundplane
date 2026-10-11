package releaseoperation

import (
	"github.com/AlanD20/groundplane/internal/common/backingcatalog"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type releaseExecutionSettings struct {
	strategy  domain.Strategy
	onFailure domain.OnFailure
	replicas  uint32
}

// Deploy and image-only Rollback obey the same current Service policy. History
// selects rollback image bytes, not old topology, health or failure settings.
func selectReleaseExecution(planning releasequeries.ReleasePlanningService, image, requestedStrategy string,
	requestedFailure domain.OnFailure, hasServing bool,
) (releaseExecutionSettings, error) {
	desired := planning.Service.Record.Desired
	strategy, err := domain.DeploymentStrategy(requestedStrategy, string(desired.Strategy))
	if err != nil {
		return releaseExecutionSettings{}, err
	}
	onFailure, err := releaseFailurePolicy(requestedFailure, desired.OnFailure)
	if err != nil {
		return releaseExecutionSettings{}, err
	}
	replicas := desired.Replicas
	if replicas == 0 {
		replicas = 1
	}
	if replicas < 1 || uint64(replicas) > uint64(^uint32(0)) {
		return releaseExecutionSettings{}, errs.New(errs.KindValidationFailed, "release replica count is invalid")
	}
	if err := domain.ValidateDeploymentReadiness(desired.Name, strategy, uint32(replicas), desired.Healthcheck.Enabled()); err != nil {
		return releaseExecutionSettings{}, err
	}
	if planning.Service.Record.BackingNetworkID != "" {
		if !backingcatalog.ValidImage(desired.Adapter, desired.AdapterVersion, image) ||
			strategy != domain.StrategyRecreate || replicas != 1 ||
			(desired.Adapter == "postgres" && planning.Service.Record.Runtime.PostgresToolsImage == "") {
			return releaseExecutionSettings{}, errs.New(
				errs.KindValidationFailed,
				"Backing image release requires an upstream image matching its selected server version and singleton recreate",
			)
		}
		if !hasServing && onFailure != domain.OnFailureLeaveActive {
			return releaseExecutionSettings{}, errs.New(errs.KindValidationFailed,
				"first Backing image release requires leave_active; no previous Deployment exists for switch-back")
		}
	}
	return releaseExecutionSettings{strategy: strategy, onFailure: onFailure, replicas: uint32(replicas)}, nil
}
