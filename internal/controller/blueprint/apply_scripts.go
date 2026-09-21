package blueprint

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

func (service *Service) prepareApplyScripts(
	ctx context.Context,
	environmentID, revisionID string,
	authored map[string]core.ScriptSpec,
	desiredServices []core.Service,
	resources desiredrevision.BlueprintScriptResources,
	allocate func(ids.Kind, string) string,
) (desiredrevision.BlueprintScriptReconciliation, etcd.BlueprintScriptPublication, error) {
	scriptRepository := service.repository
	currentScripts, scriptsReadRevision, err := service.listBlueprintScripts(ctx, environmentID, scriptRepository)
	if err != nil {
		return desiredrevision.BlueprintScriptReconciliation{}, etcd.BlueprintScriptPublication{}, err
	}
	scriptServices := make([]servicerecord.ServiceRecord, len(desiredServices))
	for index, desiredService := range desiredServices {
		scriptServices[index] = servicerecord.ServiceRecord{EnvironmentID: environmentID, Desired: desiredService}
	}
	previousScripts := make([]scriptrecord.Record, len(currentScripts))
	for index, currentScript := range currentScripts {
		previousScripts[index] = currentScript.Record
	}
	reconciledScripts, err := desiredrevision.ReconcileBlueprintScripts(
		environmentID,
		authored,
		scriptServices,
		previousScripts,
		resources,
		allocate,
	)
	if err != nil {
		return desiredrevision.BlueprintScriptReconciliation{}, etcd.BlueprintScriptPublication{}, err
	}
	scriptPublication, err := scriptRepository.PrepareBlueprintScriptPublication(
		ctx,
		environmentID,
		scriptsReadRevision,
		revisionID,
		currentScripts,
		reconciledScripts.Current,
		reconciledScripts.BodyGenerations,
	)
	if err != nil {
		return desiredrevision.BlueprintScriptReconciliation{}, etcd.BlueprintScriptPublication{}, err
	}

	return reconciledScripts, scriptPublication, nil
}
