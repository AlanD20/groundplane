package controllerbackup

import (
	"context"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/databaseversion"
	"github.com/AlanD20/groundplane/internal/controller/backup"
	"github.com/AlanD20/groundplane/internal/controller/serviceobservation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func NewDatabaseVersionObserver(
	agents *agentregistration.Repository,
	channel serviceobservation.Channel,
) backup.DatabaseVersionObserver {
	return func(ctx context.Context, selected backupplanning.DatabaseRestoreSelection) (databaseversion.Target, error) {
		probe, err := backupplanning.BuildDatabaseVersionProbe(selected)
		if err != nil {
			return databaseversion.Target{}, err
		}
		serviceID := probe.Authority.GetRestore().GetPostgres().GetDatabaseServiceId()
		if mysql := probe.Authority.GetRestore().GetMysql(); mysql != nil {
			serviceID = mysql.DatabaseServiceId
		}
		var target *agentpb.ServiceObservationTarget
		for _, artifact := range selected.Artifacts {
			for _, service := range artifact.Services {
				if service.ServiceId != serviceID {
					continue
				}
				if target != nil {
					return databaseversion.Target{}, errs.New(
						errs.KindStateConflict,
						"Restore database runtime is ambiguous",
					)
				}
				labels := make(map[string]string, len(service.ExpectedLabels))
				for _, label := range service.ExpectedLabels {
					labels[label.Key] = label.Value
				}
				generation, err := strconv.ParseUint(labels["com.groundplane.render-generation"], 10, 64)
				if err != nil {
					return databaseversion.Target{}, errs.New(
						errs.KindStateConflict,
						"Restore database generation is unavailable",
					)
				}
				role := labels["com.groundplane.runtime-role"]
				if role == "" {
					role = "backing"
				}
				target = &agentpb.ServiceObservationTarget{EnvironmentId: artifact.OwnerId, ServiceId: serviceID,
					ComposeName: service.ComposeName, PlanId: labels["com.groundplane.plan-id"], RenderGeneration: generation,
					RuntimeRole: role, ReleaseId: labels["com.groundplane.release-id"], Slot: labels["com.groundplane.slot"], DatabaseProbe: probe}
			}
		}
		if target == nil {
			return databaseversion.Target{}, errs.New(errs.KindStateConflict, "Restore database runtime is unavailable")
		}
		agent, err := agents.GetSingleton(ctx)
		if err != nil {
			return databaseversion.Target{}, err
		}
		result, err := channel.ObserveServices(ctx, agent.Record.ID, []*agentpb.ServiceObservationTarget{target})
		if err != nil {
			return databaseversion.Target{}, err
		}
		if result == nil || len(result.Observations) != 1 {
			return databaseversion.Target{}, errs.New(
				errs.KindStateConflict,
				"Restore database version observation is unavailable",
			)
		}
		return databaseversion.FromWire(result.Observations[0].DatabaseVersions)
	}
}
