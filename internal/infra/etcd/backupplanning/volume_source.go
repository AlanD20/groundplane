package backupplanning

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	environmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
	"sort"
)

func (repository *Planner) prepareManualVolumeSource(
	ctx context.Context,
	attempt backupruntime.BackupRunSourceAttemptRecord,
	environmentID string,
	fixedRevision int64,
) (backupruntime.BackupRunSourceAttemptRecord, error) {
	evidence, err := environmentqueries.LoadBackupVolumeProjectionEvidence(
		ctx, repository.store, environmentID, attempt.TargetID, fixedRevision,
	)
	if err != nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, err
	}
	if evidence.Environment.Record.VolumeDir == "" {
		return backupruntime.BackupRunSourceAttemptRecord{}, errs.New(
			errs.KindStateConflict,
			"backup Volume projection is unavailable",
		)
	}
	services, err := repository.manualBackupVolumeConsumers(
		ctx, environmentID, attempt.TargetID, evidence.Projection.Record, fixedRevision,
	)
	if err != nil {
		return backupruntime.BackupRunSourceAttemptRecord{}, err
	}
	attempt.TargetRevision = evidence.Projection.Revision
	attempt.Format = backupruntime.BackupRuntimeFormatVolume
	attempt.Snapshot.Volume = &backupruntime.BackupVolumeSourceSnapshot{
		EnvironmentID:       environmentID,
		EnvironmentRevision: evidence.Environment.Revision,
		VolumeID:            evidence.Volume.ID,
		DesiredRevisionID:   evidence.Projection.Record.RevisionID,
		ProjectionRoot:      evidence.ProjectionRoot,
		DependencyDigest:    evidence.DependencyDigest,
		RenderGeneration:    evidence.Projection.Record.RenderGeneration,
		ComposeVolumeKey:    evidence.Volume.Key,
		DockerVolumeName:    "gp_vol_" + evidence.Volume.ID,
		AuthorizedVolumeDir: evidence.Environment.Record.VolumeDir,
		Services:            services,
	}
	return attempt, nil
}

func (repository *Planner) manualBackupVolumeConsumers(
	ctx context.Context,
	environmentID string,
	volumeID string,
	projection projectionrecord.EnvironmentComposeProjection,
	fixedRevision int64,
) ([]backupruntime.BackupVolumeServiceSnapshot, error) {
	serviceKeys := make(map[string]string, len(projection.DesiredServices))
	for _, desired := range projection.DesiredServices {
		serviceKeys[desired.Desired.ID] = desired.Desired.Name
	}
	mounts := make(map[string][]string)
	for _, mount := range projection.VolumeMounts {
		if mount.VolumeID == volumeID {
			mounts[mount.ServiceID] = append(mounts[mount.ServiceID], mount.Target)
		}
	}
	serviceIDs := make([]string, 0, len(mounts))
	for serviceID := range mounts {
		serviceIDs = append(serviceIDs, serviceID)
		sort.Strings(mounts[serviceID])
	}
	sort.Strings(serviceIDs)
	result := make([]backupruntime.BackupVolumeServiceSnapshot, 0)
	for _, serviceID := range serviceIDs {
		service, serviceErr := environmentqueries.FindServiceAtRevision(ctx, repository.store, serviceID, fixedRevision)
		if serviceErr != nil || service.Record.EnvironmentID != environmentID {
			return nil, errs.New(errs.KindStateConflict, "Volume consumer Service evidence changed")
		}
		mountPaths := mounts[service.Record.Desired.ID]
		if len(mountPaths) != 0 {
			composeKey := serviceKeys[service.Record.Desired.ID]
			if composeKey == "" && backupVolumeTargetsGeneratedService(
				projection.Components,
				service.Record.Desired.ID,
			) {
				composeKey = service.Record.Desired.Name
			}
			if composeKey == "" {
				return nil, errs.New(
					errs.KindStateConflict,
					"Volume consumer projection is incomplete",
				)
			}
			result = append(result, backupruntime.BackupVolumeServiceSnapshot{
				ServiceID: service.Record.Desired.ID, ServiceRevision: servicerecord.ServiceRuntimeRevision(service),
				ComposeKey: composeKey, MountPaths: mountPaths,
				PriorIntent: backupruntime.BackupServiceRuntimeIntent(service.Record.Runtime.RuntimeIntent),
			})
		}
	}
	return result, nil
}

func backupVolumeTargetsGeneratedService(components []componentrecord.Record, serviceID string) bool {
	for _, component := range components {
		for _, generatedServiceID := range component.Runtime.GeneratedServices {
			if generatedServiceID == serviceID {
				return true
			}
		}
	}
	return false
}
