package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func projectedZoneRemovalTarget(
	intent environmentchanges.ZoneRemovalIntent,
	readRevision int64,
) (zonerecord.Record, error) {
	source := intent.DesiredProjection
	sourceRevision := intent.DesiredHeadRevision
	if intent.Authored != nil {
		source = intent.AppliedProjection
		sourceRevision = intent.AppliedProjectionRevision
	}
	projection := etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
		Record: source, Revision: sourceRevision, ReadRevision: readRevision,
	}
	var matched *zonerecord.Record
	for _, desired := range source.DesiredZones {
		if desired.Desired.ID != intent.ZoneID {
			continue
		}
		if matched != nil {
			return zonerecord.Record{}, errs.New(errs.KindInternal, "Zone removal projection has duplicate target")
		}
		joined, err := environmentqueries.JoinZone(projection, desired)
		if err != nil {
			return zonerecord.Record{}, err
		}
		matched = &joined.Record
	}
	if matched == nil || matched.EnvironmentID != intent.EnvironmentID || matched.Desired.Name != intent.ZoneName ||
		intent.ZoneRevision != sourceRevision {
		return zonerecord.Record{}, errs.New(errs.KindStateConflict, "Zone removal projected target changed")
	}
	return *matched, nil
}

func zoneRemovalTaskIdentity(task TaskRecord) (string, string, error) {
	environmentID := task.Params[taskjournal.TaskZoneEnvironmentParam]
	operationID := task.Params[taskjournal.TaskZoneRemovalOperationParam]
	if task.Type != taskjournal.TaskRemove || ids.Validate(ids.KindNetwork, task.Target) != nil ||
		ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindOperation, operationID) != nil {
		return "", "", errs.New(errs.KindInternal, "durable Zone removal Task shape is invalid")
	}
	return environmentID, operationID, nil
}
