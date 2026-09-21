package etcd

import (
	"bytes"
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	environmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type componentTaskChange struct {
	applies    bool
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

const (
	componentTaskBlueprintProcedureParam             = "blueprint_compose_procedure"
	componentTaskBlueprintProcedureNone              = "none"
	componentTaskBlueprintProcedureFullReconcile     = "full-reconcile"
	componentTaskBlueprintProcedureCandidateReleases = "candidate-releases"
)

func componentTaskDesiredProjectionZones(
	ctx context.Context,
	store taskRepositoryStore,
	task TaskRecord,
	intent environmentchanges.ComponentTaskIntent,
	zones []string,
) (string, map[string]zonerecord.Record, error) {
	desiredRevisionID := task.Params[blueprints.EnvironmentDesiredRevisionParam]
	if ids.Validate(ids.KindTask, desiredRevisionID) != nil {
		return "", nil, errs.New(errs.KindStateConflict, "Component Task desired revision is invalid")
	}
	hierarchy := composeHierarchyRepository(store)
	projection, found, err := hierarchy.GetEnvironmentComposeProjectionRevision(
		ctx, intent.EnvironmentID, desiredRevisionID,
	)
	if err != nil {
		return "", nil, err
	}
	if !found || projection.Record.EnvironmentID != intent.EnvironmentID ||
		projection.Record.RevisionID != desiredRevisionID ||
		projection.Record.RenderGeneration != uint64(task.RenderGeneration) {
		return "", nil, errs.New(
			errs.KindStateConflict,
			"Component Task desired projection changed",
		)
	}
	projected := make(map[string]zonerecord.Record, len(projection.Record.DesiredZones))
	for _, desired := range projection.Record.DesiredZones {
		zone, joinErr := environmentqueries.JoinZone(projection, desired)
		if joinErr != nil {
			return "", nil, joinErr
		}
		projected[zone.Record.Desired.ID] = zone.Record
	}
	result := make(map[string]zonerecord.Record, len(zones))
	for _, zoneID := range zones {
		zone, ok := projected[zoneID]
		if !ok {
			return "", nil, errs.New(errs.KindStateConflict, "Component Task Zone is not in its desired projection")
		}
		result[zoneID] = zone
	}
	return desiredRevisionID, result, nil
}

func clearComponentTaskChange(change componentTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}

func componentTaskActiveValueMatches(value *etcdstore.KeyValue, taskID string) bool {
	return value != nil && bytes.Equal(value.Value, []byte(taskID))
}
