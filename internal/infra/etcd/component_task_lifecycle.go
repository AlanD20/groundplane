package etcd

import (
	"bytes"
	"context"
	"sort"

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
) (componentTaskAcknowledgementAuthority, error) {
	desiredRevisionID := task.Params[blueprints.EnvironmentDesiredRevisionParam]
	if ids.Validate(ids.KindTask, desiredRevisionID) != nil {
		return componentTaskAcknowledgementAuthority{}, errs.New(
			errs.KindStateConflict,
			"Component Task desired revision is invalid",
		)
	}
	hierarchy := composeHierarchyRepository(store)
	projection, found, err := hierarchy.GetEnvironmentComposeProjectionRevision(
		ctx, intent.EnvironmentID, desiredRevisionID,
	)
	if err != nil {
		return componentTaskAcknowledgementAuthority{}, err
	}
	if !found || projection.Record.EnvironmentID != intent.EnvironmentID ||
		projection.Record.RevisionID != desiredRevisionID ||
		projection.Record.RenderGeneration != uint64(task.RenderGeneration) {
		return componentTaskAcknowledgementAuthority{}, errs.New(
			errs.KindStateConflict,
			"Component Task desired projection changed",
		)
	}
	projected := make(map[string]zonerecord.Record, len(projection.Record.DesiredZones))
	runtimeZones := make([]zonerecord.Record, 0, len(projection.Record.DesiredZones))
	for _, desired := range projection.Record.DesiredZones {
		zone, joinErr := environmentqueries.JoinZone(projection, desired)
		if joinErr != nil {
			return componentTaskAcknowledgementAuthority{}, joinErr
		}
		projected[zone.Record.Desired.ID] = zone.Record
		runtimeZones = append(runtimeZones, zone.Record)
	}
	sort.Slice(runtimeZones, func(left, right int) bool {
		return runtimeZones[left].Desired.ID < runtimeZones[right].Desired.ID
	})
	result := make(map[string]zonerecord.Record, len(zones))
	for _, zoneID := range zones {
		zone, ok := projected[zoneID]
		if !ok {
			return componentTaskAcknowledgementAuthority{}, errs.New(
				errs.KindStateConflict,
				"Component Task Zone is not in its desired projection",
			)
		}
		result[zoneID] = zone
	}
	return componentTaskAcknowledgementAuthority{
		revisionID: desiredRevisionID,
		zones:      result,
		runtime: componentTaskRuntimeAuthority{
			artifact: append([]byte(nil), projection.Record.ComposeArtifact...),
			zones:    runtimeZones,
		},
	}, nil
}

type componentTaskRuntimeAuthority struct {
	artifact []byte
	zones    []zonerecord.Record
}

type componentTaskAcknowledgementAuthority struct {
	revisionID string
	zones      map[string]zonerecord.Record
	runtime    componentTaskRuntimeAuthority
}

func clearComponentTaskChange(change componentTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}

func componentTaskActiveValueMatches(value *etcdstore.KeyValue, taskID string) bool {
	return value != nil && bytes.Equal(value.Value, []byte(taskID))
}
