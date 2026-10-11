package etcd

import (
	"bytes"
	"context"
	"encoding/hex"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	environmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
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
	revisionID     string
	zones          map[string]zonerecord.Record
	runtime        componentTaskRuntimeAuthority
	child          *etcdstore.Versioned[componentplanning.ChildExecution]
	requireRootCAS bool
}

func componentTaskBlueprintChildAuthority(
	ctx context.Context,
	store taskRepositoryStore,
	task TaskRecord,
	intent environmentchanges.ComponentTaskIntent,
	zones []string,
	revision int64,
) (componentTaskAcknowledgementAuthority, error) {
	componentID := task.Params[taskjournal.TaskBlueprintComponentUnitParam]
	parentID := task.Params[taskjournal.TaskBlueprintParentParam]
	if componentID == "" {
		return componentTaskAcknowledgementAuthority{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint Component child identity is missing",
		)
	}
	if !taskjournal.IsBlueprintChild(task.Params) || ids.Validate(ids.KindComponent, componentID) != nil ||
		ids.Validate(ids.KindTask, parentID) != nil ||
		task.Params[blueprints.EnvironmentDesiredRevisionParam] != parentID ||
		task.Owner.EnvironmentID != intent.EnvironmentID || task.Target != intent.EnvironmentID ||
		len(intent.Candidates) != 1 || intent.Candidates[0].Candidate.Desired.ID != componentID {
		return componentTaskAcknowledgementAuthority{}, errs.New(
			errs.KindStateConflict,
			"Blueprint Component child differs from its candidate",
		)
	}
	stored, err := componentplanning.ReadChildExecution(ctx, store, task.ID, revision)
	if err != nil {
		return componentTaskAcknowledgementAuthority{}, err
	}
	plan, err := stored.Record.ExecutionPlan()
	if err != nil {
		return componentTaskAcknowledgementAuthority{}, err
	}
	if stored.ReadRevision != revision || stored.Record.ParentTaskID != parentID ||
		stored.Record.EnvironmentID != intent.EnvironmentID ||
		!componentrecord.EqualRecord(stored.Record.Component, intent.Candidates[0].Candidate) ||
		plan.GetPlanId() != task.PlanID || hex.EncodeToString(plan.GetPlanHash()) != task.PlanHash ||
		plan.GetRenderGeneration() != uint64(task.RenderGeneration) || plan.GetTargetId() != task.Target {
		return componentTaskAcknowledgementAuthority{}, errs.New(
			errs.KindStateConflict,
			"Blueprint Component child execution authority changed",
		)
	}
	projected := make(map[string]zonerecord.Record, len(stored.Record.Zones))
	runtimeZones := make([]zonerecord.Record, len(stored.Record.Zones))
	copy(runtimeZones, stored.Record.Zones)
	for _, zone := range runtimeZones {
		projected[zone.Desired.ID] = zone
	}
	result := make(map[string]zonerecord.Record, len(zones))
	for _, zoneID := range zones {
		zone, ok := projected[zoneID]
		if !ok {
			return componentTaskAcknowledgementAuthority{}, errs.New(
				errs.KindStateConflict,
				"Blueprint Component child Zone is absent from its execution authority",
			)
		}
		result[zoneID] = zone
	}
	artifact, err := componentTaskCandidateArtifact(plan, stored.Record.Component)
	if err != nil {
		return componentTaskAcknowledgementAuthority{}, err
	}
	return componentTaskAcknowledgementAuthority{
		revisionID: parentID,
		zones:      result,
		runtime: componentTaskRuntimeAuthority{
			artifact: artifact,
			zones:    runtimeZones,
		},
		child:          &stored,
		requireRootCAS: true,
	}, nil
}

func componentTaskCandidateArtifact(
	plan *agentpb.ExecutionPlan,
	candidate componentrecord.Record,
) ([]byte, error) {
	if !candidate.Desired.Enabled {
		if len(candidate.Runtime.GeneratedServices) != 0 {
			return nil, errs.New(errs.KindStateConflict, "disabled Component child has generated Services")
		}
		return nil, nil
	}
	if len(candidate.Runtime.GeneratedServices) != 1 {
		return nil, errs.New(errs.KindStateConflict, "enabled Component child has no generated Service")
	}
	serviceID := candidate.Runtime.GeneratedServices[0]
	artifactID := ""
	for _, step := range plan.GetSteps() {
		apply := step.GetComposeApply()
		if apply == nil || len(apply.GetServiceIds()) != 1 || apply.GetServiceIds()[0] != serviceID {
			continue
		}
		if artifactID != "" {
			return nil, errs.New(errs.KindStateConflict, "Component child candidate Apply is ambiguous")
		}
		artifactID = apply.GetArtifactId()
	}
	if artifactID == "" {
		return nil, errs.New(errs.KindStateConflict, "Component child candidate Apply is unavailable")
	}
	var selected *agentpb.ComposeArtifact
	for _, artifact := range plan.GetArtifacts() {
		if artifact.GetArtifactId() != artifactID {
			continue
		}
		if selected != nil {
			return nil, errs.New(errs.KindStateConflict, "Component child candidate artifact is ambiguous")
		}
		selected = artifact
	}
	if selected == nil {
		return nil, errs.New(errs.KindStateConflict, "Component child candidate artifact is unavailable")
	}
	value, err := (proto.MarshalOptions{Deterministic: true}).Marshal(selected)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return value, nil
}

func clearComponentTaskChange(change componentTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}

func componentTaskActiveValueMatches(value *etcdstore.KeyValue, taskID string) bool {
	return value != nil && bytes.Equal(value.Value, []byte(taskID))
}
