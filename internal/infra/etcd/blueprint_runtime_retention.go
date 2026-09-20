package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"slices"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type blueprintRuntimeRetention struct {
	sourceRevision     int64
	sourceReadRevision int64
	sourceSHA          [32]byte
	mixedSHA           [32]byte
	conditions         []etcdstore.Condition
	hasNative          bool
}

// PrepareBlueprintRuntimeRetention creates a source-only publication fragment.
// Its source is rechecked at the captured revision; final publication compares
// that exact applied-key revision without updating native applied authority.
func (ledger *ReleaseLedger) PrepareBlueprintRuntimeRetention(
	ctx context.Context,
	publication BlueprintReleasePublication,
	task TaskRecord,
	captured Versioned[EnvironmentComposeProjection],
	sources []BlueprintRetainedRuntimeSource,
	mixed *agentpb.ComposeArtifact,
) (BlueprintReleasePublication, error) {
	environmentID := task.Target
	if ctx == nil || ledger == nil || ids.Validate(ids.KindEnvironment, environmentID) != nil || mixed == nil ||
		mixed.OwnerId != environmentID || captured.Revision < 0 || captured.ReadRevision <= 0 || captured.ReadRevision < captured.Revision || len(sources) == 0 {
		return BlueprintReleasePublication{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint retained runtime source is invalid",
		)
	}
	scope := ReleasePlanningScope{
		ReadRevision: captured.ReadRevision,
		Environment:  Versioned[hierarchyrecord.EnvironmentRecord]{Record: hierarchyrecord.EnvironmentRecord{ID: environmentID}},
	}
	actual, found, err := ledger.GetPlanningAppliedProjection(ctx, scope)
	if err != nil {
		return BlueprintReleasePublication{}, err
	}
	if found != (captured.Revision > 0) || actual.Revision != captured.Revision ||
		!bytes.Equal(actual.Record.ComposeArtifact, captured.Record.ComposeArtifact) {
		return BlueprintReleasePublication{}, errs.New(
			errs.KindStateConflict,
			"Blueprint retained runtime source changed",
		)
	}
	value, err := (proto.MarshalOptions{Deterministic: true}).Marshal(mixed)
	if err != nil {
		return BlueprintReleasePublication{}, errs.Wrap(errs.KindInternal, err)
	}
	planning := make([]ReleasePlanningService, len(sources))
	for index, source := range sources {
		planning[index] = source.Planning
	}
	conditions, hasNative, err := ledger.blueprintRuntimePlanningConditions(
		ctx,
		environmentID,
		captured.ReadRevision,
		planning,
	)
	if err != nil {
		return BlueprintReleasePublication{}, err
	}
	sourceConditions, err := ledger.blueprintRuntimeSourceConditions(ctx, environmentID, captured.ReadRevision, sources)
	if err != nil {
		return BlueprintReleasePublication{}, err
	}
	conditions = append(conditions, sourceConditions...)
	if hasNative && captured.Revision == 0 {
		return BlueprintReleasePublication{}, errs.New(
			errs.KindStateConflict,
			"Blueprint retained serving runtime has no applied source",
		)
	}
	conditions = append(
		[]etcdstore.Condition{{Key: environmentComposeProjectionKey(environmentID), ModRevision: captured.Revision}},
		conditions...)
	if !publication.IsZero() {
		if err := publication.validate(environmentID, task); err != nil {
			return BlueprintReleasePublication{}, err
		}
		fragment, err := (BlueprintReleasePublication{conditions: conditions}).withExistingComparisons(
			publication.conditions,
		)
		if err != nil {
			return BlueprintReleasePublication{}, err
		}
		publication.conditions = append(slices.Clone(publication.conditions), fragment.conditions...)
		if hasNative {
			publication.retained = &blueprintRuntimeRetention{sourceRevision: captured.Revision,
				sourceReadRevision: captured.ReadRevision, conditions: slices.Clone(conditions), hasNative: true,
				sourceSHA: sha256.Sum256(captured.Record.ComposeArtifact), mixedSHA: sha256.Sum256(value)}
		}
		if err := publication.validate(environmentID, task); err != nil {
			return BlueprintReleasePublication{}, err
		}
		return publication, nil
	}
	publication = BlueprintReleasePublication{environmentID: environmentID, operationID: task.OperationID,
		conditions: slices.Clone(conditions),
		retained: &blueprintRuntimeRetention{
			sourceRevision:     captured.Revision,
			sourceReadRevision: captured.ReadRevision,
			conditions:         slices.Clone(conditions),
			hasNative:          hasNative,
			sourceSHA:          sha256.Sum256(captured.Record.ComposeArtifact),
			mixedSHA:           sha256.Sum256(value),
		}}
	if err := publication.validate(environmentID, task); err != nil {
		return BlueprintReleasePublication{}, err
	}
	return publication, nil
}

func (ledger *ReleaseLedger) blueprintRuntimePlanningConditions(
	ctx context.Context,
	environmentID string,
	readRevision int64,
	captured []ReleasePlanningService,
) ([]etcdstore.Condition, bool, error) {
	scope, err := ledger.LoadPlanningScopeAtRevision(ctx, environmentID, readRevision)
	if err != nil {
		return nil, false, err
	}
	serviceIDs := make([]string, len(captured))
	for index, source := range captured {
		serviceIDs[index] = source.Service.Record.Desired.ID
		if source.Service.ReadRevision != readRevision || index > 0 && serviceIDs[index-1] >= serviceIDs[index] {
			return nil, false, errs.New(
				errs.KindValidationFailed,
				"Blueprint retained Release sources are not canonical",
			)
		}
	}
	actual, err := ledger.LoadPlanningServices(ctx, scope, serviceIDs)
	if err != nil {
		return nil, false, err
	}
	conditions := make([]etcdstore.Condition, len(captured))
	hasNative := false
	for index, source := range captured {
		if actual[index].ProjectionRevision != source.ProjectionRevision ||
			actual[index].Projection != source.Projection {
			return nil, false, errs.New(errs.KindStateConflict, "Blueprint retained Release source changed")
		}
		conditions[index] = etcdstore.Condition{
			Key:         releaseProjectionKey(serviceIDs[index]),
			ModRevision: source.ProjectionRevision,
		}
		hasNative = hasNative || source.Projection.ServingReleaseID != ""
	}
	return conditions, hasNative, nil
}

func (publication BlueprintReleasePublication) validateRetainedRuntime(
	task TaskRecord,
	projection EnvironmentComposeProjection,
) error {
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(projection.ComposeArtifact, artifact) != nil {
		return errs.New(errs.KindValidationFailed, "Blueprint runtime artifact is invalid")
	}
	historical := false
	for _, service := range artifact.Services {
		if service.OwnerComponentId != "" ||
			service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED {
			continue
		}
		for _, label := range service.ExpectedLabels {
			if label.Key == "com.groundplane.plan-id" && label.Value != task.PlanID ||
				label.Key == "com.groundplane.render-generation" &&
					label.Value != strconv.FormatInt(int64(task.RenderGeneration), 10) {
				historical = true
			}
		}
	}
	if historical != (publication.retained != nil && publication.retained.hasNative) {
		return errs.New(
			errs.KindValidationFailed,
			"Blueprint retained runtime source authority is absent or unexpected",
		)
	}
	if publication.retained != nil &&
		(publication.retained.mixedSHA != sha256.Sum256(projection.ComposeArtifact) || publication.retained.sourceSHA == [32]byte{}) {
		return errs.New(errs.KindValidationFailed, "Blueprint retained runtime artifact changed")
	}
	return nil
}
