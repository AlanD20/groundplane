package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releasequeries "github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintRetainedRuntimeSource binds a planning projection to the exact
// serving Release and acknowledged runtime used by Blueprint publication.
type BlueprintRetainedRuntimeSource struct {
	Planning releasequeries.ReleasePlanningService
	Release  *releaserender.ServiceLifecycleRelease
	Intent   *releasequeries.ServingRelease
	Runtime  *etcdstore.Versioned[serviceruntimerecord.Record]
}

func (ledger *ReleaseLedger) blueprintRuntimeSourceConditions(
	ctx context.Context,
	environmentID string,
	revision int64,
	sources []BlueprintRetainedRuntimeSource,
) ([]etcdstore.Condition, error) {
	var conditions []etcdstore.Condition
	for _, source := range sources {
		if source.Planning.Projection.ServingReleaseID == "" {
			if source.Release != nil || source.Intent != nil || source.Runtime != nil {
				return nil, errs.New(errs.KindValidationFailed, "Blueprint retained absent source has native authority")
			}
			continue
		}
		serviceID := source.Planning.Service.Record.Desired.ID
		if source.Release == nil || source.Intent == nil ||
			releaserender.ValidateServiceLifecycleRelease(
				*source.Release,
				releaserender.ServiceLifecycleRenderInput{ServiceID: serviceID, EnvironmentID: environmentID},
			) != nil {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint retained native authority is invalid")
		}
		authority := source.Release
		serving, err := ledger.ResolveServing(ctx, environmentID, serviceID, revision)
		if err != nil {
			return nil, err
		}
		actual, err := json.Marshal(serving)
		if err != nil {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
		captured, err := json.Marshal(source.Intent)
		if err != nil {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
		if !bytes.Equal(actual, captured) || serving.Intent.ID != authority.ServingReleaseID ||
			serving.ProjectionRevision != authority.ProjectionRevision ||
			serving.IntentRevision != authority.IntentRevision ||
			serving.Projection != source.Planning.Projection ||
			serving.ProjectionRevision != source.Planning.ProjectionRevision {
			return nil, errs.New(errs.KindStateConflict, "Blueprint retained serving source changed")
		}
		runtimeCondition, err := ledger.blueprintAcknowledgedRuntimeCondition(ctx, environmentID, revision, source)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, runtimeCondition)
		current, err := ledger.GetReleaseRenderInputAt(ctx, authority.ServingReleaseID, revision)
		if err != nil {
			return nil, err
		}
		value, err := releaserender.EncodeReleaseRenderInput(current.Record)
		if err != nil {
			return nil, err
		}
		wanted, err := releaserender.EncodeReleaseRenderInput(authority.Current)
		if err != nil {
			return nil, err
		}
		digest, err := domain.Digest(json.RawMessage(value))
		if err != nil {
			return nil, err
		}
		if current.Revision != authority.RenderRevision || !bytes.Equal(value, wanted) ||
			digest != serving.Intent.RenderInputDigest ||
			current.Record.CandidateWorkload != serving.Intent.CandidateWorkload ||
			current.Record.Strategy != serving.Intent.Strategy ||
			current.Record.Slot != serving.Intent.Slot {
			return nil, errs.New(errs.KindStateConflict, "Blueprint retained render source changed")
		}
		conditions = append(
			conditions,
			etcdstore.Condition{
				Key:         releases.ReleaseIntentStagingKey("", authority.ServingReleaseID),
				ModRevision: authority.IntentRevision,
			},
			etcdstore.Condition{
				Key:         releases.ReleaseRenderInputStagingKey("", authority.ServingReleaseID),
				ModRevision: authority.RenderRevision,
			},
		)
		needsPrior := current.Record.Strategy == domain.StrategyBlueGreen &&
			current.Record.PriorStrategy == domain.StrategyBlueGreen &&
			serving.Intent.PriorServingReleaseID != ""
		if needsPrior != (authority.RetainedPrior != nil) {
			return nil, errs.New(errs.KindStateConflict, "Blueprint retained inactive source is absent or unexpected")
		}
		if !needsPrior {
			continue
		}
		if authority.PriorServingReleaseID != serving.Intent.PriorServingReleaseID {
			return nil, errs.New(errs.KindStateConflict, "Blueprint retained inactive Release changed")
		}
		prior, err := ledger.GetReleaseRenderInputAt(ctx, authority.PriorServingReleaseID, revision)
		if err != nil {
			return nil, err
		}
		value, err = releaserender.EncodeReleaseRenderInput(prior.Record)
		if err != nil {
			return nil, err
		}
		wanted, err = releaserender.EncodeReleaseRenderInput(*authority.RetainedPrior)
		if err != nil {
			return nil, err
		}
		if prior.Revision != authority.RetainedPriorRenderRevision || !bytes.Equal(value, wanted) {
			return nil, errs.New(errs.KindStateConflict, "Blueprint retained inactive render source changed")
		}
		conditions = append(
			conditions,
			etcdstore.Condition{
				Key:         releases.ReleaseRenderInputStagingKey("", authority.PriorServingReleaseID),
				ModRevision: authority.RetainedPriorRenderRevision,
			},
		)
	}
	return conditions, nil
}

func (ledger *ReleaseLedger) blueprintAcknowledgedRuntimeCondition(
	ctx context.Context,
	environmentID string,
	revision int64,
	source BlueprintRetainedRuntimeSource,
) (etcdstore.Condition, error) {
	serviceID := source.Planning.Service.Record.Desired.ID
	if source.Runtime == nil || source.Runtime.Revision <= 0 || source.Runtime.Revision > revision ||
		source.Runtime.ReadRevision != revision || serviceruntimerecord.Validate(source.Runtime.Record) != nil ||
		source.Runtime.Record.EnvironmentID != environmentID || source.Runtime.Record.Runtime.ServiceID != serviceID ||
		source.Runtime.Record.Runtime.ReleaseID != source.Release.ServingReleaseID ||
		source.Runtime.Record.Runtime.Target != string(source.Release.Current.CandidateTarget) {
		return etcdstore.Condition{}, errs.New(
			errs.KindStateConflict,
			"Blueprint retained acknowledged runtime is unavailable or changed",
		)
	}
	actual, err := ledger.LoadAcknowledgedServiceRuntimesAtRevision(ctx, environmentID, []string{serviceID}, revision)
	if err != nil {
		return etcdstore.Condition{}, err
	}
	if len(actual) != 1 || actual[0].Revision != source.Runtime.Revision {
		return etcdstore.Condition{}, errs.New(errs.KindStateConflict, "Blueprint retained runtime revision changed")
	}
	got, want := actual[0].Record.Runtime, source.Runtime.Record.Runtime
	if got.ReleaseID != want.ReleaseID || got.Target != want.Target || got.ProxyGeneration != want.ProxyGeneration ||
		!bytes.Equal(got.CurrentArtifact, want.CurrentArtifact) ||
		!bytes.Equal(got.RetainedPriorArtifact, want.RetainedPriorArtifact) ||
		!bytes.Equal(got.ProxyConfigSHA256, want.ProxyConfigSHA256) {
		return etcdstore.Condition{}, errs.New(
			errs.KindStateConflict,
			"Blueprint retained acknowledged runtime changed",
		)
	}
	return etcdstore.Condition{Key: serviceruntimerecord.Key(serviceID), ModRevision: source.Runtime.Revision}, nil
}
