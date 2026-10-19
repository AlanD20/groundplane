package etcd

import (
	"bytes"
	"context"
	"time"

	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	scriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	scriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// BlueprintNativePredecessorCapture is transient fixed-revision publication
// evidence. Historical render inputs are not duplicated in the marker.
type BlueprintNativePredecessorCapture struct {
	ServiceID              string                                 `json:"service_id"`
	FixedReadRevision      int64                                  `json:"fixed_read_revision"`
	ProjectionRevision     int64                                  `json:"projection_revision"`
	RuntimeRevision        int64                                  `json:"runtime_revision,omitempty"`
	Serving                *releaserender.ServiceLifecycleRelease `json:"serving,omitempty"`
	RetainedPriorReleaseID string                                 `json:"retained_prior_release_id,omitempty"`
	CurrentArtifact        []byte                                 `json:"current_artifact,omitempty"`
	RetainedPriorArtifact  []byte                                 `json:"retained_prior_artifact,omitempty"`
}

// BlueprintNativePredecessor is the immutable runtime view reconstructed from
// the marker's reference and its candidate's digest-bound staged render input.
type BlueprintNativePredecessor struct {
	ServiceID             string
	FixedReadRevision     int64
	ProjectionRevision    int64
	RuntimeRevision       int64
	Serving               *releases.BlueprintNativeServingPredecessor
	CurrentArtifact       []byte
	RetainedPriorArtifact []byte
}

func (capture BlueprintNativePredecessorCapture) Runtime() BlueprintNativePredecessor {
	runtime := BlueprintNativePredecessor{
		ServiceID: capture.ServiceID, FixedReadRevision: capture.FixedReadRevision,
		ProjectionRevision: capture.ProjectionRevision, RuntimeRevision: capture.RuntimeRevision,
		CurrentArtifact:       capture.CurrentArtifact,
		RetainedPriorArtifact: capture.RetainedPriorArtifact,
	}
	if capture.Serving != nil {
		runtime.Serving = &releases.BlueprintNativeServingPredecessor{
			ServingReleaseID: capture.Serving.ServingReleaseID, Target: capture.Serving.Current.CandidateTarget,
			RetainedPriorReleaseID: capture.RetainedPriorReleaseID,
		}
	}
	return runtime
}

type ReleaseWorkPublicationEvidence struct {
	StorageConditions  []etcdstore.Condition
	RenderingInput     projectionrecord.EnvironmentComposeProjection
	ServiceRollout     *etcdstore.Versioned[releaserender.ServiceRolloutCapture]
	ProxyAddresses     networkreservations.ProxyAddresses
	NativePredecessors []BlueprintNativePredecessorCapture
	Manifest           releases.VersionedReleaseManifest
	EnvironmentID      string
	Work               workqueue.View
	PreparedInput      PreparedReleaseWorkInput
	HookWork           ReleaseHookWorkPublication
	PublishedAt        time.Time
	SourcePrepared     scriptsourcepublication.PreparedSourceSet
	SourceMembers      []scriptsourceevidence.ScriptSourcePreparationMember
	HookPrepared       PreparedReleaseWorkHooks
}

func (ledger *ReleaseLedger) blueprintNativePredecessorConditions(
	ctx context.Context,
	evidence ReleaseWorkPublicationEvidence,
) ([]etcdstore.Condition, error) {
	if len(evidence.NativePredecessors) > 0 && evidence.RenderingInput.EnvironmentID != evidence.EnvironmentID {
		return nil, errs.New(errs.KindStateConflict, "Blueprint predecessor rendering input changed")
	}
	member, conditions, err := ledger.releaseWorkManifestMember(ctx, evidence)
	if err != nil {
		return nil, err
	}
	if len(evidence.NativePredecessors) != 1 || evidence.NativePredecessors[0].ServiceID != member.Render.ServiceID {
		return nil, errs.New(
			errs.KindStateConflict,
			"Blueprint Release requires its selected Service predecessor or explicit absence",
		)
	}
	for _, capture := range evidence.NativePredecessors {
		if capture.FixedReadRevision <= 0 || capture.ProjectionRevision < 0 ||
			capture.ProjectionRevision > capture.FixedReadRevision || capture.RuntimeRevision < 0 ||
			capture.RuntimeRevision > capture.FixedReadRevision {
			return nil, releases.CorruptReleaseRecord()
		}
		if capture.Serving == nil && (capture.RetainedPriorReleaseID != "" || capture.RuntimeRevision != 0 ||
			len(capture.CurrentArtifact) != 0 || len(capture.RetainedPriorArtifact) != 0) {
			return nil, releases.CorruptReleaseRecord()
		}
		if capture.Serving != nil && (releaserender.ValidateServiceLifecycleRelease(*capture.Serving,
			releaserender.ServiceLifecycleRenderInput{ServiceID: capture.ServiceID, EnvironmentID: evidence.EnvironmentID}) != nil ||
			capture.Serving.ProjectionRevision != capture.ProjectionRevision) {
			return nil, releases.CorruptReleaseRecord()
		}
	}
	references, err := blueprintNativePredecessorReferences(evidence.NativePredecessors)
	if err != nil {
		return nil, err
	}
	_, err = resolveBlueprintNativePredecessors(references, []releaserender.ReleaseTaskRenderMember{member})
	if err != nil {
		return nil, err
	}
	for _, captured := range evidence.NativePredecessors {
		if captured.Serving == nil && captured.ProjectionRevision == 0 {
			keys := []string{
				releases.ReleaseProjectionKey(captured.ServiceID),
				serviceruntimerecord.Key(captured.ServiceID),
			}
			read, err := ledger.store.GetMany(
				ctx,
				etcdstore.GetManyRequest{Keys: keys, Revision: captured.FixedReadRevision},
			)
			if err != nil {
				return nil, err
			}
			if read == nil || read.ReadRevision != captured.FixedReadRevision || len(read.Values) != len(keys) {
				return nil, releases.CorruptReleaseRecord()
			}
			absent := read.Values[0] == nil && read.Values[1] == nil
			etcdstore.ClearValues(read.Values)
			if !absent {
				return nil, errs.New(
					errs.KindStateConflict,
					"Blueprint first deployment found existing Service runtime",
				)
			}
			conditions = append(conditions, etcdstore.Condition{Key: keys[0]}, etcdstore.Condition{Key: keys[1]})
			continue
		}
		scope, err := ledger.LoadPlanningScopeForCapturedInput(ctx, evidence.RenderingInput, captured.FixedReadRevision)
		if err != nil {
			return nil, err
		}
		planning, err := ledger.LoadPlanningServices(ctx, scope, []string{captured.ServiceID})
		if err != nil {
			return nil, err
		}
		if len(planning) != 1 || planning[0].ProjectionRevision != captured.ProjectionRevision ||
			(planning[0].Projection.ServingReleaseID == "") != (captured.Serving == nil) {
			return nil, errs.New(errs.KindStateConflict, "Blueprint native predecessor projection changed")
		}
		source := BlueprintRetainedRuntimeSource{Planning: planning[0], Release: captured.Serving}
		if captured.Serving != nil {
			serving, err := ledger.ResolveServing(
				ctx,
				evidence.EnvironmentID,
				captured.ServiceID,
				captured.FixedReadRevision,
			)
			if err != nil {
				return nil, err
			}
			source.Intent = &serving
			runtimes, err := ledger.LoadAcknowledgedServiceRuntimesAtRevision(
				ctx, evidence.EnvironmentID, []string{captured.ServiceID}, captured.FixedReadRevision,
			)
			if err != nil {
				return nil, err
			}
			if len(runtimes) != 1 || runtimes[0].Revision != captured.RuntimeRevision {
				return nil, errs.New(errs.KindStateConflict, "Blueprint native predecessor runtime changed")
			}
			source.Runtime = &runtimes[0]
		}
		guards, err := ledger.blueprintRuntimeSourceConditions(
			ctx,
			evidence.EnvironmentID,
			captured.FixedReadRevision,
			[]BlueprintRetainedRuntimeSource{source},
		)
		if err != nil {
			return nil, err
		}
		applied, _, err := ledger.GetPlanningAppliedProjection(ctx, scope)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, guards...)
		conditions = append(
			conditions,
			etcdstore.Condition{
				Key:         releases.ReleaseProjectionKey(captured.ServiceID),
				ModRevision: captured.ProjectionRevision,
			},
			etcdstore.Condition{
				Key:         projectionrecord.EnvironmentComposeProjectionStorageKey(evidence.EnvironmentID),
				ModRevision: applied.Revision,
			},
		)
		if captured.Serving == nil {
			if captured.RuntimeRevision != 0 || len(captured.CurrentArtifact) != 0 ||
				len(captured.RetainedPriorArtifact) != 0 {
				return nil, releases.CorruptReleaseRecord()
			}
			conditions = append(conditions, etcdstore.Condition{Key: serviceruntimerecord.Key(captured.ServiceID)})
		} else if source.Runtime == nil ||
			!bytes.Equal(source.Runtime.Record.Runtime.CurrentArtifact, captured.CurrentArtifact) ||
			!bytes.Equal(source.Runtime.Record.Runtime.RetainedPriorArtifact, captured.RetainedPriorArtifact) {
			return nil, errs.New(errs.KindStateConflict, "Blueprint predecessor bytes differ from the acknowledged Service runtime")
		}
	}
	return conditions, nil
}

func validateBlueprintNativeIntent(intent domain.Intent, snapshots []BlueprintNativePredecessor) error {
	expected := ""
	for _, snapshot := range snapshots {
		if snapshot.ServiceID == intent.ServiceID && snapshot.Serving != nil {
			expected = snapshot.Serving.ServingReleaseID
		}
	}
	if intent.PriorServingReleaseID != expected {
		return releases.CorruptReleaseRecord()
	}
	return nil
}

func validateBlueprintNativePredecessors(
	snapshots []BlueprintNativePredecessor,
	procedure *agentpb.CandidateReleaseProcedure,
	environmentID string,
) error {
	byService := make(map[string]BlueprintNativePredecessor, len(snapshots))
	for index, captured := range snapshots {
		if ids.Validate(ids.KindService, captured.ServiceID) != nil || captured.FixedReadRevision <= 0 ||
			captured.ProjectionRevision < 0 ||
			captured.ProjectionRevision > captured.FixedReadRevision ||
			index > 0 && snapshots[index-1].ServiceID >= captured.ServiceID ||
			index > 0 && snapshots[index-1].FixedReadRevision != captured.FixedReadRevision {
			return releases.CorruptReleaseRecord()
		}
		if captured.Serving == nil {
			if captured.RuntimeRevision != 0 || len(captured.CurrentArtifact) != 0 ||
				len(captured.RetainedPriorArtifact) != 0 {
				return releases.CorruptReleaseRecord()
			}
		} else if ids.Validate(ids.KindDeployment, captured.Serving.ServingReleaseID) != nil ||
			captured.ProjectionRevision <= 0 || captured.RuntimeRevision <= 0 ||
			captured.RuntimeRevision > captured.FixedReadRevision || captured.Serving.Target.Validate() != nil ||
			len(captured.CurrentArtifact) == 0 ||
			(captured.Serving.RetainedPriorReleaseID != "") != (len(captured.RetainedPriorArtifact) != 0) ||
			captured.Serving.RetainedPriorReleaseID != "" && ids.Validate(ids.KindDeployment, captured.Serving.RetainedPriorReleaseID) != nil ||
			executionplan.ValidateNativePredecessorWitness(environmentID, captured.ServiceID, captured.CurrentArtifact, captured.RetainedPriorArtifact) != nil {
			return releases.CorruptReleaseRecord()
		}
		byService[captured.ServiceID] = captured
	}
	for _, member := range procedure.GetMembers() {
		captured, found := byService[member.GetServiceId()]
		delete(byService, member.GetServiceId())
		prior := member.GetServingPredecessor()
		if !found || captured.Serving == nil {
			if prior.GetPriorArtifactId() != "" || prior.GetPriorReleaseId() != "" || prior.GetPriorTarget() != "" ||
				prior.GetRetainedPriorArtifactId() != "" {
				return releases.CorruptReleaseRecord()
			}
			continue
		}
		artifact, err := executionplan.OpenNativePredecessorArtifact(environmentID, captured.CurrentArtifact)
		if err != nil || prior.GetPriorArtifactId() != artifact.GetArtifactId() ||
			prior.GetPriorReleaseId() != captured.Serving.ServingReleaseID ||
			prior.GetPriorTarget() != string(captured.Serving.Target) {
			return releases.CorruptReleaseRecord()
		}
		if len(captured.RetainedPriorArtifact) != 0 {
			retained, err := executionplan.OpenNativePredecessorArtifact(environmentID, captured.RetainedPriorArtifact)
			if err != nil || prior.GetRetainedPriorArtifactId() != retained.GetArtifactId() {
				return releases.CorruptReleaseRecord()
			}
			for _, service := range retained.Services {
				if service.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY &&
					composeServiceReleaseID(service) != captured.Serving.RetainedPriorReleaseID {
					return releases.CorruptReleaseRecord()
				}
			}
		} else if prior.GetRetainedPriorArtifactId() != "" {
			return releases.CorruptReleaseRecord()
		}
	}
	if len(byService) != 0 {
		return releases.CorruptReleaseRecord()
	}
	return nil
}
