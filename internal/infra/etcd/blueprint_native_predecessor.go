package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	scriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	scriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"slices"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
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

func buildBlueprintNativeRestorationAuthority(
	task TaskRecord,
	predecessor taskMaterializationAppliedPredecessor,
	native []BlueprintNativePredecessor,
	manifest releases.ReleaseStagedManifest,
	procedure *agentpb.CandidateReleaseProcedure,
	applied []byte,
) (taskassignments.ReleaseRestorationAuthority, string, error) {
	if !taskHasBlueprintCandidateAppliedAuthority(task) || predecessor.Present != (len(applied) != 0) ||
		validateBlueprintNativePredecessors(native, procedure, task.Owner.EnvironmentID) != nil {
		return taskassignments.ReleaseRestorationAuthority{}, "", taskassignments.CorruptTaskAssignment()
	}
	authority := taskassignments.ReleaseRestorationAuthority{
		Schema:              1,
		TaskID:              task.ID,
		OperationID:         task.OperationID,
		PlanHash:            task.PlanHash,
		EnvironmentID:       task.Owner.EnvironmentID,
		CandidateArtifactID: task.Params[taskjournal.TaskComposeArtifactParam],
	}
	if predecessor.Present {
		if _, err := taskassignments.OpenRestorationWitness(task.Owner.EnvironmentID, applied); err != nil {
			return taskassignments.ReleaseRestorationAuthority{}, "", err
		}
		digest := sha256.Sum256(applied)
		authority.AppliedPredecessor = &taskassignments.ReleaseAppliedPredecessorAuthority{
			KeyRevision:           predecessor.KeyRevision,
			RevisionID:            predecessor.RevisionID,
			RenderGeneration:      predecessor.RenderGeneration,
			ComposeArtifactSHA256: hex.EncodeToString(digest[:]),
			ComposeArtifact:       slices.Clone(applied),
		}
	}
	for _, member := range manifest.Members {
		candidate := taskassignments.ReleaseRestorationCandidate{
			ServiceID: member.ServiceID,
			ReleaseID: member.ReleaseID,
			Target:    taskassignments.ReleaseRestorationCandidateAbsence,
		}
		witness := taskassignments.ReleaseNativePredecessorAuthority{ServiceID: member.ServiceID}
		for _, captured := range native {
			if captured.ServiceID != member.ServiceID {
				continue
			}
			witness.CurrentArtifact, witness.RetainedPriorArtifact = slices.Clone(
				captured.CurrentArtifact,
			), slices.Clone(
				captured.RetainedPriorArtifact,
			)
			if captured.Serving != nil {
				candidate.Target = taskassignments.ReleaseRestorationServingPredecessor
			}
		}
		authority.Candidates = append(authority.Candidates, candidate)
		authority.NativePredecessors = append(authority.NativePredecessors, witness)
	}
	sort.Slice(
		authority.Candidates,
		func(i, j int) bool { return authority.Candidates[i].ServiceID < authority.Candidates[j].ServiceID },
	)
	sort.Slice(authority.NativePredecessors, func(i, j int) bool {
		return authority.NativePredecessors[i].ServiceID < authority.NativePredecessors[j].ServiceID
	})
	digest, err := taskassignments.ReleaseRestorationAuthoritySHA256(authority)
	return authority, digest, err
}

func bindNativeRecoveryExpectation(
	expectation *releaseRecoveryProofExpectation,
	assignment taskassignments.TaskAssignmentRecord,
	native []BlueprintNativePredecessor,
	procedure *agentpb.CandidateReleaseProcedure,
	index int,
) error {
	authority := assignment.RestorationAuthority
	if authority == nil || len(authority.NativePredecessors) != len(authority.Candidates) ||
		index >= len(authority.NativePredecessors) ||
		validateBlueprintNativePredecessors(native, procedure, authority.EnvironmentID) != nil {
		return taskassignments.CorruptTaskAssignment()
	}
	witness := authority.NativePredecessors[index]
	candidate := authority.Candidates[index]
	if witness.ServiceID != candidate.ServiceID {
		return taskassignments.CorruptTaskAssignment()
	}
	var expected, retained []byte
	for _, captured := range native {
		if captured.ServiceID == candidate.ServiceID {
			expected, retained = captured.CurrentArtifact, captured.RetainedPriorArtifact
		}
	}
	if !bytes.Equal(expected, witness.CurrentArtifact) || !bytes.Equal(retained, witness.RetainedPriorArtifact) ||
		(len(expected) == 0) != (candidate.Target == taskassignments.ReleaseRestorationCandidateAbsence) {
		return taskassignments.CorruptTaskAssignment()
	}
	expectation.nativeArtifact = slices.Clone(expected)
	return nil
}

func validateNativeRestorationDescriptor(
	authority taskassignments.ReleaseRestorationAuthority,
	procedure *agentpb.CandidateReleaseProcedure,
) error {
	if len(authority.NativePredecessors) != len(procedure.GetMembers()) || len(authority.NativePredecessors) == 0 {
		return taskassignments.CorruptTaskAssignment()
	}
	for index, member := range procedure.GetMembers() {
		witness := authority.NativePredecessors[index]
		if witness.ServiceID != member.GetServiceId() {
			return taskassignments.CorruptTaskAssignment()
		}
		prior := member.GetServingPredecessor()
		if len(witness.CurrentArtifact) == 0 {
			if prior.GetPriorArtifactId() != "" || prior.GetPriorReleaseId() != "" || prior.GetPriorTarget() != "" ||
				prior.GetRetainedPriorArtifactId() != "" {
				return taskassignments.CorruptTaskAssignment()
			}
			continue
		}
		artifact, err := taskassignments.OpenRestorationWitness(authority.EnvironmentID, witness.CurrentArtifact)
		if err != nil || artifact.GetArtifactId() != prior.GetPriorArtifactId() {
			return taskassignments.CorruptTaskAssignment()
		}
		for _, service := range artifact.Services {
			if service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
				continue
			}
			target := service.GetSlot()
			if target == "" {
				target = "singleton"
			}
			if composeServiceReleaseID(service) != prior.GetPriorReleaseId() || target != prior.GetPriorTarget() {
				return taskassignments.CorruptTaskAssignment()
			}
		}
		if len(witness.RetainedPriorArtifact) != 0 {
			retained, err := taskassignments.OpenRestorationWitness(authority.EnvironmentID, witness.RetainedPriorArtifact)
			if err != nil || retained.GetArtifactId() != prior.GetRetainedPriorArtifactId() {
				return taskassignments.CorruptTaskAssignment()
			}
		} else if prior.GetRetainedPriorArtifactId() != "" {
			return taskassignments.CorruptTaskAssignment()
		}
	}
	return nil
}

type BlueprintReleasePublicationEvidence struct {
	NativePredecessors         []BlueprintNativePredecessorCapture
	Manifest                   releases.VersionedReleaseManifest
	EnvironmentID              string
	Task                       TaskRecord
	CandidateReleaseDescriptor executionplan.CandidateReleaseDescriptor
	Plan                       *agentpb.ExecutionPlan
	Hooks                      []ReleaseHookExecutionPublication
	PublishedAt                time.Time
	SourcePrepared             scriptsourcepublication.PreparedSourceSet
	SourceMembers              []scriptsourceevidence.ScriptSourcePreparationMember
	HookPrepared               PreparedBlueprintReleaseHooks
}

func (ledger *ReleaseLedger) blueprintNativePredecessorConditions(
	ctx context.Context,
	evidence BlueprintReleasePublicationEvidence,
) ([]etcdstore.Condition, error) {
	native := make([]BlueprintNativePredecessor, len(evidence.NativePredecessors))
	for index, capture := range evidence.NativePredecessors {
		native[index] = capture.Runtime()
		if capture.Serving == nil && capture.RetainedPriorReleaseID != "" {
			return nil, releases.CorruptReleaseRecord()
		}
		if capture.Serving != nil && (releaserender.ValidateServiceLifecycleRelease(*capture.Serving,
			releaserender.ServiceLifecycleRenderInput{ServiceID: capture.ServiceID, EnvironmentID: evidence.EnvironmentID}) != nil ||
			capture.Serving.ProjectionRevision != capture.ProjectionRevision) {
			return nil, releases.CorruptReleaseRecord()
		}
	}
	if err := validateBlueprintNativePredecessors(native, evidence.Plan.GetCandidateReleaseProcedure(), evidence.EnvironmentID); err != nil {
		return nil, err
	}
	references, err := blueprintNativePredecessorReferences(evidence.NativePredecessors)
	if err != nil {
		return nil, err
	}
	_, conditions, err := ledger.tasks.blueprintNativePredecessorsAtRevision(ctx, evidence.Task,
		releases.ReleasePublicationMarker{NativePredecessors: references}, evidence.Manifest.Record, evidence.Manifest.Revision)
	if err != nil {
		return nil, err
	}
	for _, captured := range evidence.NativePredecessors {
		scope, err := ledger.LoadPlanningScopeAtRevision(ctx, evidence.EnvironmentID, captured.FixedReadRevision)
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
		applied, present, err := ledger.GetPlanningAppliedProjection(ctx, scope)
		if err != nil {
			return nil, err
		}
		if captured.Serving != nil && !present {
			return nil, releases.CorruptReleaseRecord()
		}
		conditions = append(conditions, guards...)
		conditions = append(
			conditions,
			etcdstore.Condition{Key: releases.ReleaseProjectionKey(captured.ServiceID), ModRevision: captured.ProjectionRevision},
			etcdstore.Condition{Key: projectionrecord.EnvironmentComposeProjectionStorageKey(evidence.EnvironmentID), ModRevision: applied.Revision},
		)
		if captured.Serving != nil {
			conditions = append(conditions, etcdstore.Condition{
				Key:         serviceruntimerecord.Key(captured.ServiceID),
				ModRevision: captured.RuntimeRevision,
			})
		}
		for _, value := range [][]byte{captured.CurrentArtifact, captured.RetainedPriorArtifact} {
			if len(value) == 0 {
				continue
			}
			found := false
			for _, artifact := range evidence.Plan.GetArtifacts() {
				encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
				if err == nil && bytes.Equal(encoded, value) {
					found = true
				}
			}
			if !found {
				return nil, errs.New(errs.KindValidationFailed, "Blueprint native predecessor is not PlanHash bound")
			}
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

func validateBlueprintNativeRenderInput(
	input ReleaseTaskRenderInput,
	marker releases.ReleasePublicationMarker,
	task TaskRecord,
) error {
	procedure, err := executionplan.OpenCandidateReleaseDescriptor(marker.CandidateReleaseDescriptor)
	if err != nil ||
		validateBlueprintNativePredecessors(input.NativePredecessors, procedure, task.Owner.EnvironmentID) != nil {
		return releases.CorruptReleaseRecord()
	}
	for _, member := range input.Members {
		if validateBlueprintNativeIntent(member.Intent, input.NativePredecessors) != nil {
			return releases.CorruptReleaseRecord()
		}
		expected := ""
		for _, selected := range procedure.GetMembers() {
			if selected.ServiceId == member.Intent.ServiceID {
				expected = selected.GetServingPredecessor().GetPriorArtifactId()
			}
		}
		if member.Render.PriorArtifactID != expected {
			return releases.CorruptReleaseRecord()
		}
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
		artifact, err := taskassignments.OpenRestorationWitness(environmentID, captured.CurrentArtifact)
		if err != nil || prior.GetPriorArtifactId() != artifact.GetArtifactId() ||
			prior.GetPriorReleaseId() != captured.Serving.ServingReleaseID ||
			prior.GetPriorTarget() != string(captured.Serving.Target) {
			return releases.CorruptReleaseRecord()
		}
		if len(captured.RetainedPriorArtifact) != 0 {
			retained, err := taskassignments.OpenRestorationWitness(environmentID, captured.RetainedPriorArtifact)
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
