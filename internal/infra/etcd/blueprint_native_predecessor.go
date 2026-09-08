package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BlueprintNativePredecessor seals native serving identity independently of the
// Environment's acknowledged Component/projection witness.
type BlueprintNativePredecessor struct {
	ServiceID             string                   `json:"service_id"`
	FixedReadRevision     int64                    `json:"fixed_read_revision"`
	ProjectionRevision    int64                    `json:"projection_revision"`
	Serving               *ServiceLifecycleRelease `json:"serving,omitempty"`
	CurrentArtifact       []byte                   `json:"current_artifact,omitempty"`
	RetainedPriorArtifact []byte                   `json:"retained_prior_artifact,omitempty"`
}

type ReleaseNativePredecessorAuthority struct {
	ServiceID             string `json:"service_id"`
	CurrentArtifact       []byte `json:"current_artifact,omitempty"`
	RetainedPriorArtifact []byte `json:"retained_prior_artifact,omitempty"`
}

func cloneReleaseNativePredecessors(values []ReleaseNativePredecessorAuthority) []ReleaseNativePredecessorAuthority {
	result := slices.Clone(values)
	for index := range result {
		result[index].CurrentArtifact = slices.Clone(result[index].CurrentArtifact)
		result[index].RetainedPriorArtifact = slices.Clone(result[index].RetainedPriorArtifact)
	}
	return result
}

func buildBlueprintNativeRestorationAuthority(
	task TaskRecord,
	predecessor taskMaterializationAppliedPredecessor,
	marker ReleasePublicationMarker,
	manifest ReleaseStagedManifest,
	procedure *agentpb.CandidateReleaseProcedure,
	applied []byte,
) (ReleaseRestorationAuthority, string, error) {
	if !taskHasBlueprintCandidateAppliedAuthority(task) || predecessor.Present != (len(applied) != 0) ||
		validateBlueprintNativePredecessors(marker.NativePredecessors, procedure, task.Owner.EnvironmentID) != nil {
		return ReleaseRestorationAuthority{}, "", corruptTaskAssignment()
	}
	authority := ReleaseRestorationAuthority{
		Schema:              1,
		TaskID:              task.ID,
		OperationID:         task.OperationID,
		PlanHash:            task.PlanHash,
		EnvironmentID:       task.Owner.EnvironmentID,
		CandidateArtifactID: task.Params[TaskComposeArtifactParam],
	}
	if predecessor.Present {
		if _, err := openRestorationWitness(task.Owner.EnvironmentID, applied); err != nil {
			return ReleaseRestorationAuthority{}, "", err
		}
		digest := sha256.Sum256(applied)
		authority.AppliedPredecessor = &ReleaseAppliedPredecessorAuthority{
			KeyRevision:           predecessor.KeyRevision,
			RevisionID:            predecessor.RevisionID,
			RenderGeneration:      predecessor.RenderGeneration,
			ComposeArtifactSHA256: hex.EncodeToString(digest[:]),
			ComposeArtifact:       slices.Clone(applied),
		}
	}
	for _, member := range manifest.Members {
		candidate := ReleaseRestorationCandidate{
			ServiceID: member.ServiceID,
			ReleaseID: member.ReleaseID,
			Target:    ReleaseRestorationCandidateAbsence,
		}
		witness := ReleaseNativePredecessorAuthority{ServiceID: member.ServiceID}
		for _, captured := range marker.NativePredecessors {
			if captured.ServiceID != member.ServiceID {
				continue
			}
			witness.CurrentArtifact, witness.RetainedPriorArtifact = slices.Clone(
				captured.CurrentArtifact,
			), slices.Clone(
				captured.RetainedPriorArtifact,
			)
			if captured.Serving != nil {
				candidate.Target = ReleaseRestorationServingPredecessor
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
	digest, err := releaseRestorationAuthoritySHA256(authority)
	return authority, digest, err
}

func validateNativeRestorationMemberWitness(authority ReleaseRestorationAuthority) error {
	if len(authority.NativePredecessors) != len(authority.Candidates) ||
		len(authority.NativePredecessors) > maximumReleasePublicationMembers {
		return corruptTaskAssignment()
	}
	if authority.AppliedPredecessor != nil {
		if _, err := openRestorationWitness(authority.EnvironmentID, authority.AppliedPredecessor.ComposeArtifact); err != nil {
			return err
		}
	}
	total := 0
	for index, witness := range authority.NativePredecessors {
		candidate := authority.Candidates[index]
		total += len(witness.CurrentArtifact) + len(witness.RetainedPriorArtifact)
		if witness.ServiceID != candidate.ServiceID || total > MaximumTaskRecordBytes {
			return corruptTaskAssignment()
		}
		if len(witness.CurrentArtifact) == 0 {
			if candidate.Target != ReleaseRestorationCandidateAbsence || len(witness.RetainedPriorArtifact) != 0 {
				return corruptTaskAssignment()
			}
			continue
		}
		if candidate.Target != ReleaseRestorationServingPredecessor {
			return corruptTaskAssignment()
		}
		if err := executionplan.ValidateNativePredecessorWitness(authority.EnvironmentID, witness.ServiceID, witness.CurrentArtifact, witness.RetainedPriorArtifact); err != nil {
			return corruptTaskAssignment()
		}
	}
	return nil
}

func bindNativeRecoveryExpectation(
	expectation *releaseRecoveryProofExpectation,
	assignment TaskAssignmentRecord,
	marker ReleasePublicationMarker,
	procedure *agentpb.CandidateReleaseProcedure,
	index int,
) error {
	authority := assignment.RestorationAuthority
	if authority == nil || len(authority.NativePredecessors) != len(authority.Candidates) ||
		index >= len(authority.NativePredecessors) ||
		validateBlueprintNativePredecessors(marker.NativePredecessors, procedure, authority.EnvironmentID) != nil {
		return corruptTaskAssignment()
	}
	witness := authority.NativePredecessors[index]
	candidate := authority.Candidates[index]
	if witness.ServiceID != candidate.ServiceID {
		return corruptTaskAssignment()
	}
	var expected, retained []byte
	for _, captured := range marker.NativePredecessors {
		if captured.ServiceID == candidate.ServiceID {
			expected, retained = captured.CurrentArtifact, captured.RetainedPriorArtifact
		}
	}
	if !bytes.Equal(expected, witness.CurrentArtifact) || !bytes.Equal(retained, witness.RetainedPriorArtifact) ||
		(len(expected) == 0) != (candidate.Target == ReleaseRestorationCandidateAbsence) {
		return corruptTaskAssignment()
	}
	expectation.nativeArtifact = slices.Clone(expected)
	return nil
}

func validateNativeRestorationDescriptor(
	authority ReleaseRestorationAuthority,
	procedure *agentpb.CandidateReleaseProcedure,
) error {
	if len(authority.NativePredecessors) != len(procedure.GetMembers()) || len(authority.NativePredecessors) == 0 {
		return corruptTaskAssignment()
	}
	for index, member := range procedure.GetMembers() {
		witness := authority.NativePredecessors[index]
		if witness.ServiceID != member.GetServiceId() {
			return corruptTaskAssignment()
		}
		prior := member.GetServingPredecessor()
		if len(witness.CurrentArtifact) == 0 {
			if prior.GetPriorArtifactId() != "" || prior.GetPriorReleaseId() != "" || prior.GetPriorTarget() != "" ||
				prior.GetRetainedPriorArtifactId() != "" {
				return corruptTaskAssignment()
			}
			continue
		}
		artifact, err := openRestorationWitness(authority.EnvironmentID, witness.CurrentArtifact)
		if err != nil || artifact.GetArtifactId() != prior.GetPriorArtifactId() {
			return corruptTaskAssignment()
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
				return corruptTaskAssignment()
			}
		}
		if len(witness.RetainedPriorArtifact) != 0 {
			retained, err := openRestorationWitness(authority.EnvironmentID, witness.RetainedPriorArtifact)
			if err != nil || retained.GetArtifactId() != prior.GetRetainedPriorArtifactId() {
				return corruptTaskAssignment()
			}
		} else if prior.GetRetainedPriorArtifactId() != "" {
			return corruptTaskAssignment()
		}
	}
	return nil
}

type BlueprintReleasePublicationEvidence struct {
	NativePredecessors         []BlueprintNativePredecessor
	Manifest                   VersionedReleaseManifest
	EnvironmentID              string
	Task                       TaskRecord
	CandidateReleaseDescriptor executionplan.CandidateReleaseDescriptor
	Plan                       *agentpb.ExecutionPlan
	Hooks                      []ReleaseHookExecutionPublication
	PublishedAt                time.Time
	SourcePrepared             PreparedSourceSet
	SourceMembers              []ScriptSourcePreparationMember
	HookPrepared               PreparedBlueprintReleaseHooks
}

func (ledger *ReleaseLedger) blueprintNativePredecessorConditions(
	ctx context.Context,
	evidence BlueprintReleasePublicationEvidence,
) ([]Condition, error) {
	if err := validateBlueprintNativePredecessors(evidence.NativePredecessors, evidence.Plan.GetCandidateReleaseProcedure(), evidence.EnvironmentID); err != nil {
		return nil, err
	}
	for _, member := range evidence.Manifest.Record.Members {
		loaded, err := ledger.store.GetMany(
			ctx,
			GetManyRequest{
				Keys:     []string{releaseIntentStagingKey("", member.ReleaseID)},
				Revision: evidence.Manifest.Revision,
			},
		)
		if err != nil {
			return nil, err
		}
		if loaded == nil || len(loaded.Values) != 1 || loaded.Values[0] == nil {
			return nil, corruptReleaseRecord()
		}
		intent, err := decodeReleaseRecord[domain.Intent](loaded.Values[0].Value, "release-intent")
		if err != nil || validateBlueprintNativeIntent(intent, evidence.NativePredecessors) != nil {
			return nil, corruptReleaseRecord()
		}
	}
	var conditions []Condition
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
			return nil, corruptReleaseRecord()
		}
		conditions = append(conditions, guards...)
		conditions = append(
			conditions,
			Condition{Key: releaseProjectionKey(captured.ServiceID), ModRevision: captured.ProjectionRevision},
			Condition{Key: environmentComposeProjectionKey(evidence.EnvironmentID), ModRevision: applied.Revision},
		)
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
		return corruptReleaseRecord()
	}
	return nil
}

func validateBlueprintNativeRenderInput(
	input ReleaseTaskRenderInput,
	marker ReleasePublicationMarker,
	task TaskRecord,
) error {
	procedure, err := executionplan.OpenCandidateReleaseDescriptor(marker.CandidateReleaseDescriptor)
	if err != nil ||
		validateBlueprintNativePredecessors(input.NativePredecessors, procedure, task.Owner.EnvironmentID) != nil {
		return corruptReleaseRecord()
	}
	for _, member := range input.Members {
		if validateBlueprintNativeIntent(member.Intent, input.NativePredecessors) != nil {
			return corruptReleaseRecord()
		}
		expected := ""
		for _, selected := range procedure.GetMembers() {
			if selected.ServiceId == member.Intent.ServiceID {
				expected = selected.GetServingPredecessor().GetPriorArtifactId()
			}
		}
		if member.Render.PriorArtifactID != expected {
			return corruptReleaseRecord()
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
			return corruptReleaseRecord()
		}
		if captured.Serving == nil {
			if len(captured.CurrentArtifact) != 0 || len(captured.RetainedPriorArtifact) != 0 {
				return corruptReleaseRecord()
			}
		} else if validateServiceLifecycleRelease(*captured.Serving, ServiceLifecycleRenderInput{ServiceID: captured.ServiceID, EnvironmentID: environmentID}) != nil ||
			captured.Serving.ProjectionRevision != captured.ProjectionRevision || len(captured.CurrentArtifact) == 0 ||
			(captured.Serving.RetainedPrior != nil) != (len(captured.RetainedPriorArtifact) != 0) {
			return corruptReleaseRecord()
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
				return corruptReleaseRecord()
			}
			continue
		}
		artifact, err := openRestorationWitness(environmentID, captured.CurrentArtifact)
		if err != nil || prior.GetPriorArtifactId() != artifact.GetArtifactId() ||
			prior.GetPriorReleaseId() != captured.Serving.ServingReleaseID ||
			prior.GetPriorTarget() != string(captured.Serving.Current.CandidateTarget) {
			return corruptReleaseRecord()
		}
		if len(captured.RetainedPriorArtifact) != 0 {
			retained, err := openRestorationWitness(environmentID, captured.RetainedPriorArtifact)
			if err != nil || prior.GetRetainedPriorArtifactId() != retained.GetArtifactId() {
				return corruptReleaseRecord()
			}
		} else if prior.GetRetainedPriorArtifactId() != "" {
			return corruptReleaseRecord()
		}
	}
	if len(byService) != 0 {
		return corruptReleaseRecord()
	}
	return nil
}
