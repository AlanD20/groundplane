package etcd

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type releaseRecoveryProofKind uint8

type releaseRecoveryProofExpectation struct {
	kind releaseRecoveryProofKind
	// Ordinary recreate may restore the publication-owned prior topology or
	// observe the untouched historical witness. Both retain exact artifact identity.
	priorTopologyArtifactID string
	nativeArtifact          []byte
}

func validateReleaseRecoveryProof(
	assignment taskassignments.TaskAssignmentRecord,
	procedure *agentpb.CandidateReleaseProcedure,
	result taskjournal.TaskResultRecord,
	expectations []releaseRecoveryProofExpectation,
) error {
	authority := assignment.RestorationAuthority
	if authority == nil || result.ReconciliationRequired || len(expectations) != len(authority.Candidates) {
		return taskassignments.CorruptTaskAssignment()
	}
	absenceIndex, proxyIndex, recreateIndex := 0, 0, 0
	for memberIndex, candidate := range authority.Candidates {
		expectation := expectations[memberIndex]
		kind := expectation.kind
		if kind != releaseRecoveryProofCaptured && kind != releaseRecoveryProofProxy &&
			kind != releaseRecoveryProofRecreate ||
			(kind == releaseRecoveryProofRecreate) != (expectation.priorTopologyArtifactID != "") {
			return taskassignments.CorruptTaskAssignment()
		}
		switch candidate.Target {
		case taskassignments.ReleaseRestorationCandidateAbsence:
			evidence := result.CandidateAbsenceEvidence
			if evidence == nil || !evidence.AbsenceProven || evidence.AssignmentID != assignment.AssignmentID ||
				evidence.PlanHash != authority.PlanHash || evidence.AuthoritySHA256 != assignment.RestorationAuthoritySHA256 ||
				evidence.CandidateArtifactID != authority.CandidateArtifactID || absenceIndex >= len(evidence.Candidates) ||
				memberIndex >= len(procedure.GetMembers()) ||
				evidence.ComposeProjectName != procedure.GetMembers()[memberIndex].GetCandidateAbsence().
					GetComposeProjectName() {
				return taskassignments.CorruptTaskAssignment()
			}
			if evidence.Candidates[absenceIndex].ServiceID != candidate.ServiceID ||
				evidence.Candidates[absenceIndex].ReleaseID != candidate.ReleaseID {
				return taskassignments.CorruptTaskAssignment()
			}
			absenceIndex++
		case taskassignments.ReleaseRestorationServingPredecessor:
			if authority.AppliedPredecessor == nil {
				return taskassignments.CorruptTaskAssignment()
			}
			artifact := &agentpb.ComposeArtifact{}
			encoded := authority.AppliedPredecessor.ComposeArtifact
			if len(expectation.nativeArtifact) != 0 {
				encoded = expectation.nativeArtifact
			}
			if proto.Unmarshal(encoded, artifact) != nil {
				return taskassignments.CorruptTaskAssignment()
			}
			var workload, proxy *agentpb.ComposeService
			for _, item := range artifact.GetServices() {
				if item.GetServiceId() != candidate.ServiceID {
					continue
				}
				switch item.GetRole() {
				case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY:
					if proxy != nil {
						return taskassignments.CorruptTaskAssignment()
					}
					proxy = item
				case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
					agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON:
					if composeServiceReleaseID(item) == "" {
						continue
					}
					if workload != nil {
						return taskassignments.CorruptTaskAssignment()
					}
					workload = item
				}
			}
			if workload == nil {
				return taskassignments.CorruptTaskAssignment()
			}
			target := workload.GetSlot()
			if target == "" {
				target = "singleton"
			}
			releaseID := composeServiceReleaseID(workload)
			if kind == releaseRecoveryProofProxy && proxy == nil {
				return taskassignments.CorruptTaskAssignment()
			}
			if kind == releaseRecoveryProofProxy || kind == releaseRecoveryProofCaptured && proxy != nil {
				generation, generationErr := executionplan.ProxyConfigGeneration(proxy.GetProxyConfigJson(), releaseID)
				if proxyIndex >= len(result.ProxyEvidence) {
					return errs.New(errs.KindStateConflict, "release recovery proxy evidence is missing")
				}
				evidence := result.ProxyEvidence[proxyIndex]
				switch {
				case generationErr != nil:
					return errs.New(errs.KindStateConflict, "release recovery native proxy generation is invalid")
				case evidence.ServiceID != candidate.ServiceID || evidence.ReleaseID != releaseID || evidence.Target != target:
					return errs.New(
						errs.KindStateConflict,
						"release recovery proxy workload identity differs from native predecessor",
					)
				case !evidence.Compensated:
					return errs.New(errs.KindStateConflict, "release recovery proxy restoration is not proven")
				case evidence.ProxyGeneration != generation:
					return errs.New(
						errs.KindStateConflict,
						"release recovery proxy generation differs from native predecessor",
					)
				case evidence.ConfigSHA256 != hex.EncodeToString(proxy.GetProxyConfigSha256()):
					return errs.New(
						errs.KindStateConflict,
						"release recovery proxy configuration digest differs from native predecessor",
					)
				}
				proxyIndex++
			} else {
				if recreateIndex >= len(result.RecreateEvidence) {
					return taskassignments.CorruptTaskAssignment()
				}
				evidence := result.RecreateEvidence[recreateIndex]
				expectedArtifactID := artifact.GetArtifactId()
				if kind == releaseRecoveryProofRecreate && evidence.ArtifactID != expectedArtifactID {
					expectedArtifactID = expectation.priorTopologyArtifactID
				}
				if evidence.ServiceID != candidate.ServiceID || evidence.ArtifactID != expectedArtifactID ||
					!evidence.Compensated || evidence.ReleaseID != releaseID || evidence.Target != target {
					return taskassignments.CorruptTaskAssignment()
				}
				recreateIndex++
			}
		default:
			return taskassignments.CorruptTaskAssignment()
		}
	}
	if proxyIndex != len(result.ProxyEvidence) || recreateIndex != len(result.RecreateEvidence) ||
		absenceIndex == 0 && result.CandidateAbsenceEvidence != nil ||
		absenceIndex > 0 &&
			(result.CandidateAbsenceEvidence == nil || absenceIndex != len(result.CandidateAbsenceEvidence.Candidates)) {
		return taskassignments.CorruptTaskAssignment()
	}
	return nil
}

const (
	releaseRecoveryProofCaptured releaseRecoveryProofKind = iota + 1
	releaseRecoveryProofProxy
	releaseRecoveryProofRecreate
)

// recoveryProofSelectionAtRevision binds the ordinary plan compiler's strategy
// branch to immutable publication inputs, never to a current Service projection.
func (repository *TaskRepository) recoveryProofSelectionAtRevision(
	ctx context.Context, task TaskRecord, assignment taskassignments.TaskAssignmentRecord, revision int64,
) (*agentpb.CandidateReleaseProcedure, []releaseRecoveryProofExpectation, []etcdstore.Condition, error) {
	publication := task.Params[releaserender.TaskReleasePublicationParam]
	keys := []string{releases.ReleasePublicationKey(publication), releases.ReleaseManifestStagingKey(publication)}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 ||
		read.Values[0] == nil || read.Values[1] == nil {
		return nil, nil, nil, releases.CorruptReleaseRecord()
	}
	defer etcdstore.ClearValues(read.Values)
	marker, markerErr := releases.DecodeReleaseRecord[releases.ReleasePublicationMarker](read.Values[0].Value, "release-publication")
	manifest, manifestErr := releases.DecodeReleaseRecord[releases.ReleaseStagedManifest](read.Values[1].Value, "release-staged-manifest")
	procedure, err := validateReleaseCandidateMarker(task, marker, manifest)
	if markerErr != nil || manifestErr != nil || err != nil ||
		validateAssignmentRestorationDescriptor(task, assignment, procedure) != nil {
		return nil, nil, nil, releases.CorruptReleaseRecord()
	}
	conditions := []etcdstore.Condition{{Key: keys[0], ModRevision: read.Values[0].ModRevision},
		{Key: keys[1], ModRevision: read.Values[1].ModRevision}}
	var native []BlueprintNativePredecessor
	if taskHasBlueprintCandidateAppliedAuthority(task) {
		var sourceConditions []etcdstore.Condition
		native, sourceConditions, err = repository.blueprintNativePredecessorsAtRevision(
			ctx,
			task,
			marker,
			manifest,
			revision,
		)
		if err != nil {
			return nil, nil, nil, err
		}
		conditions = append(conditions, sourceConditions...)
	}
	expectations := make([]releaseRecoveryProofExpectation, len(assignment.RestorationAuthority.Candidates))
	for index, candidate := range assignment.RestorationAuthority.Candidates {
		expectations[index].kind = releaseRecoveryProofCaptured
		if taskHasBlueprintCandidateAppliedAuthority(task) {
			if err := bindNativeRecoveryExpectation(&expectations[index], assignment, native, procedure, index); err != nil {
				return nil, nil, nil, err
			}
			continue
		}
		if candidate.Target == taskassignments.ReleaseRestorationCandidateAbsence {
			continue
		}
		if task.Type != taskjournal.TaskDeploy && task.Type != taskjournal.TaskRollback {
			return nil, nil, nil, releases.CorruptReleaseRecord()
		}
		memberIndex := slices.IndexFunc(manifest.Members, func(member releases.ReleaseStagedMemberRef) bool {
			return member.ServiceID == candidate.ServiceID && member.ReleaseID == candidate.ReleaseID
		})
		if memberIndex < 0 {
			return nil, nil, nil, releases.CorruptReleaseRecord()
		}
		expectation, sourceConditions, err := repository.ordinaryRecoveryProofKindAtRevision(
			ctx, task, assignment, manifest.Members[memberIndex], revision,
		)
		if err != nil {
			return nil, nil, nil, err
		}
		expectations[index] = expectation
		conditions = append(conditions, sourceConditions...)
	}
	return procedure, expectations, conditions, nil
}

func (repository *TaskRepository) ordinaryRecoveryProofKindAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	member releases.ReleaseStagedMemberRef,
	revision int64,
) (releaseRecoveryProofExpectation, []etcdstore.Condition, error) {
	publication := task.Params[releaserender.TaskReleasePublicationParam]
	keys := []string{releases.ReleaseIntentStagingKey(publication, member.ReleaseID),
		releases.ReleaseRenderInputStagingKey(publication, member.ReleaseID), releases.ReleaseOperationKey(task.OperationID)}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return releaseRecoveryProofExpectation{}, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 3 ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil {
		return releaseRecoveryProofExpectation{}, nil, releases.CorruptReleaseRecord()
	}
	defer etcdstore.ClearValues(read.Values)
	intent, intentErr := releases.DecodeReleaseRecord[domain.Intent](read.Values[0].Value, "release-intent")
	head, headErr := releases.DecodeReleaseRecord[releases.ReleaseOperationHead](read.Values[2].Value, "release-operation")
	raw, rawErr := releases.DecodeReleaseRecord[json.RawMessage](read.Values[1].Value, "release-render-input")
	render, renderErr := releaserender.DecodeReleaseRenderInput(raw)
	intentDigest, intentDigestErr := domain.Digest(intent)
	renderDigest, renderDigestErr := domain.Digest(raw)
	if intentErr != nil || headErr != nil || rawErr != nil || renderErr != nil || intentDigestErr != nil ||
		renderDigestErr != nil ||
		!ordinaryRecoveryIntentMatchesAttempt(task, intent, head) ||
		domain.ValidateIntent(intent) != nil ||
		intentDigest != member.IntentDigest ||
		renderDigest != member.RenderDigest ||
		renderDigest != intent.RenderInputDigest ||
		intent.ID != member.ReleaseID ||
		intent.ServiceID != member.ServiceID ||
		intent.OperationID != task.OperationID ||
		intent.EnvironmentID != task.Owner.EnvironmentID ||
		render.ReleaseID != member.ReleaseID ||
		render.PlanID != task.PlanID ||
		render.ServiceID != member.ServiceID ||
		render.EnvironmentID != task.Owner.EnvironmentID ||
		render.ArtifactID != intent.RenderInputID ||
		render.ArtifactID != assignment.RestorationAuthority.CandidateArtifactID ||
		render.CandidateWorkload != intent.CandidateWorkload ||
		render.Strategy != intent.Strategy ||
		render.Slot != intent.Slot {
		return releaseRecoveryProofExpectation{}, nil, releases.CorruptReleaseRecord()
	}
	if !recoveryRenderMatchesPredecessor(render, intent, assignment.RestorationAuthority) {
		return releaseRecoveryProofExpectation{}, nil, releases.CorruptReleaseRecord()
	}
	var expectation releaseRecoveryProofExpectation
	expectation.nativeArtifact = slices.Clone(render.PriorRuntime.CurrentArtifact)
	switch render.Strategy {
	case domain.StrategyRecreate:
		expectation.kind, expectation.priorTopologyArtifactID = releaseRecoveryProofRecreate, render.PriorArtifactID
	case domain.StrategyBlueGreen:
		expectation.kind = releaseRecoveryProofProxy
	default:
		return releaseRecoveryProofExpectation{}, nil, releases.CorruptReleaseRecord()
	}
	if len(render.PriorRuntime.RetainedPriorArtifact) != 0 {
		expectation.kind, expectation.priorTopologyArtifactID = releaseRecoveryProofCaptured, ""
	}
	return expectation, []etcdstore.Condition{{Key: keys[0], ModRevision: read.Values[0].ModRevision},
		{Key: keys[1], ModRevision: read.Values[1].ModRevision},
		{Key: keys[2], ModRevision: read.Values[2].ModRevision}}, nil
}

func ordinaryRecoveryIntentMatchesAttempt(task TaskRecord, intent domain.Intent, head releases.ReleaseOperationHead) bool {
	if head.OperationID != task.OperationID || head.PublicationID != task.Params[releaserender.TaskReleasePublicationParam] ||
		head.EnvironmentID != task.Owner.EnvironmentID || head.LatestTaskID != task.ID || len(head.Attempts) == 0 ||
		intent.OriginatingTaskID != head.Attempts[0].TaskID {
		return false
	}
	expectedKind := domain.OperationDeploy
	if task.Type == taskjournal.TaskRollback {
		expectedKind = domain.OperationRollback
	} else if task.Type != taskjournal.TaskDeploy {
		return false
	}
	if intent.OperationKind != expectedKind {
		return false
	}
	previous := ""
	seen := make(map[string]struct{}, len(head.Attempts))
	for _, attempt := range head.Attempts {
		if attempt.TaskID == "" || attempt.ID != attempt.TaskID || attempt.RetryOf != previous {
			return false
		}
		if _, duplicate := seen[attempt.TaskID]; duplicate {
			return false
		}
		seen[attempt.TaskID] = struct{}{}
		previous = attempt.TaskID
	}
	latest := head.Attempts[len(head.Attempts)-1]
	return latest.TaskID == task.ID && latest.RetryOf == task.RetryOf
}

func recoveryRenderMatchesPredecessor(
	render releaserender.ReleaseRenderInput, intent domain.Intent, authority *taskassignments.ReleaseRestorationAuthority,
) bool {
	if authority == nil || render.PriorRuntime == nil || render.PriorArtifactID == "" ||
		render.PriorWorkload == nil || intent.PriorServingReleaseID == "" {
		return false
	}
	var encoded []byte
	for _, witness := range authority.NativePredecessors {
		if witness.ServiceID == render.ServiceID &&
			bytes.Equal(witness.CurrentArtifact, render.PriorRuntime.CurrentArtifact) &&
			bytes.Equal(witness.RetainedPriorArtifact, render.PriorRuntime.RetainedPriorArtifact) {
			encoded = witness.CurrentArtifact
		}
	}
	if len(encoded) == 0 {
		return false
	}
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(encoded, artifact) != nil || artifact.ArtifactId != render.PriorArtifactID {
		return false
	}
	found := false
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != render.ServiceID || composeServiceReleaseID(service) == "" {
			continue
		}
		if service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT &&
			service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON {
			continue
		}
		target := service.GetSlot()
		if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON {
			target = string(domain.WorkloadSingleton)
		}
		if found || composeServiceReleaseID(service) != intent.PriorServingReleaseID ||
			target != string(render.PriorTarget) ||
			service.GetImageReference() != render.PriorWorkload.LocalImageID ||
			service.GetExpectedReplicas() != render.PriorWorkload.ReplicaCount {
			return false
		}
		found = true
	}
	return found
}
