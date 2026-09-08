package etcd

import (
	"context"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type releaseRecoveryProofKind uint8

type releaseRecoveryProofExpectation struct {
	kind releaseRecoveryProofKind
	// Only ordinary recreate uses a publication-owned, regenerated prior topology.
	// Captured/Blueprint and proxy proofs retain their existing authority identity.
	priorTopologyArtifactID string
	nativeArtifact          []byte
}

func validateReleaseRecoveryProof(
	assignment TaskAssignmentRecord,
	procedure *agentpb.CandidateReleaseProcedure,
	result TaskResultRecord,
	expectations []releaseRecoveryProofExpectation,
) error {
	authority := assignment.RestorationAuthority
	if authority == nil || result.ReconciliationRequired || len(expectations) != len(authority.Candidates) {
		return corruptTaskAssignment()
	}
	absenceIndex, proxyIndex, recreateIndex := 0, 0, 0
	for memberIndex, candidate := range authority.Candidates {
		expectation := expectations[memberIndex]
		kind := expectation.kind
		if kind != releaseRecoveryProofCaptured && kind != releaseRecoveryProofProxy &&
			kind != releaseRecoveryProofRecreate ||
			(kind == releaseRecoveryProofRecreate) != (expectation.priorTopologyArtifactID != "") {
			return corruptTaskAssignment()
		}
		switch candidate.Target {
		case ReleaseRestorationCandidateAbsence:
			evidence := result.CandidateAbsenceEvidence
			if evidence == nil || !evidence.AbsenceProven || evidence.AssignmentID != assignment.AssignmentID ||
				evidence.PlanHash != authority.PlanHash || evidence.AuthoritySHA256 != assignment.RestorationAuthoritySHA256 ||
				evidence.CandidateArtifactID != authority.CandidateArtifactID || absenceIndex >= len(evidence.Candidates) ||
				memberIndex >= len(procedure.GetMembers()) ||
				evidence.ComposeProjectName != procedure.GetMembers()[memberIndex].GetCandidateAbsence().
					GetComposeProjectName() {
				return corruptTaskAssignment()
			}
			if evidence.Candidates[absenceIndex].ServiceID != candidate.ServiceID ||
				evidence.Candidates[absenceIndex].ReleaseID != candidate.ReleaseID {
				return corruptTaskAssignment()
			}
			absenceIndex++
		case ReleaseRestorationServingPredecessor:
			if authority.AppliedPredecessor == nil {
				return corruptTaskAssignment()
			}
			artifact := &agentpb.ComposeArtifact{}
			encoded := authority.AppliedPredecessor.ComposeArtifact
			if len(expectation.nativeArtifact) != 0 {
				encoded = expectation.nativeArtifact
			}
			if proto.Unmarshal(encoded, artifact) != nil {
				return corruptTaskAssignment()
			}
			var workload, proxy *agentpb.ComposeService
			for _, item := range artifact.GetServices() {
				if item.GetServiceId() != candidate.ServiceID {
					continue
				}
				switch item.GetRole() {
				case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY:
					if proxy != nil {
						return corruptTaskAssignment()
					}
					proxy = item
				case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
					agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON:
					if composeServiceReleaseID(item) == "" {
						continue
					}
					if workload != nil {
						return corruptTaskAssignment()
					}
					workload = item
				}
			}
			if workload == nil {
				return corruptTaskAssignment()
			}
			target := workload.GetSlot()
			if target == "" {
				target = "singleton"
			}
			releaseID := composeServiceReleaseID(workload)
			if kind == releaseRecoveryProofProxy && proxy == nil {
				return corruptTaskAssignment()
			}
			if kind == releaseRecoveryProofProxy || kind == releaseRecoveryProofCaptured && proxy != nil {
				generation, generationErr := executionplan.ProxyConfigGeneration(proxy.GetProxyConfigJson(), releaseID)
				if proxyIndex >= len(result.ProxyEvidence) {
					return corruptTaskAssignment()
				}
				evidence := result.ProxyEvidence[proxyIndex]
				if generationErr != nil || evidence.ServiceID != candidate.ServiceID ||
					evidence.ReleaseID != releaseID ||
					evidence.Target != target ||
					!evidence.Compensated ||
					evidence.ConfigSHA256 != hex.EncodeToString(proxy.GetProxyConfigSha256()) ||
					evidence.ProxyGeneration != generation {
					return corruptTaskAssignment()
				}
				proxyIndex++
			} else {
				if recreateIndex >= len(result.RecreateEvidence) {
					return corruptTaskAssignment()
				}
				evidence := result.RecreateEvidence[recreateIndex]
				expectedArtifactID := artifact.GetArtifactId()
				if kind == releaseRecoveryProofRecreate {
					expectedArtifactID = expectation.priorTopologyArtifactID
				}
				if evidence.ServiceID != candidate.ServiceID || evidence.ArtifactID != expectedArtifactID ||
					!evidence.Compensated || evidence.ReleaseID != releaseID || evidence.Target != target {
					return corruptTaskAssignment()
				}
				recreateIndex++
			}
		default:
			return corruptTaskAssignment()
		}
	}
	if proxyIndex != len(result.ProxyEvidence) || recreateIndex != len(result.RecreateEvidence) ||
		absenceIndex == 0 && result.CandidateAbsenceEvidence != nil ||
		absenceIndex > 0 &&
			(result.CandidateAbsenceEvidence == nil || absenceIndex != len(result.CandidateAbsenceEvidence.Candidates)) {
		return corruptTaskAssignment()
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
	ctx context.Context, task TaskRecord, assignment TaskAssignmentRecord, revision int64,
) (*agentpb.CandidateReleaseProcedure, []releaseRecoveryProofExpectation, []Condition, error) {
	publication := task.Params[TaskReleasePublicationParam]
	keys := []string{releasePublicationKey(publication), releaseManifestStagingKey(publication)}
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 ||
		read.Values[0] == nil || read.Values[1] == nil {
		return nil, nil, nil, corruptReleaseRecord()
	}
	defer clearKeyValues(read.Values)
	marker, markerErr := decodeReleaseRecord[ReleasePublicationMarker](read.Values[0].Value, "release-publication")
	manifest, manifestErr := decodeReleaseRecord[ReleaseStagedManifest](read.Values[1].Value, "release-staged-manifest")
	procedure, err := validateReleaseCandidateMarker(task, marker, manifest)
	if markerErr != nil || manifestErr != nil || err != nil ||
		validateAssignmentRestorationDescriptor(task, assignment, procedure) != nil {
		return nil, nil, nil, corruptReleaseRecord()
	}
	conditions := []Condition{{Key: keys[0], ModRevision: read.Values[0].ModRevision},
		{Key: keys[1], ModRevision: read.Values[1].ModRevision}}
	expectations := make([]releaseRecoveryProofExpectation, len(assignment.RestorationAuthority.Candidates))
	for index, candidate := range assignment.RestorationAuthority.Candidates {
		expectations[index].kind = releaseRecoveryProofCaptured
		if taskHasBlueprintCandidateAppliedAuthority(task) {
			if err := bindNativeRecoveryExpectation(&expectations[index], assignment, marker, procedure, index); err != nil {
				return nil, nil, nil, err
			}
			continue
		}
		if candidate.Target == ReleaseRestorationCandidateAbsence {
			continue
		}
		if task.Type != TaskDeploy && task.Type != TaskRollback {
			return nil, nil, nil, corruptReleaseRecord()
		}
		if index >= len(manifest.Members) || manifest.Members[index].ServiceID != candidate.ServiceID ||
			manifest.Members[index].ReleaseID != candidate.ReleaseID {
			return nil, nil, nil, corruptReleaseRecord()
		}
		expectation, sourceConditions, err := repository.ordinaryRecoveryProofKindAtRevision(
			ctx, task, assignment, manifest.Members[index], revision,
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
	assignment TaskAssignmentRecord,
	member ReleaseStagedMemberRef,
	revision int64,
) (releaseRecoveryProofExpectation, []Condition, error) {
	publication := task.Params[TaskReleasePublicationParam]
	keys := []string{releaseIntentStagingKey(publication, member.ReleaseID),
		releaseRenderInputStagingKey(publication, member.ReleaseID), releaseOperationKey(task.OperationID)}
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return releaseRecoveryProofExpectation{}, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 3 ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil {
		return releaseRecoveryProofExpectation{}, nil, corruptReleaseRecord()
	}
	defer clearKeyValues(read.Values)
	intent, intentErr := decodeReleaseRecord[domain.Intent](read.Values[0].Value, "release-intent")
	head, headErr := decodeReleaseRecord[ReleaseOperationHead](read.Values[2].Value, "release-operation")
	raw, rawErr := decodeReleaseRecord[json.RawMessage](read.Values[1].Value, "release-render-input")
	render, renderErr := decodeReleaseRenderInput(raw)
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
		return releaseRecoveryProofExpectation{}, nil, corruptReleaseRecord()
	}
	if !recoveryRenderMatchesPredecessor(render, intent, assignment.RestorationAuthority) {
		return releaseRecoveryProofExpectation{}, nil, corruptReleaseRecord()
	}
	var expectation releaseRecoveryProofExpectation
	switch render.Strategy {
	case domain.StrategyRecreate:
		expectation.kind, expectation.priorTopologyArtifactID = releaseRecoveryProofRecreate, render.PriorArtifactID
	case domain.StrategyBlueGreen:
		expectation.kind = releaseRecoveryProofProxy
	default:
		return releaseRecoveryProofExpectation{}, nil, corruptReleaseRecord()
	}
	return expectation, []Condition{{Key: keys[0], ModRevision: read.Values[0].ModRevision},
		{Key: keys[1], ModRevision: read.Values[1].ModRevision},
		{Key: keys[2], ModRevision: read.Values[2].ModRevision}}, nil
}

func ordinaryRecoveryIntentMatchesAttempt(task TaskRecord, intent domain.Intent, head ReleaseOperationHead) bool {
	if head.OperationID != task.OperationID || head.PublicationID != task.Params[TaskReleasePublicationParam] ||
		head.EnvironmentID != task.Owner.EnvironmentID || head.LatestTaskID != task.ID || len(head.Attempts) == 0 ||
		intent.OriginatingTaskID != head.Attempts[0].TaskID {
		return false
	}
	expectedKind := domain.OperationDeploy
	if task.Type == TaskRollback {
		expectedKind = domain.OperationRollback
	} else if task.Type != TaskDeploy {
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
	render ReleaseRenderInput, intent domain.Intent, authority *ReleaseRestorationAuthority,
) bool {
	if authority == nil || authority.AppliedPredecessor == nil || render.PriorArtifactID == "" ||
		render.PriorWorkload == nil || intent.PriorServingReleaseID == "" {
		return false
	}
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(authority.AppliedPredecessor.ComposeArtifact, artifact) != nil {
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
