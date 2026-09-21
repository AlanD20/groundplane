package etcd

type platformResolverLiveLineage struct {
	priorObservationModRevision    int64
	priorObservationRevision       uint64
	predecessorTaskID              string
	expectedPreviousArtifactSHA256 string
	expectedPreviousArtifactID     string
	expectedPreviousGeneration     uint64
}

func platformResolverObservationForLineage(
	input PlatformComponentTaskRenderInput,
	lineage platformResolverLiveLineage,
	observation *ComponentObservationRecord,
) *ComponentObservationRecord {
	if observation != nil && observation.Revision == lineage.priorObservationRevision &&
		observation.TaskID == lineage.predecessorTaskID &&
		observation.CorefileSHA256 == lineage.expectedPreviousArtifactSHA256 &&
		(lineage.expectedPreviousArtifactSHA256 == "" || observation.DNSResolverProof != nil &&
			observation.DNSResolverProof.ArtifactID == lineage.expectedPreviousArtifactID &&
			observation.DNSResolverProof.RenderGeneration == lineage.expectedPreviousGeneration) {
		return observation
	}
	if lineage.expectedPreviousArtifactSHA256 == "" {
		return &ComponentObservationRecord{}
	}
	composeArtifact := input.ComposeArtifact
	if input.RollbackComposeArtifact != nil &&
		lineage.expectedPreviousArtifactID == input.ExpectedPreviousArtifactID {
		composeArtifact = input.RollbackComposeArtifact
	}
	return &ComponentObservationRecord{
		ComponentID: input.ComponentID, ServiceID: input.GeneratedServiceID,
		PlanID: input.OwnershipPlanID, ComposeArtifactID: input.ComposeArtifactID,
		Enabled: true, Healthy: true, RenderGeneration: lineage.expectedPreviousGeneration,
		OwnershipGeneration: input.OwnershipGeneration,
		CorefileSHA256:      lineage.expectedPreviousArtifactSHA256,
		TaskID:              lineage.predecessorTaskID, Revision: lineage.priorObservationRevision,
		DNSResolverProof: &TaskDNSResolverObservationEvidence{
			ComponentID: input.ComponentID, ServiceID: input.GeneratedServiceID,
			ArtifactID:       lineage.expectedPreviousArtifactID,
			ArtifactSHA256:   lineage.expectedPreviousArtifactSHA256,
			RenderGeneration: lineage.expectedPreviousGeneration,
		},
		ComposeArtifact: composeArtifact,
	}
}

func platformResolverFinalLiveLineage(
	predecessor TaskRecord,
	input PlatformComponentTaskRenderInput,
) (platformResolverLiveLineage, bool) {
	if predecessor.Result == nil || predecessor.Result.Kind != TaskResultCompose ||
		predecessor.Result.ReconciliationRequired {
		return platformResolverLiveLineage{}, false
	}
	result := predecessor.Result
	switch predecessor.Status {
	case TaskStatusCompleted:
		evidence := result.DNSResolverCandidateObservation
		if input.DisableService || evidence == nil || result.DNSResolverRollbackObservation != nil ||
			evidence.ComponentID != input.ComponentID || evidence.ServiceID != input.GeneratedServiceID ||
			evidence.ArtifactID != input.ArtifactID || evidence.ArtifactSHA256 != input.ArtifactSHA256 ||
			evidence.RenderGeneration != uint64(predecessor.RenderGeneration) {
			return platformResolverLiveLineage{}, false
		}
		return platformResolverLiveLineage{
			priorObservationRevision:       input.PriorObservationRevision + 1,
			predecessorTaskID:              predecessor.ID,
			expectedPreviousArtifactSHA256: evidence.ArtifactSHA256,
			expectedPreviousArtifactID:     evidence.ArtifactID,
			expectedPreviousGeneration:     evidence.RenderGeneration,
		}, true
	case TaskStatusFailed, TaskStatusTimedOut, TaskStatusAborted:
		evidence := result.DNSResolverRollbackObservation
		if input.ExpectedPreviousArtifactSHA256 == "" {
			if evidence != nil {
				return platformResolverLiveLineage{}, false
			}
		} else if evidence == nil || evidence.ComponentID != input.ComponentID ||
			evidence.ServiceID != input.GeneratedServiceID ||
			evidence.ArtifactID != input.ExpectedPreviousArtifactID ||
			evidence.ArtifactSHA256 != input.ExpectedPreviousArtifactSHA256 ||
			evidence.RenderGeneration != input.ExpectedPreviousGeneration {
			return platformResolverLiveLineage{}, false
		}
		return platformResolverLiveLineage{
			priorObservationModRevision:    input.PriorObservationModRevision,
			priorObservationRevision:       input.PriorObservationRevision,
			predecessorTaskID:              input.PredecessorTaskID,
			expectedPreviousArtifactSHA256: input.ExpectedPreviousArtifactSHA256,
			expectedPreviousArtifactID:     input.ExpectedPreviousArtifactID,
			expectedPreviousGeneration:     input.ExpectedPreviousGeneration,
		}, true
	default:
		return platformResolverLiveLineage{}, false
	}
}
