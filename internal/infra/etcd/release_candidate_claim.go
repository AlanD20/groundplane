package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"slices"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateReleaseCandidateDescriptor(
	descriptor executionplan.CandidateReleaseDescriptor,
	task TaskRecord,
	manifest ReleaseStagedManifest,
) (*agentpb.CandidateReleaseProcedure, error) {
	procedure, err := executionplan.OpenCandidateReleaseDescriptor(descriptor)
	if err != nil {
		return nil, corruptReleaseRecord()
	}
	planHash, err := hex.DecodeString(task.PlanHash)
	if err != nil || descriptor.PlanID != task.PlanID || !bytes.Equal(descriptor.PlanHash, planHash) ||
		descriptor.Operation != candidateReleaseTaskOperation(task) ||
		manifest.PublicationID != task.Params[TaskReleasePublicationParam] || manifest.OperationID != task.OperationID ||
		len(procedure.GetMembers()) != len(manifest.Members) {
		return nil, corruptReleaseRecord()
	}
	if !slices.Equal(descriptor.ComponentActionStepIDs, task.ComponentActionStepIDs) {
		return nil, corruptReleaseRecord()
	}
	manifestMembers := make(map[string]string, len(manifest.Members))
	for _, member := range manifest.Members {
		manifestMembers[member.ServiceID] = member.ReleaseID
	}
	taskSteps := make(map[string]struct{}, len(task.Steps))
	for _, step := range task.Steps {
		taskSteps[step.ID] = struct{}{}
	}
	if err := validateTaskConfigurationProcedure(task, procedure, taskSteps); err != nil {
		return nil, err
	}
	for _, id := range descriptor.ComponentActionStepIDs {
		if _, exists := taskSteps[id]; !exists {
			return nil, corruptReleaseRecord()
		}
	}
	for _, member := range procedure.GetMembers() {
		if manifestMembers[member.GetServiceId()] != member.GetCandidateReleaseId() {
			return nil, corruptReleaseRecord()
		}
		if member.GetCandidateArtifactId() != task.Params[TaskComposeArtifactParam] {
			return nil, corruptReleaseRecord()
		}
		stepIDs := append([]string(nil), member.GetForwardStepIds()...)
		if serving := member.GetServingPredecessor(); serving != nil {
			stepIDs = append(stepIDs, serving.GetProbeStepId(), serving.GetCompensateStepId())
		} else if absence := member.GetCandidateAbsence(); absence != nil {
			stepIDs = append(stepIDs, absence.GetProbeStepId(), absence.GetCompensateStepId())
		}
		for _, stepID := range stepIDs {
			if _, exists := taskSteps[stepID]; !exists {
				return nil, corruptReleaseRecord()
			}
		}
	}
	return procedure, nil
}

func (repository *TaskRepository) prepareOrdinaryRestorationAuthority(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (ReleaseRestorationAuthority, string, []etcdstore.Condition, error) {
	if task.Type == taskjournal.TaskUpdate || task.Params[TaskReleasePublicationParam] == "" {
		return ReleaseRestorationAuthority{}, "", nil, corruptTaskAssignment()
	}
	publicationID := task.Params[TaskReleasePublicationParam]
	keys := []string{
		releasePublicationKey(publicationID), releaseManifestStagingKey(publicationID),
		projectionrecord.EnvironmentComposeProjectionStorageKey(task.Owner.EnvironmentID),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return ReleaseRestorationAuthority{}, "", nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil {
		return ReleaseRestorationAuthority{}, "", nil, corruptReleaseRecord()
	}
	marker, markerErr := decodeReleaseRecord[ReleasePublicationMarker](read.Values[0].Value, "release-publication")
	manifest, manifestErr := decodeReleaseRecord[ReleaseStagedManifest](read.Values[1].Value, "release-staged-manifest")
	procedure, descriptorErr := validateReleaseCandidateMarker(task, marker, manifest)
	if markerErr != nil || manifestErr != nil || descriptorErr != nil {
		return ReleaseRestorationAuthority{}, "", nil, corruptReleaseRecord()
	}
	target := ReleaseRestorationTarget("")
	artifactID := ""
	for _, member := range procedure.GetMembers() {
		memberTarget := ReleaseRestorationCandidateAbsence
		if member.GetServingPredecessor() != nil {
			memberTarget = ReleaseRestorationServingPredecessor
		}
		if target == "" {
			target, artifactID = memberTarget, member.GetCandidateArtifactId()
		}
		if target != memberTarget || artifactID != member.GetCandidateArtifactId() {
			return ReleaseRestorationAuthority{}, "", nil, corruptReleaseRecord()
		}
	}
	candidates := make([]ReleaseRestorationCandidate, len(manifest.Members))
	for index, member := range manifest.Members {
		candidates[index] = ReleaseRestorationCandidate{
			ServiceID: member.ServiceID,
			ReleaseID: member.ReleaseID,
			Target:    target,
		}
	}
	sort.Slice(candidates, func(left, right int) bool {
		if candidates[left].ServiceID == candidates[right].ServiceID {
			return candidates[left].ReleaseID < candidates[right].ReleaseID
		}
		return candidates[left].ServiceID < candidates[right].ServiceID
	})
	authority := ReleaseRestorationAuthority{
		Schema: 1, TaskID: task.ID, OperationID: task.OperationID, PlanHash: task.PlanHash,
		EnvironmentID: task.Owner.EnvironmentID, CandidateArtifactID: artifactID, Candidates: candidates,
	}
	witnesses, sourceConditions, err := repository.ordinaryRestorationMembersAtRevision(
		ctx,
		task,
		manifest,
		procedure,
		revision,
	)
	if err != nil {
		return ReleaseRestorationAuthority{}, "", nil, err
	}
	authority.NativePredecessors = witnesses
	if err := validateNativeRestorationDescriptor(authority, procedure); err != nil {
		return ReleaseRestorationAuthority{}, "", nil, err
	}
	projectionRevision := int64(0)
	if read.Values[2] != nil {
		projectionRevision = read.Values[2].ModRevision
	}
	if target == ReleaseRestorationServingPredecessor {
		if read.Values[2] == nil {
			return ReleaseRestorationAuthority{}, "", nil, corruptReleaseRecord()
		}
		projection, decodeErr := projectionrecord.DecodeEnvironmentComposeProjectionStorage(read.Values[2].Value)
		if decodeErr != nil || projection.EnvironmentID != task.Owner.EnvironmentID ||
			len(projection.ComposeArtifact) == 0 {
			return ReleaseRestorationAuthority{}, "", nil, corruptReleaseRecord()
		}
		digest := sha256.Sum256(projection.ComposeArtifact)
		authority.AppliedPredecessor = &ReleaseAppliedPredecessorAuthority{
			KeyRevision: projectionRevision, RevisionID: projection.RevisionID,
			RenderGeneration: projection.RenderGeneration, ComposeArtifactSHA256: hex.EncodeToString(digest[:]),
			ComposeArtifact: append([]byte(nil), projection.ComposeArtifact...),
		}
	}
	digest, err := releaseRestorationAuthoritySHA256(authority)
	if err != nil {
		return ReleaseRestorationAuthority{}, "", nil, err
	}
	return authority, digest, append(sourceConditions, []etcdstore.Condition{
		{Key: keys[0], ModRevision: read.Values[0].ModRevision},
		{Key: keys[1], ModRevision: read.Values[1].ModRevision},
		{Key: keys[2], ModRevision: projectionRevision},
	}...), nil
}

func candidateReleaseTaskOperation(task TaskRecord) agentpb.PlanOperation {
	switch task.Type {
	case TaskDeploy:
		return agentpb.PlanOperation_PLAN_OPERATION_DEPLOY
	case TaskRollback:
		return agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK
	case TaskUpdate:
		if task.Params[TaskReleasePublicationParam] != "" {
			return agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
		}
	}
	return agentpb.PlanOperation_PLAN_OPERATION_UNSPECIFIED
}

func (repository *TaskRepository) prepareBlueprintRestorationAuthority(
	ctx context.Context,
	task TaskRecord,
	writer taskMaterializationWriterRecord,
	revision int64,
) (ReleaseRestorationAuthority, string, []etcdstore.Condition, error) {
	publicationID := task.Params[TaskReleasePublicationParam]
	keys := []string{
		releasePublicationKey(publicationID),
		releaseManifestStagingKey(publicationID),
		projectionrecord.EnvironmentComposeProjectionStorageKey(task.Owner.EnvironmentID),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return ReleaseRestorationAuthority{}, "", nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || writer.BlueprintAppliedPredecessor == nil {
		return ReleaseRestorationAuthority{}, "", nil, corruptReleaseRecord()
	}
	marker, markerErr := decodeReleaseRecord[ReleasePublicationMarker](read.Values[0].Value, "release-publication")
	manifest, manifestErr := decodeReleaseRecord[ReleaseStagedManifest](read.Values[1].Value, "release-staged-manifest")
	procedure, descriptorErr := validateReleaseCandidateDescriptor(marker.CandidateReleaseDescriptor, task, manifest)
	if markerErr != nil || manifestErr != nil || descriptorErr != nil ||
		validateBlueprintCandidateManifest(task, marker, manifest) != nil {
		return ReleaseRestorationAuthority{}, "", nil, corruptReleaseRecord()
	}
	observed, observedErr := taskMaterializationAppliedPredecessorFromValue(
		read.Values[2], task.Owner.EnvironmentID, task.RenderGeneration,
	)
	if observedErr != nil || observed != *writer.BlueprintAppliedPredecessor {
		return ReleaseRestorationAuthority{}, "", nil, errs.New(
			errs.KindStateConflict,
			"Blueprint applied predecessor changed during claim",
		)
	}
	var composeArtifact []byte
	if observed.Present {
		projection, decodeErr := projectionrecord.DecodeEnvironmentComposeProjectionStorage(read.Values[2].Value)
		if decodeErr != nil || projection.RevisionID != observed.RevisionID ||
			projection.RenderGeneration != observed.RenderGeneration {
			return ReleaseRestorationAuthority{}, "", nil, errs.New(
				errs.KindStateConflict,
				"Blueprint applied predecessor artifact changed during claim",
			)
		}
		composeArtifact = projection.ComposeArtifact
	}
	native, sourceConditions, err := repository.blueprintNativePredecessorsAtRevision(
		ctx,
		task,
		marker,
		manifest,
		revision,
	)
	if err != nil {
		return ReleaseRestorationAuthority{}, "", nil, err
	}
	authority, digest, err := buildBlueprintNativeRestorationAuthority(
		task,
		observed,
		native,
		manifest,
		procedure,
		composeArtifact,
	)
	if err != nil {
		return ReleaseRestorationAuthority{}, "", nil, err
	}
	if err := validateSelectedRestorationTargets(procedure, authority.Candidates); err != nil {
		return ReleaseRestorationAuthority{}, "", nil, err
	}
	projectionRevision := int64(0)
	if read.Values[2] != nil {
		projectionRevision = read.Values[2].ModRevision
	}
	conditions := []etcdstore.Condition{
		{Key: keys[0], ModRevision: read.Values[0].ModRevision},
		{Key: keys[1], ModRevision: read.Values[1].ModRevision},
		{Key: keys[2], ModRevision: projectionRevision},
	}
	return authority, digest, append(conditions, sourceConditions...), nil
}

func validateSelectedRestorationTargets(
	procedure *agentpb.CandidateReleaseProcedure,
	candidates []ReleaseRestorationCandidate,
) error {
	if len(candidates) != len(procedure.GetMembers()) {
		return corruptTaskAssignment()
	}
	for index, member := range procedure.GetMembers() {
		candidate := candidates[index]
		if candidate.ServiceID != member.GetServiceId() || candidate.ReleaseID != member.GetCandidateReleaseId() ||
			candidate.Target == ReleaseRestorationServingPredecessor && member.GetServingPredecessor() == nil ||
			candidate.Target == ReleaseRestorationCandidateAbsence && member.GetCandidateAbsence() == nil ||
			candidate.Target != ReleaseRestorationServingPredecessor &&
				candidate.Target != ReleaseRestorationCandidateAbsence {
			return corruptTaskAssignment()
		}
	}
	return nil
}

func validateReleaseCandidateMarker(
	task TaskRecord,
	marker ReleasePublicationMarker,
	manifest ReleaseStagedManifest,
) (*agentpb.CandidateReleaseProcedure, error) {
	publicationID := task.Params[TaskReleasePublicationParam]
	if validatePublicationID(publicationID) != nil || marker.PublicationID != publicationID ||
		marker.OperationID != task.OperationID || marker.ManifestDigest != manifest.Digest ||
		manifest.PublicationID != publicationID || manifest.OperationID != task.OperationID ||
		marker.PublishedAt.IsZero() || marker.PublishedAt.Location() != time.UTC ||
		len(manifest.Members) == 0 || len(manifest.Members) > maximumReleasePublicationMembers {
		return nil, corruptReleaseRecord()
	}
	digest, err := blueprintCandidateManifestDigest(manifest)
	if err != nil || digest != manifest.Digest {
		return nil, corruptReleaseRecord()
	}
	procedure, err := validateReleaseCandidateDescriptor(marker.CandidateReleaseDescriptor, task, manifest)
	if err != nil {
		return nil, err
	}
	if task.Type == taskjournal.TaskUpdate {
		if err := validateBlueprintNativePredecessorReferences(marker.NativePredecessors, procedure); err != nil {
			return nil, err
		}
	} else if len(marker.NativePredecessors) != 0 {
		return nil, corruptReleaseRecord()
	}
	return procedure, nil
}

func (repository *TaskRepository) candidateReleaseDescriptorAtRevision(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (executionplan.CandidateReleaseDescriptor, *agentpb.CandidateReleaseProcedure, error) {
	publicationID := task.Params[TaskReleasePublicationParam]
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		releasePublicationKey(publicationID), releaseManifestStagingKey(publicationID),
	}, Revision: revision})
	if err != nil {
		return executionplan.CandidateReleaseDescriptor{}, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 || read.Values[0] == nil ||
		read.Values[1] == nil {
		return executionplan.CandidateReleaseDescriptor{}, nil, corruptReleaseRecord()
	}
	marker, markerErr := decodeReleaseRecord[ReleasePublicationMarker](read.Values[0].Value, "release-publication")
	manifest, manifestErr := decodeReleaseRecord[ReleaseStagedManifest](read.Values[1].Value, "release-staged-manifest")
	if markerErr != nil || manifestErr != nil {
		return executionplan.CandidateReleaseDescriptor{}, nil, corruptReleaseRecord()
	}
	procedure, err := validateReleaseCandidateMarker(task, marker, manifest)
	if err != nil {
		return executionplan.CandidateReleaseDescriptor{}, nil, err
	}
	return executionplan.CloneCandidateReleaseDescriptor(marker.CandidateReleaseDescriptor), procedure, nil
}

// CandidateReleaseDescriptor returns the exact marker-owned descriptor at the
// caller's fixed assignment revision for Controller dispatch validation.
func (repository *TaskRepository) CandidateReleaseDescriptor(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (executionplan.CandidateReleaseDescriptor, error) {
	descriptor, _, err := repository.candidateReleaseDescriptorAtRevision(ctx, task, revision)
	return descriptor, err
}

func validateAssignmentRestorationDescriptor(
	task TaskRecord,
	assignment TaskAssignmentRecord,
	procedure *agentpb.CandidateReleaseProcedure,
) error {
	authority := assignment.RestorationAuthority
	if authority == nil || validateReleaseRestorationAuthority(*authority) != nil ||
		authority.TaskID != task.ID || authority.OperationID != task.OperationID || authority.PlanHash != task.PlanHash ||
		len(authority.Candidates) != len(procedure.GetMembers()) {
		return corruptTaskAssignment()
	}
	if err := validateSelectedRestorationTargets(procedure, authority.Candidates); err != nil {
		return err
	}
	for index, member := range procedure.GetMembers() {
		candidate := authority.Candidates[index]
		if candidate.ServiceID != member.GetServiceId() || candidate.ReleaseID != member.GetCandidateReleaseId() ||
			authority.CandidateArtifactID != member.GetCandidateArtifactId() {
			return corruptTaskAssignment()
		}
	}
	if taskHasBlueprintCandidateAppliedAuthority(task) || task.Type == taskjournal.TaskDeploy || task.Type == taskjournal.TaskRollback {
		if err := validateNativeRestorationDescriptor(*authority, procedure); err != nil {
			return err
		}
	} else if len(authority.NativePredecessors) != 0 {
		return corruptTaskAssignment()
	}
	digest, err := releaseRestorationAuthoritySHA256(*authority)
	if err != nil || digest != assignment.RestorationAuthoritySHA256 {
		return corruptTaskAssignment()
	}
	return nil
}
