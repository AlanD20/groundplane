package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const releaseRecoveryPrefix = "/v1/records/release-recoveries/"

type TaskExecutionMode string

const (
	TaskExecutionModeForward      TaskExecutionMode = "forward"
	TaskExecutionModeRecoveryOnly TaskExecutionMode = "recovery_only"
)

type ReleaseRestorationTarget string

const (
	ReleaseRestorationServingPredecessor ReleaseRestorationTarget = "serving_predecessor"
	ReleaseRestorationCandidateAbsence   ReleaseRestorationTarget = "candidate_absence"
)

type ReleaseRestorationCandidate struct {
	ServiceID string `json:"service_id"`
	ReleaseID string `json:"release_id"`
}

type ReleaseServingPredecessorAuthority struct {
	KeyRevision           int64  `json:"key_revision"`
	RevisionID            string `json:"revision_id"`
	RenderGeneration      uint64 `json:"render_generation"`
	ComposeArtifactSHA256 string `json:"compose_artifact_sha256"`
	ComposeArtifact       []byte `json:"compose_artifact"`
}

// ReleaseRestorationAuthority is the one concrete claim-time authority. It is
// embedded byte-identically in every assignment copy and is never duplicated
// into a recovery record.
type ReleaseRestorationAuthority struct {
	Schema              int                                 `json:"schema"`
	Target              ReleaseRestorationTarget            `json:"target"`
	TaskID              string                              `json:"task_id"`
	OperationID         string                              `json:"operation_id"`
	PlanHash            string                              `json:"plan_hash"`
	EnvironmentID       string                              `json:"environment_id"`
	CandidateArtifactID string                              `json:"candidate_artifact_id"`
	Candidates          []ReleaseRestorationCandidate       `json:"candidates"`
	ServingPredecessor  *ReleaseServingPredecessorAuthority `json:"serving_predecessor,omitempty"`
}

type ReleaseRecoveryPhase string

const (
	ReleaseRecoveryPhaseProbe      ReleaseRecoveryPhase = "probe"
	ReleaseRecoveryPhaseCompensate ReleaseRecoveryPhase = "compensate"
	ReleaseRecoveryPhaseProven     ReleaseRecoveryPhase = "proven"
)

type releaseRecoveryRecord struct {
	Schema                     int                               `json:"schema"`
	TaskID                     string                            `json:"task_id"`
	AssignmentID               string                            `json:"assignment_id"`
	OperationID                string                            `json:"operation_id"`
	PlanHash                   string                            `json:"plan_hash"`
	RestorationAuthoritySHA256 string                            `json:"restoration_authority_sha256"`
	PrimaryReportSHA256        string                            `json:"primary_report_sha256"`
	PrimaryStatus              TaskStatus                        `json:"primary_status"`
	PrimaryResult              TaskResultRecord                  `json:"primary_result"`
	RecoveryDeadline           time.Time                         `json:"recovery_deadline"`
	MutationEvidence           []releaseRecoveryMutationEvidence `json:"mutation_evidence"`
	RecoveryStepIDs            []string                          `json:"recovery_step_ids"`
	Cursor                     uint32                            `json:"cursor"`
	Phase                      ReleaseRecoveryPhase              `json:"phase"`
	EvidenceRevision           int64                             `json:"evidence_revision"`
}

type releaseRecoveryMutationEvidence struct {
	StepID    string `json:"step_id"`
	Running   bool   `json:"running"`
	Completed bool   `json:"completed"`
}

func releaseRecoveryKey(taskID string) string { return releaseRecoveryPrefix + taskID }

func releaseRestorationAuthoritySHA256(authority ReleaseRestorationAuthority) (string, error) {
	if err := validateReleaseRestorationAuthority(authority); err != nil {
		return "", err
	}
	return domain.Digest(authority)
}

func validateReleaseRestorationAuthority(authority ReleaseRestorationAuthority) error {
	if authority.Schema != 1 || ids.Validate(ids.KindTask, authority.TaskID) != nil ||
		ids.Validate(ids.KindOperation, authority.OperationID) != nil || !validSHA256(authority.PlanHash) ||
		ids.Validate(ids.KindEnvironment, authority.EnvironmentID) != nil ||
		ids.Validate(ids.KindConfig, authority.CandidateArtifactID) != nil || len(authority.Candidates) == 0 {
		return corruptTaskAssignment()
	}
	previous := ""
	for _, candidate := range authority.Candidates {
		identity := candidate.ServiceID + "\x00" + candidate.ReleaseID
		if ids.Validate(ids.KindService, candidate.ServiceID) != nil || ids.Validate(ids.KindDeployment, candidate.ReleaseID) != nil || identity <= previous {
			return corruptTaskAssignment()
		}
		previous = identity
	}
	switch authority.Target {
	case ReleaseRestorationServingPredecessor:
		predecessor := authority.ServingPredecessor
		if predecessor == nil || predecessor.KeyRevision <= 0 || ids.Validate(ids.KindTask, predecessor.RevisionID) != nil ||
			predecessor.RenderGeneration == 0 || !validSHA256(predecessor.ComposeArtifactSHA256) ||
			len(predecessor.ComposeArtifact) == 0 || len(predecessor.ComposeArtifact) > MaximumTaskRecordBytes {
			return corruptTaskAssignment()
		}
		digest := sha256.Sum256(predecessor.ComposeArtifact)
		if hex.EncodeToString(digest[:]) != predecessor.ComposeArtifactSHA256 {
			return corruptTaskAssignment()
		}
	case ReleaseRestorationCandidateAbsence:
		if authority.ServingPredecessor != nil {
			return corruptTaskAssignment()
		}
	default:
		return corruptTaskAssignment()
	}
	return nil
}

func validateReleaseRecoveryRecord(record releaseRecoveryRecord) error {
	if record.Schema != 1 || ids.Validate(ids.KindTask, record.TaskID) != nil ||
		ids.Validate(ids.KindAssignment, record.AssignmentID) != nil || ids.Validate(ids.KindOperation, record.OperationID) != nil ||
		!validSHA256(record.PlanHash) || !validSHA256(record.RestorationAuthoritySHA256) ||
		!validSHA256(record.PrimaryReportSHA256) || record.PrimaryStatus == TaskStatusCompleted ||
		!validTerminalTaskStatus(record.PrimaryStatus) || len(record.RecoveryStepIDs) == 0 ||
		!record.RecoveryDeadline.Equal(record.RecoveryDeadline.UTC()) || record.RecoveryDeadline.IsZero() ||
		int(record.Cursor) > len(record.RecoveryStepIDs) || record.EvidenceRevision <= 0 {
		return errs.New(errs.KindInternal, "release recovery record is corrupt")
	}
	for index, stepID := range record.RecoveryStepIDs {
		if ids.Validate(ids.KindStep, stepID) != nil || slices.Contains(record.RecoveryStepIDs[:index], stepID) {
			return errs.New(errs.KindInternal, "release recovery record is corrupt")
		}
	}
	for index, evidence := range record.MutationEvidence {
		if ids.Validate(ids.KindStep, evidence.StepID) != nil || !evidence.Running ||
			evidence.Completed && !evidence.Running {
			return errs.New(errs.KindInternal, "release recovery record is corrupt")
		}
		for _, prior := range record.MutationEvidence[:index] {
			if prior.StepID == evidence.StepID {
				return errs.New(errs.KindInternal, "release recovery record is corrupt")
			}
		}
	}
	probeCount := len(record.RecoveryStepIDs) / 2
	if probeCount == 0 || len(record.RecoveryStepIDs)%2 != 0 {
		return errs.New(errs.KindInternal, "release recovery record is corrupt")
	}
	switch record.Phase {
	case ReleaseRecoveryPhaseProbe:
		if int(record.Cursor) >= probeCount {
			return errs.New(errs.KindInternal, "release recovery record is corrupt")
		}
	case ReleaseRecoveryPhaseCompensate:
		if int(record.Cursor) < probeCount || int(record.Cursor) >= len(record.RecoveryStepIDs) {
			return errs.New(errs.KindInternal, "release recovery record is corrupt")
		}
	case ReleaseRecoveryPhaseProven:
		if int(record.Cursor) != len(record.RecoveryStepIDs) {
			return errs.New(errs.KindInternal, "release recovery record is corrupt")
		}
	default:
		return errs.New(errs.KindInternal, "release recovery record is corrupt")
	}
	reportDigest, err := domain.Digest(struct {
		Status TaskStatus       `json:"status"`
		Result TaskResultRecord `json:"result"`
	}{record.PrimaryStatus, record.PrimaryResult})
	if err != nil || reportDigest != record.PrimaryReportSHA256 {
		return errs.New(errs.KindInternal, "release recovery record is corrupt")
	}
	return nil
}

func encodeReleaseRecoveryRecord(record releaseRecoveryRecord) ([]byte, error) {
	if err := validateReleaseRecoveryRecord(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("release-recovery", record)
}

func decodeReleaseRecoveryRecord(value []byte) (releaseRecoveryRecord, error) {
	record, err := decodeEnvelope[releaseRecoveryRecord](value, "release-recovery")
	if err != nil || validateReleaseRecoveryRecord(record) != nil {
		return releaseRecoveryRecord{}, errs.New(errs.KindInternal, "release recovery record is corrupt")
	}
	return record, nil
}

func releaseRecoveryRecordSHA256(record releaseRecoveryRecord) (string, error) {
	if err := validateReleaseRecoveryRecord(record); err != nil {
		return "", err
	}
	// Cursor, phase, and their evidence revision are mutable progress. The
	// assignment carries the digest of the immutable recovery authority so one
	// dispatch digest remains valid while AppendTaskEvent advances the record.
	return domain.Digest(struct {
		Schema                     int                               `json:"schema"`
		TaskID                     string                            `json:"task_id"`
		AssignmentID               string                            `json:"assignment_id"`
		OperationID                string                            `json:"operation_id"`
		PlanHash                   string                            `json:"plan_hash"`
		RestorationAuthoritySHA256 string                            `json:"restoration_authority_sha256"`
		PrimaryReportSHA256        string                            `json:"primary_report_sha256"`
		PrimaryStatus              TaskStatus                        `json:"primary_status"`
		PrimaryResult              TaskResultRecord                  `json:"primary_result"`
		RecoveryDeadline           time.Time                         `json:"recovery_deadline"`
		MutationEvidence           []releaseRecoveryMutationEvidence `json:"mutation_evidence"`
		RecoveryStepIDs            []string                          `json:"recovery_step_ids"`
	}{
		record.Schema, record.TaskID, record.AssignmentID, record.OperationID,
		record.PlanHash, record.RestorationAuthoritySHA256, record.PrimaryReportSHA256,
		record.PrimaryStatus, record.PrimaryResult, record.RecoveryDeadline, record.MutationEvidence, record.RecoveryStepIDs,
	})
}

func (repository *TaskRepository) createReleaseRecoveryRecord(ctx context.Context, record releaseRecoveryRecord) (Versioned[releaseRecoveryRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[releaseRecoveryRecord]{}, err
	}
	value, err := encodeReleaseRecoveryRecord(record)
	if err != nil {
		return Versioned[releaseRecoveryRecord]{}, err
	}
	defer clear(value)
	key := releaseRecoveryKey(record.TaskID)
	result, err := repository.store.Transact(ctx, []Condition{{Key: key}}, []Mutation{{Type: MutationPut, Key: key, Value: value}})
	if err != nil {
		return Versioned[releaseRecoveryRecord]{}, err
	}
	if result.Succeeded {
		return Versioned[releaseRecoveryRecord]{Record: record, Revision: result.Revision, ReadRevision: result.Revision}, nil
	}
	read, err := repository.store.Get(ctx, key)
	if err != nil || read.Entry == nil {
		return Versioned[releaseRecoveryRecord]{}, errs.New(errs.KindStateConflict, "release recovery record creation conflicted")
	}
	stored, decodeErr := decodeReleaseRecoveryRecord(read.Entry.Value)
	storedValue, encodeErr := encodeReleaseRecoveryRecord(stored)
	if decodeErr != nil || encodeErr != nil || !bytes.Equal(value, storedValue) {
		clear(storedValue)
		return Versioned[releaseRecoveryRecord]{}, errs.New(errs.KindStateConflict, "release recovery record creation conflicted")
	}
	clear(storedValue)
	return Versioned[releaseRecoveryRecord]{Record: stored, Revision: read.Entry.ModRevision, ReadRevision: read.ReadRevision}, nil
}

func releaseRestorationStepIDs(
	procedure *agentpb.CandidateReleaseProcedure,
	target ReleaseRestorationTarget,
) ([]string, error) {
	stepIDs := make([]string, 0, len(procedure.GetMembers())*2)
	for _, member := range procedure.GetMembers() {
		switch target {
		case ReleaseRestorationServingPredecessor:
			selected := member.GetServingPredecessor()
			if selected == nil {
				return nil, corruptTaskAssignment()
			}
			stepIDs = append(stepIDs, selected.GetProbeStepId())
		case ReleaseRestorationCandidateAbsence:
			selected := member.GetCandidateAbsence()
			if selected == nil {
				return nil, corruptTaskAssignment()
			}
			stepIDs = append(stepIDs, selected.GetProbeStepId())
		default:
			return nil, corruptTaskAssignment()
		}
	}
	for index := len(procedure.GetMembers()) - 1; index >= 0; index-- {
		member := procedure.GetMembers()[index]
		switch target {
		case ReleaseRestorationServingPredecessor:
			stepIDs = append(stepIDs, member.GetServingPredecessor().GetCompensateStepId())
		case ReleaseRestorationCandidateAbsence:
			stepIDs = append(stepIDs, member.GetCandidateAbsence().GetCompensateStepId())
		}
	}
	return stepIDs, nil
}

func advanceReleaseRecoveryRecord(record releaseRecoveryRecord, input TaskEventInput, revision int64) (releaseRecoveryRecord, bool, error) {
	if err := validateReleaseRecoveryRecord(record); err != nil || record.Phase == ReleaseRecoveryPhaseProven ||
		int(record.Cursor) >= len(record.RecoveryStepIDs) || input.Identity.StepID != record.RecoveryStepIDs[record.Cursor] {
		return releaseRecoveryRecord{}, false, errs.New(errs.KindStateConflict, "release recovery event is not the canonical next step")
	}
	switch input.State {
	case TaskEventStateRunning, TaskEventStateFailed, TaskEventStateAborted, TaskEventStateTimedOut:
		return record, false, nil
	case TaskEventStateCompleted:
		next := record
		next.Cursor++
		next.EvidenceRevision = revision
		probeCount := uint32(len(next.RecoveryStepIDs) / 2)
		switch {
		case int(next.Cursor) == len(next.RecoveryStepIDs):
			next.Phase = ReleaseRecoveryPhaseProven
		case next.Cursor >= probeCount:
			next.Phase = ReleaseRecoveryPhaseCompensate
		default:
			next.Phase = ReleaseRecoveryPhaseProbe
		}
		if err := validateReleaseRecoveryRecord(next); err != nil {
			return releaseRecoveryRecord{}, false, err
		}
		return next, true, nil
	default:
		return releaseRecoveryRecord{}, false, errs.New(errs.KindStateConflict, "release recovery event state is invalid")
	}
}

func (repository *TaskRepository) releaseRecoveryDirectiveAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment TaskAssignmentRecord,
	procedure *agentpb.CandidateReleaseProcedure,
	revision int64,
) (*ReleaseRecoveryDirective, error) {
	if assignment.ExecutionMode != TaskExecutionModeRecoveryOnly || !validSHA256(assignment.ReleaseRecoveryRecordSHA256) {
		return nil, corruptTaskAssignment()
	}
	read, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{releaseRecoveryKey(task.ID)}, Revision: revision,
	})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, corruptTaskAssignment()
	}
	record, err := decodeReleaseRecoveryRecord(read.Values[0].Value)
	if err != nil || record.TaskID != task.ID || record.AssignmentID != assignment.AssignmentID ||
		record.OperationID != task.OperationID || record.PlanHash != task.PlanHash ||
		record.RestorationAuthoritySHA256 != assignment.RestorationAuthoritySHA256 ||
		!record.RecoveryDeadline.Equal(assignment.RecoveryDeadline) ||
		record.EvidenceRevision > revision {
		return nil, corruptTaskAssignment()
	}
	digest, err := releaseRecoveryRecordSHA256(record)
	if err != nil || digest != assignment.ReleaseRecoveryRecordSHA256 {
		return nil, corruptTaskAssignment()
	}
	wantSteps, err := releaseRestorationStepIDs(procedure, assignment.RestorationAuthority.Target)
	if err != nil || !slices.Equal(wantSteps, record.RecoveryStepIDs) {
		return nil, corruptTaskAssignment()
	}
	applicable, err := releaseApplicableCompensationStepIDs(procedure, assignment.RestorationAuthority.Target, record.MutationEvidence)
	if err != nil {
		return nil, err
	}
	return &ReleaseRecoveryDirective{
		Phase: record.Phase, StepIDs: slices.Clone(record.RecoveryStepIDs), Cursor: record.Cursor,
		RecordSHA256: digest, ApplicableCompensationStepIDs: applicable,
	}, nil
}

func releaseApplicableCompensationStepIDs(
	procedure *agentpb.CandidateReleaseProcedure,
	target ReleaseRestorationTarget,
	evidence []releaseRecoveryMutationEvidence,
) ([]string, error) {
	completed := make(map[string]struct{})
	evidenceIndex := 0
	for _, member := range procedure.GetMembers() {
		for _, stepID := range member.GetForwardStepIds() {
			if evidenceIndex >= len(evidence) || evidence[evidenceIndex].StepID != stepID {
				continue
			}
			if evidence[evidenceIndex].Completed {
				completed[stepID] = struct{}{}
			}
			evidenceIndex++
		}
	}
	if evidenceIndex != len(evidence) {
		return nil, corruptTaskAssignment()
	}
	result := make([]string, 0, len(procedure.GetMembers()))
	for index := len(procedure.GetMembers()) - 1; index >= 0; index-- {
		member := procedure.GetMembers()[index]
		memberCompleted := false
		for _, stepID := range member.GetForwardStepIds() {
			if _, ok := completed[stepID]; ok {
				memberCompleted = true
				break
			}
		}
		if !memberCompleted {
			continue
		}
		var compensationStepID string
		switch target {
		case ReleaseRestorationServingPredecessor:
			compensationStepID = member.GetServingPredecessor().GetCompensateStepId()
		case ReleaseRestorationCandidateAbsence:
			compensationStepID = member.GetCandidateAbsence().GetCompensateStepId()
		default:
			return nil, corruptTaskAssignment()
		}
		if compensationStepID == "" {
			return nil, corruptTaskAssignment()
		}
		result = append(result, compensationStepID)
	}
	return result, nil
}

func buildBlueprintRestorationAuthority(
	task TaskRecord,
	predecessor taskMaterializationAppliedPredecessor,
	manifest ReleaseStagedManifest,
	predecessorComposeArtifact []byte,
) (ReleaseRestorationAuthority, string, error) {
	if !taskHasBlueprintCandidateAppliedAuthority(task) || manifest.PublicationID != task.Params[TaskReleasePublicationParam] ||
		manifest.OperationID != task.OperationID || len(manifest.Members) == 0 || predecessor.Present != (len(predecessorComposeArtifact) != 0) {
		return ReleaseRestorationAuthority{}, "", corruptTaskAssignment()
	}
	candidates := make([]ReleaseRestorationCandidate, len(manifest.Members))
	for index, member := range manifest.Members {
		candidates[index] = ReleaseRestorationCandidate{ServiceID: member.ServiceID, ReleaseID: member.ReleaseID}
	}
	sort.Slice(candidates, func(left, right int) bool {
		if candidates[left].ServiceID == candidates[right].ServiceID {
			return candidates[left].ReleaseID < candidates[right].ReleaseID
		}
		return candidates[left].ServiceID < candidates[right].ServiceID
	})
	authority := ReleaseRestorationAuthority{
		Schema: 1, TaskID: task.ID, OperationID: task.OperationID, PlanHash: task.PlanHash,
		EnvironmentID: task.Owner.EnvironmentID, CandidateArtifactID: task.Params[TaskComposeArtifactParam], Candidates: candidates,
		Target: ReleaseRestorationCandidateAbsence,
	}
	if predecessor.Present {
		artifactDigest := sha256.Sum256(predecessorComposeArtifact)
		authority.Target = ReleaseRestorationServingPredecessor
		authority.ServingPredecessor = &ReleaseServingPredecessorAuthority{
			KeyRevision: predecessor.KeyRevision, RevisionID: predecessor.RevisionID,
			RenderGeneration: predecessor.RenderGeneration, ComposeArtifactSHA256: hex.EncodeToString(artifactDigest[:]),
			ComposeArtifact: slices.Clone(predecessorComposeArtifact),
		}
	}
	digest, err := releaseRestorationAuthoritySHA256(authority)
	if err != nil {
		return ReleaseRestorationAuthority{}, "", err
	}
	return authority, digest, nil
}

func validTerminalTaskStatus(status TaskStatus) bool {
	switch status {
	case TaskStatusFailed, TaskStatusAborted, TaskStatusTimedOut:
		return true
	default:
		return false
	}
}

func canonicalPrimaryReportSHA256(status TaskStatus, result TaskResultRecord) (string, error) {
	return domain.Digest(struct {
		Status TaskStatus       `json:"status"`
		Result TaskResultRecord `json:"result"`
	}{status, result})
}

func cloneReleaseRestorationAuthority(value *ReleaseRestorationAuthority) *ReleaseRestorationAuthority {
	if value == nil {
		return nil
	}
	clone := *value
	clone.Candidates = slices.Clone(value.Candidates)
	if value.ServingPredecessor != nil {
		predecessor := *value.ServingPredecessor
		predecessor.ComposeArtifact = slices.Clone(value.ServingPredecessor.ComposeArtifact)
		clone.ServingPredecessor = &predecessor
	}
	return &clone
}
