package agentchannel

import (
	"bytes"
	"context"
	"encoding/hex"

	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type candidateReleaseWireAuthority struct {
	mode           agentpb.TaskExecutionMode
	restoration    *agentpb.ReleaseRestorationAuthority
	recovery       *agentpb.ReleaseRecoveryDirective
	recoveryDigest []byte
}

type candidateReleaseDescriptorStore interface {
	CandidateReleaseDescriptor(
		context.Context,
		etcd.TaskRecord,
		int64,
	) (executionplan.CandidateReleaseDescriptor, error)
}

func validateCandidateReleaseAssignment(
	ctx context.Context,
	store TaskStore,
	claim etcd.TaskAssignment,
	plan *agentpb.ExecutionPlan,
) error {
	if claim.Task.Record.Params[releaserender.TaskReleasePublicationParam] == "" {
		return nil
	}
	descriptors, ok := store.(candidateReleaseDescriptorStore)
	if !ok {
		return errs.New(errs.KindInternal, "candidate Release descriptor store is not configured")
	}
	descriptor, err := descriptors.CandidateReleaseDescriptor(ctx, claim.Task.Record, claim.Task.ReadRevision)
	if err != nil {
		return err
	}
	if err := executionplan.CandidateReleaseDescriptorMatchesPlan(descriptor, plan); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func candidateReleaseAssignmentAuthority(
	claim etcd.TaskAssignment,
	plan *agentpb.ExecutionPlan,
) (candidateReleaseWireAuthority, error) {
	record := claim.Assignment.Record
	result := candidateReleaseWireAuthority{}
	switch record.ExecutionMode {
	case taskassignments.TaskExecutionModeForward:
		result.mode = agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD
		if record.ReleaseRecoveryRecordSHA256 != "" {
			return result, errs.New(errs.KindInternal, "forward assignment carries recovery record authority")
		}
	case taskassignments.TaskExecutionModeRecoveryOnly:
		result.mode = agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY
		digest, err := hex.DecodeString(record.ReleaseRecoveryRecordSHA256)
		if err != nil || len(digest) != 32 {
			return result, errs.New(errs.KindInternal, "recovery assignment digest is invalid")
		}
		result.recoveryDigest = digest
	default:
		return result, errs.New(errs.KindInternal, "durable assignment execution mode is invalid")
	}
	procedure := plan.GetCandidateReleaseProcedure()
	if procedure == nil {
		if record.AttachRemovalAuthoritySHA256 != "" {
			if record.RestorationAuthority != nil ||
				record.ServiceAbsenceProcedureSHA256 != "" || record.ManagedNetworkProcedureSHA256 != "" {
				return result, errs.New(errs.KindInternal, "Detach assignment carries foreign recovery authority")
			}
			if _, err := executionplan.AttachRemovalRecoveryStepIDs(plan); err != nil {
				return result, err
			}
			if result.mode == agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY {
				directive := claim.AttachRemovalRecovery
				if directive == nil || directive.RecordSHA256 != record.ReleaseRecoveryRecordSHA256 {
					return result, errs.New(errs.KindInternal, "durable Detach recovery cursor is missing")
				}
				phase := agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_COMPENSATE
				if directive.Phase == taskassignments.ReleaseRecoveryPhaseProven {
					phase = agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROVEN
				}
				result.recovery = &agentpb.ReleaseRecoveryDirective{
					StepIds: append([]string(nil), directive.StepIDs...), Cursor: directive.Cursor, Phase: phase,
					ReleaseRecoveryRecordSha256: append([]byte(nil), result.recoveryDigest...),
				}
				if err := executionplan.ValidateAttachRemovalRecoveryDirective(plan, result.recovery, result.recoveryDigest); err != nil {
					return result, err
				}
			}
			return result, nil
		}
		if network := plan.GetManagedNetworkRecoveryProcedure(); network != nil {
			digest, digestErr := taskassignments.ManagedNetworkRecoveryProcedureSHA256(network)
			if digestErr != nil || digest != record.ManagedNetworkProcedureSHA256 ||
				record.RestorationAuthority != nil || record.RestorationAuthoritySHA256 != "" ||
				record.ServiceAbsenceProcedureSHA256 != "" {
				return result, errs.New(errs.KindInternal, "managed network recovery assignment authority is invalid")
			}
			if result.mode == agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY {
				directive := claim.ManagedNetworkRecovery
				if directive == nil || directive.RecordSHA256 != record.ReleaseRecoveryRecordSHA256 ||
					etcd.ValidateManagedNetworkRecoveryDirective(network, directive) != nil {
					return result, errs.New(errs.KindInternal, "durable managed network recovery directive is missing")
				}
				phase := agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_COMPENSATE
				if directive.Phase == taskassignments.ReleaseRecoveryPhaseProbe {
					phase = agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROBE
				}
				result.recovery = &agentpb.ReleaseRecoveryDirective{
					StepIds: append([]string(nil), directive.StepIDs...), Cursor: directive.Cursor,
					Phase:                       phase,
					ReleaseRecoveryRecordSha256: append([]byte(nil), result.recoveryDigest...),
				}
			}
			return result, nil
		}
		if absence := plan.GetServiceAbsenceRecoveryProcedure(); absence != nil {
			digest, digestErr := taskassignments.ServiceAbsenceProcedureSHA256(absence)
			if digestErr != nil || digest != record.ServiceAbsenceProcedureSHA256 ||
				record.RestorationAuthority != nil ||
				record.RestorationAuthoritySHA256 != "" {
				return result, errs.New(errs.KindInternal, "Service absence recovery assignment authority is invalid")
			}
			if result.mode == agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY {
				directive := claim.ServiceAbsenceRecovery
				if directive == nil || directive.RecordSHA256 != record.ReleaseRecoveryRecordSHA256 {
					return result, errs.New(errs.KindInternal, "durable Service absence recovery directive is missing")
				}
				phase := agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_UNSPECIFIED
				switch directive.Phase {
				case taskassignments.ReleaseRecoveryPhaseProbe:
					phase = agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROBE
				case taskassignments.ReleaseRecoveryPhaseCompensate:
					phase = agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_COMPENSATE
				case taskassignments.ReleaseRecoveryPhaseProven:
					phase = agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROVEN
				default:
					return result, errs.New(errs.KindInternal, "durable Service absence recovery phase is invalid")
				}
				result.recovery = &agentpb.ReleaseRecoveryDirective{
					StepIds: append([]string(nil), directive.StepIDs...), Cursor: directive.Cursor,
					Phase:                         phase,
					ReleaseRecoveryRecordSha256:   append([]byte(nil), result.recoveryDigest...),
					ApplicableCompensationStepIds: append([]string(nil), directive.ApplicableCompensationStepIDs...),
				}
			}
			return result, nil
		}
		if record.RestorationAuthority != nil || record.RestorationAuthoritySHA256 != "" ||
			len(result.recoveryDigest) != 0 ||
			record.ServiceAbsenceProcedureSHA256 != "" || record.ManagedNetworkProcedureSHA256 != "" {
			return result, errs.New(errs.KindInternal, "non-release assignment carries restoration authority")
		}
		return result, nil
	}
	authority := record.RestorationAuthority
	if record.ServiceAbsenceProcedureSHA256 != "" ||
		record.ManagedNetworkProcedureSHA256 != "" || record.AttachRemovalAuthoritySHA256 != "" {
		return result, errs.New(errs.KindInternal, "candidate Release assignment carries foreign recovery authority")
	}
	authorityDigest, err := hex.DecodeString(record.RestorationAuthoritySHA256)
	if authority == nil || err != nil || len(authorityDigest) != 32 || authority.TaskID != claim.Task.Record.ID ||
		authority.OperationID != claim.Task.Record.OperationID || authority.PlanHash != claim.Task.Record.PlanHash {
		return result, errs.New(errs.KindInternal, "candidate Release assignment authority is invalid")
	}
	planHash, err := hex.DecodeString(authority.PlanHash)
	if err != nil || !bytes.Equal(planHash, plan.GetPlanHash()) {
		return result, errs.New(errs.KindInternal, "candidate Release assignment plan authority diverges")
	}
	result.restoration = &agentpb.ReleaseRestorationAuthority{
		TaskId: authority.TaskID, OperationId: authority.OperationID, PlanHash: planHash,
		EnvironmentId: authority.EnvironmentID, CandidateArtifactId: authority.CandidateArtifactID,
		AuthoritySha256: authorityDigest, Candidates: make([]*agentpb.ReleaseRestorationCandidate, len(authority.Candidates)),
	}
	for index, candidate := range authority.Candidates {
		target := agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_UNSPECIFIED
		switch candidate.Target {
		case taskassignments.ReleaseRestorationServingPredecessor:
			target = agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR
		case taskassignments.ReleaseRestorationCandidateAbsence:
			target = agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE
		default:
			return candidateReleaseWireAuthority{}, errs.New(
				errs.KindInternal,
				"candidate Release restoration target is invalid",
			)
		}
		result.restoration.Candidates[index] = &agentpb.ReleaseRestorationCandidate{
			ServiceId: candidate.ServiceID, ReleaseId: candidate.ReleaseID, Target: target,
		}
	}
	if predecessor := authority.AppliedPredecessor; predecessor != nil {
		artifactDigest, decodeErr := hex.DecodeString(predecessor.ComposeArtifactSHA256)
		if decodeErr != nil || len(artifactDigest) != 32 {
			return candidateReleaseWireAuthority{}, errs.New(
				errs.KindInternal,
				"serving predecessor assignment authority is invalid",
			)
		}
		result.restoration.AppliedPredecessor = &agentpb.ReleaseAppliedPredecessorAuthority{
			KeyRevision: predecessor.KeyRevision, RevisionId: predecessor.RevisionID,
			RenderGeneration: predecessor.RenderGeneration, ComposeArtifactSha256: artifactDigest,
			ComposeArtifact: append([]byte(nil), predecessor.ComposeArtifact...),
		}
	}
	if len(authority.NativePredecessors) != 0 {
		if len(authority.NativePredecessors) != len(authority.Candidates) {
			return candidateReleaseWireAuthority{}, errs.New(
				errs.KindInternal,
				"native predecessor assignment authority is incomplete",
			)
		}
		result.restoration.NativePredecessors = make(
			[]*agentpb.ReleaseNativePredecessorAuthority,
			len(authority.NativePredecessors),
		)
		for index, predecessor := range authority.NativePredecessors {
			if predecessor.ServiceID != authority.Candidates[index].ServiceID {
				return candidateReleaseWireAuthority{}, errs.New(
					errs.KindInternal,
					"native predecessor assignment authority diverges",
				)
			}
			result.restoration.NativePredecessors[index] = &agentpb.ReleaseNativePredecessorAuthority{
				ServiceId: predecessor.ServiceID, CurrentArtifact: append([]byte(nil), predecessor.CurrentArtifact...),
				RetainedPriorArtifact: append([]byte(nil), predecessor.RetainedPriorArtifact...),
			}
		}
	}
	if result.mode == agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY {
		directive := claim.ReleaseRecovery
		if directive == nil || directive.RecordSHA256 != record.ReleaseRecoveryRecordSHA256 {
			return candidateReleaseWireAuthority{}, errs.New(errs.KindInternal, "durable recovery directive is missing")
		}
		phase := agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_UNSPECIFIED
		switch directive.Phase {
		case taskassignments.ReleaseRecoveryPhaseProbe:
			phase = agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROBE
		case taskassignments.ReleaseRecoveryPhaseCompensate:
			phase = agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_COMPENSATE
		case taskassignments.ReleaseRecoveryPhaseProven:
			phase = agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROVEN
		default:
			return candidateReleaseWireAuthority{}, errs.New(errs.KindInternal, "durable recovery phase is invalid")
		}
		result.recovery = &agentpb.ReleaseRecoveryDirective{
			StepIds: append([]string(nil), directive.StepIDs...), Cursor: directive.Cursor,
			ReleaseRecoveryRecordSha256: append([]byte(nil), result.recoveryDigest...), Phase: phase,
			ApplicableCompensationStepIds: append([]string(nil), directive.ApplicableCompensationStepIDs...),
		}
	}
	return result, nil
}
