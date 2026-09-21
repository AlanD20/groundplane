package etcd

import (
	"encoding/hex"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// blueprintAcknowledgedRuntime contributes only a successful member's sealed
// input to the same transaction as its terminal Task and serving projection.
func blueprintAcknowledgedRuntime(
	marker releases.ReleasePublicationMarker, task TaskRecord, assignment taskassignments.TaskAssignmentRecord,
	member releases.ReleaseStagedMemberRef, result taskjournal.TaskResultRecord, terminalAt time.Time,
) ([]byte, error) {
	if result.Kind != taskjournal.TaskResultCompose || result.ExitCode != 0 ||
		result.Diagnostic != taskjournal.TaskResultDiagnosticNone ||
		result.ReconciliationRequired ||
		result.ExecutionEpoch == 0 ||
		result.ExecutionEpoch != assignment.ExecutionEpoch ||
		marker.CandidateReleaseDescriptor.PlanID != task.PlanID ||
		hex.EncodeToString(marker.CandidateReleaseDescriptor.PlanHash) != task.PlanHash {
		return nil, errs.New(errs.KindStateConflict, "Blueprint runtime requires exact successful execution authority")
	}
	procedure, err := executionplan.OpenCandidateReleaseDescriptor(marker.CandidateReleaseDescriptor)
	if err != nil {
		return nil, err
	}
	stepID := ""
	for _, candidate := range procedure.Members {
		if candidate.ServiceId == member.ServiceID && candidate.CandidateReleaseId == member.ReleaseID {
			if len(candidate.ForwardStepIds) == 0 || stepID != "" {
				return nil, errs.New(errs.KindStateConflict, "Blueprint runtime member is ambiguous")
			}
			stepID = candidate.ForwardStepIds[len(candidate.ForwardStepIds)-1]
		}
	}
	digest, err := domain.Digest(struct {
		ReleaseID string                       `json:"release_id"`
		Result    taskjournal.TaskResultRecord `json:"result"`
	}{member.ReleaseID, result})
	if err != nil {
		return nil, err
	}
	var input *executionplan.BlueprintRuntimeInput
	for index := range marker.BlueprintRuntimes {
		candidate := &marker.BlueprintRuntimes[index]
		if candidate.ServiceID != member.ServiceID || candidate.ReleaseID != member.ReleaseID {
			continue
		}
		if input != nil {
			return nil, errs.New(errs.KindStateConflict, "Blueprint prepared runtime is duplicated")
		}
		input = candidate
	}
	if input == nil || stepID == "" {
		return nil, errs.New(errs.KindStateConflict, "Blueprint prepared runtime is missing")
	}
	runtime, err := executionplan.OpenBlueprintRuntime(marker.ExecutedComposeArtifact, *input)
	if err != nil {
		return nil, err
	}
	record := serviceruntimerecord.Record{EnvironmentID: task.Owner.EnvironmentID, Runtime: runtime,
		Source: serviceruntimerecord.Acknowledgement{
			TaskID: task.ID, PlanID: task.PlanID, PlanHash: task.PlanHash, StepID: stepID,
			AgentID: assignment.AgentID, AssignmentID: assignment.AssignmentID, ExecutionEpoch: assignment.ExecutionEpoch,
			RenderGeneration: uint64(task.RenderGeneration), EffectDigest: digest, AcknowledgedAt: terminalAt,
		}}
	record.Configuration, err = taskRuntimeConfiguration(task)
	if err != nil {
		return nil, err
	}
	if err := serviceruntimerecord.Validate(record); err != nil {
		return nil, err
	}
	return releases.EncodeReleaseRecord("service-acknowledged-runtime", record)
}
