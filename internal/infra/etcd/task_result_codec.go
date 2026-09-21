package etcd

import (
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	"github.com/AlanD20/groundplane/internal/common/dnsproof"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type taskResultData struct {
	ExecutionEpoch                  uint32                                          `json:"execution_epoch,omitempty"`
	ReleaseRecoveryRecordSHA256     string                                          `json:"release_recovery_record_sha256,omitempty"`
	Kind                            taskjournal.TaskResultKind                      `json:"kind"`
	ExitCode                        int32                                           `json:"exit_code"`
	FailedStepID                    string                                          `json:"failed_step_id,omitempty"`
	Diagnostic                      taskjournal.TaskResultDiagnostic                `json:"diagnostic"`
	ReconciliationRequired          bool                                            `json:"reconciliation_required"`
	Projects                        []taskObservedProjectSummaryData                `json:"projects,omitempty"`
	ProxyEvidence                   []taskjournal.TaskProxyEvidence                 `json:"proxy_evidence,omitempty"`
	RecreateEvidence                []taskjournal.TaskRecreateEvidence              `json:"recreate_evidence,omitempty"`
	CandidateAbsenceEvidence        *taskjournal.TaskCandidateAbsenceEvidence       `json:"candidate_absence_evidence,omitempty"`
	DNSResolverCandidateObservation *taskjournal.TaskDNSResolverObservationEvidence `json:"dns_resolver_candidate_observation,omitempty"`
	DNSResolverRollbackObservation  *taskjournal.TaskDNSResolverObservationEvidence `json:"dns_resolver_rollback_observation,omitempty"`
}

type taskObservedProjectSummaryData struct {
	ProjectName    string `json:"project_name"`
	ObservedAt     string `json:"observed_at"`
	ContainerCount uint32 `json:"container_count"`
	NetworkCount   uint32 `json:"network_count"`
	VolumeCount    uint32 `json:"volume_count"`
	CollisionCount uint32 `json:"collision_count"`
}

func taskResultToData(result *taskjournal.TaskResultRecord) *taskResultData {
	if result == nil {
		return nil
	}
	data := &taskResultData{
		ExecutionEpoch: result.ExecutionEpoch, ReleaseRecoveryRecordSHA256: result.ReleaseRecoveryRecordSHA256,
		Kind: result.Kind, ExitCode: result.ExitCode, FailedStepID: result.FailedStepID,
		Diagnostic: result.Diagnostic, ReconciliationRequired: result.ReconciliationRequired,
		Projects:                 make([]taskObservedProjectSummaryData, len(result.Projects)),
		ProxyEvidence:            append([]taskjournal.TaskProxyEvidence(nil), result.ProxyEvidence...),
		RecreateEvidence:         append([]taskjournal.TaskRecreateEvidence(nil), result.RecreateEvidence...),
		CandidateAbsenceEvidence: taskjournal.CloneTaskCandidateAbsenceEvidence(result.CandidateAbsenceEvidence),
	}
	data.DNSResolverCandidateObservation = taskjournal.CloneTaskDNSResolverObservationEvidence(
		result.DNSResolverCandidateObservation,
	)
	data.DNSResolverRollbackObservation = taskjournal.CloneTaskDNSResolverObservationEvidence(result.DNSResolverRollbackObservation)
	for index, project := range result.Projects {
		data.Projects[index] = taskObservedProjectSummaryData{
			ProjectName: project.ProjectName, ObservedAt: project.ObservedAt.UTC().Format(time.RFC3339Nano),
			ContainerCount: project.ContainerCount, NetworkCount: project.NetworkCount,
			VolumeCount: project.VolumeCount, CollisionCount: project.CollisionCount,
		}
	}
	return data
}

func taskResultFromData(data *taskResultData) (*taskjournal.TaskResultRecord, error) {
	if data == nil {
		return nil, nil
	}
	result := &taskjournal.TaskResultRecord{
		ExecutionEpoch: data.ExecutionEpoch, ReleaseRecoveryRecordSHA256: data.ReleaseRecoveryRecordSHA256,
		Kind: data.Kind, ExitCode: data.ExitCode, FailedStepID: data.FailedStepID,
		Diagnostic: data.Diagnostic, ReconciliationRequired: data.ReconciliationRequired,
		Projects:                 make([]taskjournal.TaskObservedProjectSummary, len(data.Projects)),
		ProxyEvidence:            append([]taskjournal.TaskProxyEvidence(nil), data.ProxyEvidence...),
		RecreateEvidence:         append([]taskjournal.TaskRecreateEvidence(nil), data.RecreateEvidence...),
		CandidateAbsenceEvidence: taskjournal.CloneTaskCandidateAbsenceEvidence(data.CandidateAbsenceEvidence),
	}
	if data.DNSResolverCandidateObservation != nil {
		evidence := *data.DNSResolverCandidateObservation
		if _, err := dnsproof.Unmarshal(evidence.CanonicalEvidence); err != nil {
			return nil, errs.New(errs.KindInternal, "task DNS resolver observation proof is corrupt")
		}
		result.DNSResolverCandidateObservation = taskjournal.CloneTaskDNSResolverObservationEvidence(&evidence)
	}
	if data.DNSResolverRollbackObservation != nil {
		evidence := *data.DNSResolverRollbackObservation
		if _, err := dnsproof.Unmarshal(evidence.CanonicalEvidence); err != nil {
			return nil, errs.New(errs.KindInternal, "task DNS resolver rollback proof is corrupt")
		}
		result.DNSResolverRollbackObservation = taskjournal.CloneTaskDNSResolverObservationEvidence(&evidence)
	}
	for index, project := range data.Projects {
		observedAt, err := recordcodec.ParseCanonicalTimestamp(project.ObservedAt)
		if err != nil {
			return nil, err
		}
		result.Projects[index] = taskjournal.TaskObservedProjectSummary{
			ProjectName: project.ProjectName, ObservedAt: observedAt,
			ContainerCount: project.ContainerCount, NetworkCount: project.NetworkCount,
			VolumeCount: project.VolumeCount, CollisionCount: project.CollisionCount,
		}
	}
	return result, nil
}
