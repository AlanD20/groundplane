package etcd

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/dnsproof"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type taskResultData struct {
	ExecutionEpoch                  uint32                              `json:"execution_epoch,omitempty"`
	ReleaseRecoveryRecordSHA256     string                              `json:"release_recovery_record_sha256,omitempty"`
	Kind                            TaskResultKind                      `json:"kind"`
	ExitCode                        int32                               `json:"exit_code"`
	FailedStepID                    string                              `json:"failed_step_id,omitempty"`
	Diagnostic                      TaskResultDiagnostic                `json:"diagnostic"`
	ReconciliationRequired          bool                                `json:"reconciliation_required"`
	Projects                        []taskObservedProjectSummaryData    `json:"projects,omitempty"`
	ProxyEvidence                   []TaskProxyEvidence                 `json:"proxy_evidence,omitempty"`
	RecreateEvidence                []TaskRecreateEvidence              `json:"recreate_evidence,omitempty"`
	CandidateAbsenceEvidence        *TaskCandidateAbsenceEvidence       `json:"candidate_absence_evidence,omitempty"`
	DNSResolverCandidateObservation *TaskDNSResolverObservationEvidence `json:"dns_resolver_candidate_observation,omitempty"`
	DNSResolverRollbackObservation  *TaskDNSResolverObservationEvidence `json:"dns_resolver_rollback_observation,omitempty"`
}

type taskObservedProjectSummaryData struct {
	ProjectName    string `json:"project_name"`
	ObservedAt     string `json:"observed_at"`
	ContainerCount uint32 `json:"container_count"`
	NetworkCount   uint32 `json:"network_count"`
	VolumeCount    uint32 `json:"volume_count"`
	CollisionCount uint32 `json:"collision_count"`
}

func taskResultToData(result *TaskResultRecord) *taskResultData {
	if result == nil {
		return nil
	}
	data := &taskResultData{
		ExecutionEpoch: result.ExecutionEpoch, ReleaseRecoveryRecordSHA256: result.ReleaseRecoveryRecordSHA256,
		Kind: result.Kind, ExitCode: result.ExitCode, FailedStepID: result.FailedStepID,
		Diagnostic: result.Diagnostic, ReconciliationRequired: result.ReconciliationRequired,
		Projects:                 make([]taskObservedProjectSummaryData, len(result.Projects)),
		ProxyEvidence:            append([]TaskProxyEvidence(nil), result.ProxyEvidence...),
		RecreateEvidence:         append([]TaskRecreateEvidence(nil), result.RecreateEvidence...),
		CandidateAbsenceEvidence: cloneTaskCandidateAbsenceEvidence(result.CandidateAbsenceEvidence),
	}
	data.DNSResolverCandidateObservation = cloneTaskDNSResolverObservationEvidence(
		result.DNSResolverCandidateObservation,
	)
	data.DNSResolverRollbackObservation = cloneTaskDNSResolverObservationEvidence(result.DNSResolverRollbackObservation)
	for index, project := range result.Projects {
		data.Projects[index] = taskObservedProjectSummaryData{
			ProjectName: project.ProjectName, ObservedAt: project.ObservedAt.UTC().Format(time.RFC3339Nano),
			ContainerCount: project.ContainerCount, NetworkCount: project.NetworkCount,
			VolumeCount: project.VolumeCount, CollisionCount: project.CollisionCount,
		}
	}
	return data
}

func taskResultFromData(data *taskResultData) (*TaskResultRecord, error) {
	if data == nil {
		return nil, nil
	}
	result := &TaskResultRecord{
		ExecutionEpoch: data.ExecutionEpoch, ReleaseRecoveryRecordSHA256: data.ReleaseRecoveryRecordSHA256,
		Kind: data.Kind, ExitCode: data.ExitCode, FailedStepID: data.FailedStepID,
		Diagnostic: data.Diagnostic, ReconciliationRequired: data.ReconciliationRequired,
		Projects:                 make([]TaskObservedProjectSummary, len(data.Projects)),
		ProxyEvidence:            append([]TaskProxyEvidence(nil), data.ProxyEvidence...),
		RecreateEvidence:         append([]TaskRecreateEvidence(nil), data.RecreateEvidence...),
		CandidateAbsenceEvidence: cloneTaskCandidateAbsenceEvidence(data.CandidateAbsenceEvidence),
	}
	if data.DNSResolverCandidateObservation != nil {
		evidence := *data.DNSResolverCandidateObservation
		if _, err := dnsproof.Unmarshal(evidence.CanonicalEvidence); err != nil {
			return nil, errs.New(errs.KindInternal, "task DNS resolver observation proof is corrupt")
		}
		result.DNSResolverCandidateObservation = cloneTaskDNSResolverObservationEvidence(&evidence)
	}
	if data.DNSResolverRollbackObservation != nil {
		evidence := *data.DNSResolverRollbackObservation
		if _, err := dnsproof.Unmarshal(evidence.CanonicalEvidence); err != nil {
			return nil, errs.New(errs.KindInternal, "task DNS resolver rollback proof is corrupt")
		}
		result.DNSResolverRollbackObservation = cloneTaskDNSResolverObservationEvidence(&evidence)
	}
	for index, project := range data.Projects {
		observedAt, err := parseCanonicalTimestamp(project.ObservedAt)
		if err != nil {
			return nil, err
		}
		result.Projects[index] = TaskObservedProjectSummary{
			ProjectName: project.ProjectName, ObservedAt: observedAt,
			ContainerCount: project.ContainerCount, NetworkCount: project.NetworkCount,
			VolumeCount: project.VolumeCount, CollisionCount: project.CollisionCount,
		}
	}
	return result, nil
}
