package taskjournal

import "time"

type TaskObservedProjectSummary struct {
	ProjectName    string    `json:"project_name"`
	ObservedAt     time.Time `json:"observed_at"`
	ContainerCount uint32    `json:"container_count"`
	NetworkCount   uint32    `json:"network_count"`
	VolumeCount    uint32    `json:"volume_count"`
	CollisionCount uint32    `json:"collision_count"`
}

type TaskProxyEvidence struct {
	ServiceID       string `json:"service_id"`
	Target          string `json:"target"`
	ProxyGeneration uint64 `json:"proxy_generation"`
	ConfigSHA256    string `json:"config_sha256"`
	ReleaseID       string `json:"release_id"`
	Compensated     bool   `json:"compensated"`
}

type TaskRecreateEvidence struct {
	ServiceID   string `json:"service_id"`
	ReleaseID   string `json:"release_id"`
	ArtifactID  string `json:"artifact_id"`
	Compensated bool   `json:"compensated"`
	Target      string `json:"target"`
}

type TaskCandidateAbsenceCandidate struct {
	ServiceID string `json:"service_id"`
	ReleaseID string `json:"release_id"`
}

type TaskCandidateAbsenceEvidence struct {
	AssignmentID        string                          `json:"assignment_id"`
	PlanHash            string                          `json:"plan_hash"`
	AuthoritySHA256     string                          `json:"authority_sha256"`
	ComposeProjectName  string                          `json:"compose_project_name"`
	CandidateArtifactID string                          `json:"candidate_artifact_id"`
	Candidates          []TaskCandidateAbsenceCandidate `json:"candidates"`
	AbsenceProven       bool                            `json:"absence_proven"`
}

type TaskResultRecord struct {
	Kind                            TaskResultKind                      `json:"kind"`
	ExitCode                        int32                               `json:"exit_code"`
	FailedStepID                    string                              `json:"failed_step_id,omitempty"`
	Diagnostic                      TaskResultDiagnostic                `json:"diagnostic"`
	ReconciliationRequired          bool                                `json:"reconciliation_required"`
	Projects                        []TaskObservedProjectSummary        `json:"projects,omitempty"`
	ProxyEvidence                   []TaskProxyEvidence                 `json:"proxy_evidence,omitempty"`
	RecreateEvidence                []TaskRecreateEvidence              `json:"recreate_evidence,omitempty"`
	CandidateAbsenceEvidence        *TaskCandidateAbsenceEvidence       `json:"candidate_absence_evidence,omitempty"`
	DNSResolverCandidateObservation *TaskDNSResolverObservationEvidence `json:"dns_resolver_candidate_observation,omitempty"`
	DNSResolverRollbackObservation  *TaskDNSResolverObservationEvidence `json:"dns_resolver_rollback_observation,omitempty"`
	ExecutionEpoch                  uint32                              `json:"-"`
	ReleaseRecoveryRecordSHA256     string                              `json:"-"`
}

type TaskTerminalAssignmentRecord struct {
	AssignmentID    string `json:"assignment_id"`
	AgentID         string `json:"agent_id"`
	AgentGeneration uint64 `json:"agent_generation"`
}
