package handlers

import (
	"fmt"
	"strings"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func taskResourceKind(record etcd.TaskRecord) string {
	if kind := record.Params[taskjournal.TaskResourceKindParam]; kind != "" {
		return kind
	}
	prefix, _, _ := strings.Cut(record.Target, "_")
	return map[string]string{"tnt": "tenant", "prj": "project", "env": "environment", "svc": "service",
		"att": "attach", "vol": "volume", "net": "zone", "rte": "route", "scr": "script", "rg": "release_group",
		"run": "runner", "con": "connector", "sec": "secret", "ev": "entry", "cmp": "component", "agt": "agent", "rp": "recovery_point"}[prefix]
}

func taskFailureSummary(record etcd.TaskRecord) string {
	if record.Result == nil {
		return ""
	}
	var summary string
	switch record.Result.Diagnostic {
	case taskjournal.TaskResultDiagnosticConfigRejected:
		summary = "The workload rejected its configuration."
	case taskjournal.TaskResultDiagnosticComposeFailed:
		summary = "The container operation failed."
	case taskjournal.TaskResultDiagnosticTimeoutBeforeEffect:
		summary = "Execution timed out before a workload effect was recorded."
	}
	if record.Result.ExitCode != 0 {
		summary += fmt.Sprintf(" Executor exit code: %d.", record.Result.ExitCode)
	}
	return strings.TrimSpace(summary)
}

func taskResultSummary(record etcd.TaskRecord) string {
	result := record.Result
	if result == nil {
		return ""
	}
	if result.ReconciliationRequired {
		return "Recorded effects require reconciliation; completion is not proven."
	}
	if result.CandidateAbsenceEvidence != nil && result.CandidateAbsenceEvidence.AbsenceProven {
		return "The executor verified that candidate workload effects are absent."
	}
	if len(result.ProxyEvidence) != 0 || len(result.RecreateEvidence) != 0 {
		return fmt.Sprintf("Executor returned %d proxy and %d workload runtime observations.", len(result.ProxyEvidence), len(result.RecreateEvidence))
	}
	if len(result.Projects) != 0 {
		return fmt.Sprintf("Executor returned runtime observations for %d container project(s).", len(result.Projects))
	}
	return ""
}
