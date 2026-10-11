package handlers

import (
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"strings"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func taskFailure(record etcd.TaskRecord) *apiTypes.TaskFailure {
	if record.Status != taskjournal.TaskStatusFailed && record.Status != taskjournal.TaskStatusTimedOut &&
		record.Status != taskjournal.TaskStatusAborted {
		return nil
	}
	failure := &apiTypes.TaskFailure{Status: string(record.Status), Summary: taskFailureSummary(record)}
	if failure.Summary == "" {
		failure.Summary = "No specific failure reason was recorded."
	}
	if record.Result != nil {
		failure.StepID = record.Result.FailedStepID
		if record.Result.Diagnostic != taskjournal.TaskResultDiagnosticNone {
			failure.Code = string(record.Result.Diagnostic)
		}
		if record.Result.ExitCode != 0 {
			exitCode := record.Result.ExitCode
			failure.ExitCode = &exitCode
		}
		if observed := record.Result.Failure; observed != nil {
			failure.Code, failure.Summary = observed.Code, observed.Summary()
		}
	}
	if record.BlueprintFailure != nil {
		failure.Code = string(record.BlueprintFailure.Code)
	}
	return failure
}

func taskStepFailure(state string, stepID string, observed *taskprogress.Failure) *apiTypes.TaskFailure {
	if observed == nil {
		return nil
	}
	failure := &apiTypes.TaskFailure{Status: state, StepID: stepID, Code: observed.Code, Summary: observed.Summary()}
	if observed.ExitCode != 0 {
		exitCode := observed.ExitCode
		failure.ExitCode = &exitCode
	}
	return failure
}

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
	if failure := record.BlueprintFailure; failure != nil {
		if failure.Phase == "child_execution" {
			return fmt.Sprintf(
				"Blueprint Apply failed because child Task %s failed. Inspect that Task for the execution and recovery details; a new Apply is required.",
				failure.ChildTaskID,
			)
		}
		stage := map[string]string{
			"planning": "planning changes", "selection": "selecting the next change",
			"settlement": "saving a resource change", "child": "preparing an execution Task",
		}[failure.Phase]
		return fmt.Sprintf(
			"Blueprint Apply failed while %s (error code: %s). GP stopped scheduling new changes; a new Apply is required.",
			stage,
			failure.Code,
		)
	}
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
	case taskjournal.TaskResultDiagnosticDNSWithdrawalFailed:
		summary = fmt.Sprintf(
			"Service removal did not start because CoreDNS Task %s failed to withdraw its DNS answer. The Service and its proxy address are retained. Inspect the CoreDNS Task, fix its configuration and request removal again.",
			record.Result.DNSWithdrawalTaskID,
		)
	case taskjournal.TaskResultDiagnosticDNSWithdrawalTimedOut:
		summary = "Service removal reached its deadline before it could pass the DNS withdrawal gate and start. The Service and its proxy address are retained. Inspect CoreDNS Tasks before requesting removal again."
	case taskjournal.TaskResultDiagnosticReleaseNetworkFailed:
		summary = "Deploy did not start because its internal DNS bridge could not be prepared. Existing workloads are unchanged. Inspect the Agent logs for this release, fix the network problem and submit Deploy again."
	case taskjournal.TaskResultDiagnosticDNSActivationFailed:
		summary = "Deploy did not start because CoreDNS could not activate its internal Zone listeners. Existing workloads are unchanged. Inspect the CoreDNS Task, fix the resolver and submit Deploy again."
	case taskjournal.TaskResultDiagnosticDNSActivationTimedOut:
		summary = "Deploy reached its deadline before its internal DNS bridge and CoreDNS listeners were ready. Existing workloads are unchanged. Inspect CoreDNS and the Zone bridge before submitting Deploy again."
	case taskjournal.TaskResultDiagnosticHealthcheckPgrepMissing:
		summary = "Healthcheck cannot run: pgrep is not installed in the container. Use an image with pgrep or choose an available healthcheck."
	case taskjournal.TaskResultDiagnosticHealthcheckCurlMissing:
		summary = "Healthcheck cannot run: curl is not installed in the container. Use an image with curl or choose an available healthcheck."
	case taskjournal.TaskResultDiagnosticHealthcheckNCMissing:
		summary = "Healthcheck cannot run: nc is not installed in the container. Use an image with nc or choose an available healthcheck."
	case taskjournal.TaskResultDiagnosticHealthcheckCommandMissing:
		summary = "Healthcheck cannot run: a required command was not found in the container. Check the image and healthcheck command."
	case taskjournal.TaskResultDiagnosticHealthcheckCommandNotExecutable:
		summary = "Healthcheck cannot run: a required command is not executable in the container. Check executable permissions and the healthcheck command."
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
		return fmt.Sprintf(
			"Executor returned %d proxy and %d workload runtime observations.",
			len(result.ProxyEvidence),
			len(result.RecreateEvidence),
		)
	}
	if len(result.Projects) != 0 {
		return fmt.Sprintf("Executor returned runtime observations for %d container project(s).", len(result.Projects))
	}
	return ""
}
