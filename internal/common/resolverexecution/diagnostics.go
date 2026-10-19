package resolverexecution

import "github.com/AlanD20/groundplane/internal/common/workexecution"

func DiagnosticStage(
	attempt workexecution.Attempt,
	layout Layout,
	begin, progress Progress,
) (workexecution.Stage, error) {
	if begin.Validate(attempt, layout) != nil || !begin.InFlight ||
		progress.InFlight && !EqualProgress(begin, progress) ||
		!progress.InFlight && ValidateTransition(attempt, layout, &begin, progress) != nil {
		return workexecution.Stage{}, invalid("DNS resolver diagnostic has no matching recorded execution boundary")
	}
	stage := workexecution.Stage{Key: string(begin.Stage), StartedAt: begin.BeganAt, State: workexecution.StageRunning,
		Observed: "Authorized; waiting for the Agent's observation"}
	var completed string
	switch begin.Stage {
	case WriteConfiguration:
		stage.Name, stage.Expected = "Write CoreDNS configuration", "Validate and atomically write the captured Corefile using its exact managed-file predecessor"
		completed = "The captured Corefile was validated and written"
	case ReconcileRuntime:
		stage.Name, stage.Expected = "Apply CoreDNS container", "Apply the selected image and captured Compose runtime without changing unrelated containers"
		completed = "The captured CoreDNS runtime is present"
	case VerifyDNS:
		stage.Name, stage.Expected = "Verify DNS serving", "Verify the active Corefile and image, local answers, listeners and configured upstream forwarding"
		completed = "The captured DNS configuration passed its serving probes"
	case ApplyHostResolver:
		stage.Name, stage.Expected = "Switch host DNS to CoreDNS", "Apply local host resolution only after the captured DNS configuration is proven serving"
		completed = "The host resolver points to CoreDNS"
	case RestoreHostResolver:
		stage.Name, stage.Expected = "Restore host DNS baseline", "Restore the saved host resolver configuration before stopping CoreDNS"
		completed = "The host resolver baseline was restored"
	case RemoveRuntime:
		stage.Name, stage.Expected = "Stop CoreDNS container", "Remove only the captured CoreDNS runtime after host resolution has been restored"
		completed = "The captured CoreDNS runtime was removed"
	case VerifyAbsent:
		stage.Name, stage.Expected = "Verify CoreDNS stopped", "Verify no captured CoreDNS container remains"
		completed = "The captured CoreDNS runtime is absent"
	default:
		return workexecution.Stage{}, invalid("DNS resolver diagnostic names no executable stage")
	}
	if !progress.InFlight {
		stage.State, stage.FinishedAt, stage.Observed = workexecution.StageSucceeded, &progress.UpdatedAt, completed
	}
	return stage, stage.Validate()
}
