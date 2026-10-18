package attachexecution

import (
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// ProvisionDiagnosticStages describes acknowledged command boundaries only.
// Command text, credentials, hook inputs and returned facts are never included.
func ProvisionDiagnosticStages(attempt workexecution.Attempt, authority ProvisionAuthority,
	progress ProvisionProgress,
) (workexecution.Stage, workexecution.Stage, error) {
	if progress.Validate(attempt, authority) != nil || progress.Sequence == 0 {
		return workexecution.Stage{}, workexecution.Stage{}, invalid("Attach diagnostics require acknowledged command progress")
	}
	ordinal, started := progress.Completed, progress.BeganAt
	if !progress.InFlight {
		ordinal--
		started = &progress.LastResult.StartedAt
	}
	begin := workexecution.Stage{Key: "provision-" + strconv.FormatUint(uint64(ordinal), 10),
		State: workexecution.StageRunning, StartedAt: started,
		Observed: "Command authorized; waiting for the Agent to report its result"}
	completed := "Provisioning procedure completed with exit code 0"
	switch {
	case authority.IsConsumerCommand(ordinal):
		begin.Name = "Connect Service to backing network"
		begin.Expected = "Connect the captured running workloads without restarting or changing their configuration"
		completed = "All captured workloads are connected to the backing network"
	case authority.Custom:
		begin.Name = "Run Attach hook"
		begin.Expected = "Run the captured Custom Attach hook and validate and store its declared output facts"
		completed = "Attach hook completed with exit code 0; output facts were accepted"
	case ordinal == 0:
		begin.Name = "Provision backing-service credentials"
		begin.Expected = "Run the adapter's captured provisioning procedure for this consumer"
	default:
		begin.Name = "Grant access to shared database " + strconv.FormatUint(uint64(ordinal), 10)
		begin.Expected = "Apply this consumer's captured access grant without changing unrelated credentials"
		completed = "Captured access grant completed with exit code 0"
	}
	if err := begin.Validate(); err != nil {
		return workexecution.Stage{}, workexecution.Stage{}, err
	}
	stage := begin
	if !progress.InFlight {
		stage.State, stage.FinishedAt, stage.Observed = workexecution.StageSucceeded, &progress.LastResult.FinishedAt, completed
	}
	return begin, stage, stage.Validate()
}
