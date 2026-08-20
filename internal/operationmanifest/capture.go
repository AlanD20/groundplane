package operationmanifest

// Capture is raw evidence collected from each implementation surface. Record
// methods intentionally keep duplicate registrations so Validate can report
// multiplicity instead of hiding it.
type Capture struct {
	Console []ConsoleAction
	CLI     []CLILeaf
	API     []APIContract
}

func (capture *Capture) RecordConsole(actionID string) {
	capture.Console = append(capture.Console, ConsoleAction{ID: actionID})
}

func (capture *Capture) RecordCLI(path ...string) {
	cloned := append([]string(nil), path...)
	capture.CLI = append(capture.CLI, CLILeaf{Path: cloned})
}

func (capture *Capture) RecordAPI(operation APIContract) {
	operation.Errors = append([]ErrorContract(nil), operation.Errors...)
	capture.API = append(capture.API, operation)
}
