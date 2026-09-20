package agent

import (
	"crypto/sha256"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type PlanHash [sha256.Size]byte

type TaskTerminal uint8

const (
	TaskTerminalCompleted TaskTerminal = iota + 1
	TaskTerminalFailed
	TaskTerminalTimedOut
	TaskTerminalAborted
)

type TaskResult struct {
	AssignmentID                string
	TaskID                      string
	PlanHash                    PlanHash
	Terminal                    TaskTerminal
	ExitCode                    int32
	ExecutionEpoch              uint32
	ReleaseRecoveryRecordSHA256 []byte
	Compose                     *agentpb.ComposeTaskResult
	EnvironmentDirectory        *agentpb.EnvironmentDirectoryTaskResult
}

type TaskProgressState uint8

const (
	TaskProgressRunning TaskProgressState = iota + 1
	TaskProgressCompleted
	TaskProgressFailed
	TaskProgressTimedOut
	TaskProgressAborted
)

type TaskProgress struct {
	AssignmentID   string
	TaskID         string
	PlanHash       PlanHash
	StepID         string
	ExecutionEpoch uint32
	Ordinal        uint64
	State          TaskProgressState
	Chunk          []byte
}
