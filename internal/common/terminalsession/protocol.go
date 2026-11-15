// Package terminalsession defines bounded, transient interactive process I/O.
// It carries no persistent Task, Secret, or recovery authority.
package terminalsession

import (
	"io"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	MaximumFrameBytes = 16 << 10
	MaximumSessions   = 4
	HelperPath        = "/usr/local/libexec/groundplane/container-terminal-helper"
)

type Size struct {
	Columns uint16 `json:"columns"`
	Rows    uint16 `json:"rows"`
}

func (s Size) Validate() error {
	if s.Columns < 20 || s.Columns > 500 || s.Rows < 5 || s.Rows > 300 {
		return errs.New(errs.KindValidationFailed, "terminal size is out of range")
	}
	return nil
}

// Process owns one exact process and its streams, never a replaceable target.
type Process interface {
	io.Reader
	io.Writer
	Resize(Size) error
	Terminate() error
	Wait() error
	Result() Exit
}

type Exit struct {
	Reason string
	Code   int
}

type ContainerOptions struct {
	SessionID              string   `json:"session_id"`
	User                   string   `json:"user"`
	Command                []string `json:"command"`
	WorkingDirectory       string   `json:"working_directory"`
	Size                   Size     `json:"size"`
	InputInactivitySeconds int      `json:"input_inactivity_seconds"`
}

func (o ContainerOptions) Validate() error {
	if ids.Validate(ids.KindTerminal, o.SessionID) != nil || o.Size.Validate() != nil ||
		len(o.User) == 0 || len(o.User) > 128 || strings.ContainsAny(o.User, "\x00\r\n") ||
		len(o.Command) == 0 || len(o.Command) > 64 || o.Command[0] == "" ||
		len(o.WorkingDirectory) > 4096 || strings.ContainsRune(o.WorkingDirectory, 0) ||
		(o.WorkingDirectory != "" && !strings.HasPrefix(o.WorkingDirectory, "/")) ||
		o.InputInactivitySeconds < 60 || o.InputInactivitySeconds > 8*60*60 {
		return errs.New(errs.KindValidationFailed, "container terminal options are invalid")
	}
	length := 0
	for _, arg := range o.Command {
		length += len(arg)
		if strings.ContainsRune(arg, 0) {
			return errs.New(errs.KindValidationFailed, "container command contains a zero byte")
		}
	}
	if length > MaximumFrameBytes {
		return errs.New(errs.KindValidationFailed, "container command is too large")
	}
	return nil
}

// Input and Event are the helper's private pipe protocol. Output is framed so
// command text cannot forge control replies. Docker exec uses pipes, not a TTY;
// the helper owns the sole actual PTY and its requested child credentials.
type Input struct {
	Data []byte `json:"data,omitempty"`
	Size *Size  `json:"size,omitempty"`
	Stop bool   `json:"stop,omitempty"`
}

type Event struct {
	Ready    bool   `json:"ready,omitempty"`
	Data     []byte `json:"data,omitempty"`
	Closed   bool   `json:"closed,omitempty"`
	Reason   string `json:"reason,omitempty"`
	ExitCode int    `json:"exit_code,omitempty"`
}
