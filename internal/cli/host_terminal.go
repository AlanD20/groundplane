package cli

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/coder/websocket"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

type cliTerminalControl struct {
	Type    string `json:"type"`
	Data    string `json:"data,omitempty"`
	Columns int    `json:"columns,omitempty"`
	Rows    int    `json:"rows,omitempty"`
}

type cliTerminalStatus struct {
	Type    string                        `json:"type"`
	Session *apiTypes.HostTerminalSession `json:"session,omitempty"`
}

type cliTerminalResult struct {
	reason string
	err    error
}

func newHostTerminalCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "terminal",
		Short: "Open an interactive root shell on the Controller host",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			stdin, ok := cmd.InOrStdin().(*os.File)
			if !ok || !term.IsTerminal(int(stdin.Fd())) {
				return errs.New(errs.KindValidationFailed, "host terminal requires an interactive TTY")
			}
			columns, rows, err := term.GetSize(int(stdin.Fd()))
			if err != nil {
				return errs.Wrap(errs.KindInternal, fmt.Errorf("read terminal size: %w", err))
			}
			state, err := term.MakeRaw(int(stdin.Fd()))
			if err != nil {
				return errs.Wrap(errs.KindInternal, fmt.Errorf("enter raw terminal mode: %w", err))
			}
			defer term.Restore(int(stdin.Fd()), state)

			app := fromContext(cmd)
			session, err := app.Client.OpenHostTerminal(cmd.Context(), columns, rows)
			if err != nil {
				return err
			}
			conn, err := app.Client.DialHostTerminal(cmd.Context(), session, 0)
			if err != nil {
				return err
			}
			defer conn.CloseNow()
			return runInteractiveHostTerminal(cmd.Context(), stdin, cmd.OutOrStdout(), conn)
		},
	}
}

func runInteractiveHostTerminal(
	ctx context.Context,
	stdin *os.File,
	stdout io.Writer,
	conn *websocket.Conn,
) error {
	result := make(chan cliTerminalResult, 1)
	go readCLITerminal(ctx, stdout, conn, result)
	input := make(chan []byte, 8)
	inputErr := make(chan error, 1)
	go readCLIInput(stdin, input, inputErr)

	resize := make(chan os.Signal, 1)
	signal.Notify(resize, syscall.SIGWINCH)
	defer signal.Stop(resize)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case terminalResult := <-result:
			if terminalResult.reason != "" {
				_, _ = fmt.Fprintf(stdout, "\r\n[host terminal closed: %s]\r\n", terminalResult.reason)
			}
			return terminalResult.err
		case err := <-inputErr:
			return err
		case data := <-input:
			if err := writeCLIControl(ctx, conn, cliTerminalControl{Type: "input", Data: string(data)}); err != nil {
				return err
			}
		case <-resize:
			columns, rows, err := term.GetSize(int(stdin.Fd()))
			if err != nil {
				continue
			}
			if err := writeCLIControl(ctx, conn, cliTerminalControl{Type: "resize", Columns: columns, Rows: rows}); err != nil {
				return err
			}
		}
	}
}

func readCLIInput(stdin io.Reader, values chan<- []byte, result chan<- error) {
	buffer := make([]byte, 4<<10)
	for {
		count, err := stdin.Read(buffer)
		if count > 0 {
			values <- append([]byte(nil), buffer[:count]...)
		}
		if err != nil {
			result <- err
			return
		}
	}
}

func readCLITerminal(
	ctx context.Context,
	stdout io.Writer,
	conn *websocket.Conn,
	result chan<- cliTerminalResult,
) {
	for {
		kind, body, err := conn.Read(ctx)
		if err != nil {
			result <- cliTerminalResult{err: err}
			return
		}
		switch kind {
		case websocket.MessageBinary:
			if len(body) < 8 {
				result <- cliTerminalResult{err: errors.New("host terminal returned an invalid output frame")}
				return
			}
			_ = binary.BigEndian.Uint64(body[:8])
			if _, err := stdout.Write(body[8:]); err != nil {
				result <- cliTerminalResult{err: err}
				return
			}
		case websocket.MessageText:
			var status cliTerminalStatus
			if err := json.Unmarshal(body, &status); err != nil {
				result <- cliTerminalResult{err: err}
				return
			}
			switch status.Type {
			case "activity":
			case "output_truncated":
				_, _ = io.WriteString(stdout, "\r\n[earlier host terminal output was truncated]\r\n")
			case "terminated":
				reason := "terminated"
				if status.Session != nil && status.Session.Reason != "" {
					reason = status.Session.Reason
				}
				result <- cliTerminalResult{reason: reason}
				return
			default:
				result <- cliTerminalResult{err: errors.New("host terminal returned an invalid status frame")}
				return
			}
		default:
			result <- cliTerminalResult{err: errors.New("host terminal returned an invalid frame type")}
			return
		}
	}
}

func writeCLIControl(ctx context.Context, conn *websocket.Conn, control cliTerminalControl) error {
	body, err := json.Marshal(control)
	if err != nil {
		return err
	}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return conn.Write(writeCtx, websocket.MessageText, body)
}
