// Package common holds the CLI's error handler and output writer — the
// ONLY place in internal/cli that formats anything for a human. See
// docs/standards.md, section 1's import matrix ("internal/cli/ …
// common/ = error handler + output") and section 3.
package common

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// HandleErrors is the CLI's single shared error handler (the mvmctl
// HandleErrors pattern): every command's RunE result funnels through
// here exactly once, in internal/cli/root.go's Execute wrapper.
//   - nil                -> exit 0
//   - broken pipe        -> exit 0 (e.g. `| head`; not a real failure)
//   - context.Canceled   -> propagate silently (Ctrl-C already printed nothing)
//   - *errs.Error        -> print "code: message", exit 1
//   - anything else      -> print a generic message, exit 1
func HandleErrors(err error) int {
	if err == nil {
		return 0
	}
	if isBrokenPipe(err) {
		return 0
	}
	if errors.Is(err, context.Canceled) {
		return 130 // conventional SIGINT exit code
	}

	var de *errs.Error
	if errors.As(err, &de) {
		fmt.Fprintf(os.Stderr, "error: %s: %s\n", de.Code, de.Message)
		return 1
	}

	fmt.Fprintf(os.Stderr, "error: %s: unexpected error\n", errs.CodeInternal)
	return 1
}

func isBrokenPipe(err error) bool {
	return errors.Is(err, io.ErrClosedPipe) || errors.Is(err, syscall.EPIPE)
}
