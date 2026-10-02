//go:build linux

package backupstage

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"golang.org/x/sys/unix"
)

const AgentRoot = agentprotocol.StatePath + "/task-stage"

// PrepareAgentRoot creates only the fixed leaf inside trusted Agent storage.
// Ancestors may be readable, but must be root-owned and not replaceable by
// another user; the final stage directory itself is private and durable.
func PrepareAgentRoot(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	ops := defaultLinuxOperations()
	current, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return systemError("open Agent storage root", err)
	}
	for _, component := range strings.Split(strings.TrimPrefix(agentprotocol.StatePath, "/"), "/") {
		next, err := openDirectoryAt(ctx, current, component, rootResolvePolicy, ops)
		if err != nil {
			return closeWithPrimary(ctx, systemError("pin Agent storage ancestor", err), current)
		}
		if err := unix.Close(current); err != nil {
			return closeWithPrimary(ctx, systemError("close Agent storage ancestor", err), next)
		}
		current = next
		var stat unix.Stat_t
		if err := unix.Fstat(current, &stat); err != nil {
			return closeWithPrimary(ctx, systemError("inspect Agent storage ancestor", err), current)
		}
		if stat.Uid != 0 || stat.Mode&0o022 != 0 {
			return closeWithPrimary(ctx, validationError("Agent storage ancestor is replaceable"), current)
		}
	}
	leaf, err := openOrCreateDirectory(ctx, current, "task-stage", ops)
	if err != nil {
		return closeWithPrimary(ctx, err, current)
	}
	return closeFDs(ctx, leaf, current)
}
