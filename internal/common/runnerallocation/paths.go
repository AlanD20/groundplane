package runnerallocation

import "path"

// RuntimeDirectory and DaemonSocketPath share the runtime address between
// planning, host setup and persisted ownership. Callers validate the Runner id.
func RuntimeDirectory(runnerID string) string {
	return path.Join("/run/groundplane-runners", runnerID)
}

func DaemonSocketPath(runnerID string) string {
	return path.Join(RuntimeDirectory(runnerID), "xdg", "docker.sock")
}
