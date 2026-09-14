// Package serviceproxy defines the closed startup and activation procedure for
// the managed stable proxy. Configuration bytes travel on stdin, never in shell
// source. The runtime file belongs to the container, not an inherited Volume.
package serviceproxy

const (
	InitialConfigPath = "/etc/caddy/groundplane-proxy.json"
	RuntimeConfigPath = "/etc/caddy/groundplane-active.json"
	// RunningCommand is the exact /proc/1/cmdline after Startup's exec.
	RunningCommand = "caddy\x00run\x00--config\x00" + RuntimeConfigPath + "\x00"
	// Startup preserves the last selected configuration on container restart.
	// A newly created container initializes from its sealed Compose input.
	Startup = "umask 077; if [ ! -e " + RuntimeConfigPath + " ]; then " +
		"cp " + InitialConfigPath + " " + RuntimeConfigPath + ".next; sync; " +
		"mv -f " + RuntimeConfigPath + ".next " + RuntimeConfigPath + "; sync; fi; " +
		"exec caddy run --config " + RuntimeConfigPath
	// Activate publishes complete restart bytes before changing live routing.
	// Interrupted or failed reload is not success: the caller proves both views,
	// and the existing compensation procedure restores the sealed predecessor.
	// Bounded readiness reads precede writes so starting a stopped proxy cannot
	// race initialization or fail merely because Docker reports running first.
	Activate = "for gp_proxy_attempt in 1 2 3 4 5; do " +
		"if wget -q -T 1 -t 1 -O /dev/null http://127.0.0.1:2019/config/; then " +
		"umask 077; cat > " + RuntimeConfigPath + ".next; sync; " +
		"mv -f " + RuntimeConfigPath + ".next " + RuntimeConfigPath + "; sync; " +
		"exec caddy reload --config " + RuntimeConfigPath + "; fi; sleep 1; done; exit 1"
)

// StartupConfigPath identifies the file the observed process will load again.
// Captured predecessors may still start directly from their sealed Compose
// input. They are safe only when that actual file also matches the proof; a
// matching unused runtime file cannot qualify them.
func StartupConfigPath(command []byte) (string, bool) {
	switch string(command) {
	case RunningCommand:
		return RuntimeConfigPath, true
	case "caddy\x00run\x00--config\x00" + InitialConfigPath + "\x00":
		return InitialConfigPath, true
	default:
		return "", false
	}
}
