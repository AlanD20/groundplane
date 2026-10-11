package serviceproxy

// Activation keeps implementation-specific commands and stdin together. Callers
// select only a sealed implementation; no operator text is executable source.
type Activation struct {
	User    string
	Command []string
	Input   []byte
}

func PrepareActivation(implementation Implementation, manifest []byte) (Activation, error) {
	source, err := PrepareRuntimeSource(implementation, manifest)
	if err != nil {
		return Activation{}, err
	}
	if implementation == Caddy {
		return Activation{Command: []string{"sh", "-ec", Activate}, Input: source.Content}, nil
	}
	// The upstream entrypoint starts Envoy as this account. Activation must use
	// that same owner so the mode-0600 restart files remain readable by Envoy.
	return Activation{User: "envoy", Command: []string{"sh", "-ec", EnvoyActivate}, Input: source.Content}, nil
}
