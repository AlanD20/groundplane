package controllerupgrade

// These paths and private flags are the closed bootstrap/recovery process ABI.
// Operator requests carry digest identities, never replacements for these values.
const (
	ReleaseDirectory     = "/var/lib/groundplane/controller-updates"
	BinaryDirectory      = "/usr/local/libexec/groundplane"
	PreviousExecutable   = ReleaseDirectory + "/previous"
	ControllerExecutable = BinaryDirectory + "/controller"
	RecoveryExecutable   = BinaryDirectory + "/controller-recovery"
	GuardFlag            = "--upgrade-guard"
	RecoveryFlag         = "--upgrade-run"
	ControllerUnit       = "groundplane-controller.service"
)
