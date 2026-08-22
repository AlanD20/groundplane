package entrymaterialization

const (
	MaximumContentBytes     = uint64(1 << 20)
	MaximumDestinationBytes = uint32(240)
	MaximumChunkBytes       = 32 * 1024
)

// MetadataSpec is the non-secret materialization procedure shared by a sealed
// plan, transient transfer header, and helper frame.
type MetadataSpec struct {
	EnvironmentID string
	Generation    uint64
	Destination   string
	ServiceID     string
	ServiceName   string
	OutputKind    OutputKind
	UID           uint32
	GID           uint32
	Mode          Mode
}

// ValidateMetadata enforces the accepted ADR 0020 ownership and mode table
// without requiring task/step identities or plaintext.
func ValidateMetadata(spec MetadataSpec) error {
	if spec.Generation == 0 {
		return protocolError("invalid render generation")
	}
	if err := validateDestination(spec.Destination); err != nil {
		return err
	}
	if spec.UID == ^uint32(0) || spec.GID == ^uint32(0) {
		return protocolError("invalid numeric identity")
	}
	switch spec.OutputKind {
	case OutputGeneratedEnv:
		destination, err := GeneratedEnvDestination(spec.EnvironmentID, spec.ServiceName)
		if err != nil || (spec.ServiceID == "") != (spec.ServiceName == "") ||
			spec.UID != 0 || spec.GID != 0 || spec.Mode != ModePrivate ||
			spec.Destination != destination {
			return protocolError("invalid generated environment output")
		}
	case OutputPlainFile:
		if spec.ServiceID != "" || spec.ServiceName != "" || spec.Mode != ModeReadOnly {
			return protocolError("invalid plain file output")
		}
	case OutputSecretFile:
		if spec.ServiceID != "" || spec.ServiceName != "" || spec.Mode != ModePrivate {
			return protocolError("invalid secret file output")
		}
	default:
		return protocolError("unknown output kind")
	}
	return nil
}
