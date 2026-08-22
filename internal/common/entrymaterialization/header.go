// Package entrymaterialization defines the closed transient message shared by
// the Controller, Agent, and task-scoped materialization helper.
package entrymaterialization

import (
	"path"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	// TemporaryPrefix is reserved for helper-owned unpublished files.
	TemporaryPrefix = ".groundplane-materialize-"
)

// OutputKind is the closed materialization output vocabulary.
type OutputKind uint8

const (
	outputInvalid OutputKind = iota
	OutputGeneratedEnv
	OutputPlainFile
	OutputSecretFile
)

// Mode is the complete permission value accepted by the helper. File-type and
// special bits are intentionally not representable in the output table.
type Mode uint32

const (
	ModePrivate  Mode = 0o600
	ModeReadOnly Mode = 0o444
)

// Digest is an exact SHA-256 content digest.
type Digest [32]byte

// HeaderSpec is mutable construction input. NewHeader validates and copies it
// into an immutable Header value.
type HeaderSpec struct {
	TaskID        string
	StepID        string
	EnvironmentID string
	Generation    uint64
	Destination   string
	ServiceID     string
	OutputKind    OutputKind
	UID           uint32
	GID           uint32
	Mode          Mode
	Length        uint64
	Digest        Digest
}

// Header is immutable transient procedure metadata. Its fields are private so
// a validated value cannot be changed between authentication and execution.
type Header struct {
	taskID        string
	stepID        string
	environmentID string
	generation    uint64
	destination   string
	serviceID     string
	outputKind    OutputKind
	uid           uint32
	gid           uint32
	mode          Mode
	length        uint64
	digest        Digest
}

// NewHeader validates one complete procedure header.
func NewHeader(spec HeaderSpec) (Header, error) {
	header := Header{
		taskID:        spec.TaskID,
		stepID:        spec.StepID,
		environmentID: spec.EnvironmentID,
		generation:    spec.Generation,
		destination:   spec.Destination,
		serviceID:     spec.ServiceID,
		outputKind:    spec.OutputKind,
		uid:           spec.UID,
		gid:           spec.GID,
		mode:          spec.Mode,
		length:        spec.Length,
		digest:        spec.Digest,
	}
	if err := header.validate(); err != nil {
		return Header{}, err
	}
	return header, nil
}

func (h Header) validate() error {
	if ids.Validate(ids.KindTask, h.taskID) != nil ||
		ids.Validate(ids.KindStep, h.stepID) != nil ||
		ids.Validate(ids.KindEnvironment, h.environmentID) != nil {
		return protocolError("invalid stable identity")
	}
	return ValidateMetadata(MetadataSpec{
		EnvironmentID: h.environmentID,
		Generation:    h.generation,
		Destination:   h.destination,
		ServiceID:     h.serviceID,
		OutputKind:    h.outputKind,
		UID:           h.uid,
		GID:           h.gid,
		Mode:          h.mode,
	})
}

func validateDestination(value string) error {
	if value == "" || !utf8.ValidString(value) ||
		strings.ContainsRune(value, '\x00') || strings.Contains(value, `\`) ||
		strings.HasPrefix(value, "/") {
		return protocolError("invalid relative destination")
	}
	cleaned := path.Clean(value)
	if cleaned == "." || cleaned != value || strings.HasPrefix(cleaned, "../") {
		return protocolError("invalid relative destination")
	}
	for _, component := range strings.Split(value, "/") {
		if strings.HasPrefix(component, TemporaryPrefix) {
			return protocolError("reserved destination")
		}
	}
	return nil
}

// GeneratedEnvDestination derives the only generated environment destination
// shapes from stable scope ids. An empty serviceID selects environment scope.
func GeneratedEnvDestination(environmentID, serviceID string) (string, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return "", protocolError("invalid generated environment scope")
	}
	destination := "secrets/.env." + environmentID
	if serviceID == "" {
		return destination, nil
	}
	if ids.Validate(ids.KindService, serviceID) != nil {
		return "", protocolError("invalid generated service scope")
	}
	return destination + "." + serviceID, nil
}

// DigestBytes calculates the transient payload digest without converting the
// bytes to a string. Ownership of value remains with the caller.
func DigestBytes(value []byte) Digest {
	hasher := NewHasher()
	defer hasher.Destroy()
	if written, err := hasher.Write(value); err != nil || written != len(value) {
		return Digest{}
	}
	digest, err := hasher.SumAndDestroy()
	if err != nil {
		return Digest{}
	}
	return digest
}

func (h Header) TaskID() string         { return h.taskID }
func (h Header) StepID() string         { return h.stepID }
func (h Header) EnvironmentID() string  { return h.environmentID }
func (h Header) Generation() uint64     { return h.generation }
func (h Header) Destination() string    { return h.destination }
func (h Header) ServiceID() string      { return h.serviceID }
func (h Header) OutputKind() OutputKind { return h.outputKind }
func (h Header) UID() uint32            { return h.uid }
func (h Header) GID() uint32            { return h.gid }
func (h Header) Mode() Mode             { return h.mode }
func (h Header) Length() uint64         { return h.length }
func (h Header) Digest() Digest         { return h.digest }
func (k OutputKind) String() string {
	switch k {
	case OutputGeneratedEnv:
		return "generated_env"
	case OutputPlainFile:
		return "plain_file"
	case OutputSecretFile:
		return "secret_file"
	default:
		return ""
	}
}

// ParseOutputKind converts a channel value into the closed output vocabulary.
func ParseOutputKind(value string) (OutputKind, error) {
	switch value {
	case "generated_env":
		return OutputGeneratedEnv, nil
	case "plain_file":
		return OutputPlainFile, nil
	case "secret_file":
		return OutputSecretFile, nil
	default:
		return outputInvalid, protocolError("unknown output kind")
	}
}

func protocolError(reason string) error {
	return errs.New(errs.KindInternal, "entry materialization protocol: "+reason)
}
