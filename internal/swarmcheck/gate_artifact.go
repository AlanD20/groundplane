package swarmcheck

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	gateArtifactVersion  = 1
	maxGateArtifactBytes = 256 << 10
	maxGateOutputBytes   = 128 << 10
	maxGateJSONString    = (maxGateOutputBytes + 2) / 3 * 4
)

// GateState is one machine-observed repository state surrounding a gate run.
type GateState struct {
	Head  CommitID `json:"head"`
	Tree  TreeID   `json:"tree"`
	Clean bool     `json:"clean"`
}

// GateArtifact is the bounded machine-produced evidence for one closed gate.
type GateArtifact struct {
	Version     int       `json:"version"`
	Wave        WaveID    `json:"wave"`
	Gate        GateID    `json:"gate"`
	Executable  string    `json:"executable"`
	Args        []string  `json:"args"`
	Environment []string  `json:"environment"`
	Candidate   CommitID  `json:"candidate"`
	Tree        TreeID    `json:"tree"`
	Before      GateState `json:"before"`
	After       GateState `json:"after"`
	ExitCode    int       `json:"exit_code"`
	Stdout      []byte    `json:"stdout"`
	Stderr      []byte    `json:"stderr"`
}

// GateArgs returns a copy of the exact arguments for one closed MVP gate.
func GateArgs(gate GateID) ([]string, bool) {
	var args []string
	switch gate {
	case GateRepositoryCI:
		args = []string{"ci"}
	case GateOperatorVerifier:
		args = []string{}
	default:
		return nil, false
	}
	return append([]string(nil), args...), true
}

// RequiredGateIDs returns the complete closed gate set.
func RequiredGateIDs() []GateID {
	return []GateID{GateRepositoryCI, GateOperatorVerifier}
}

// NewGateArtifact builds a versioned artifact from output already bounded by
// the subprocess Runner before capture.
func NewGateArtifact(
	wave WaveID,
	gate GateID,
	executable string,
	args []string,
	environment []string,
	candidate CommitID,
	tree TreeID,
	before GateState,
	after GateState,
	exitCode int,
	stdout []byte,
	stderr []byte,
) GateArtifact {
	return GateArtifact{
		Version:     gateArtifactVersion,
		Wave:        wave,
		Gate:        gate,
		Executable:  executable,
		Args:        append([]string(nil), args...),
		Environment: append([]string(nil), environment...),
		Candidate:   candidate,
		Tree:        tree,
		Before:      before,
		After:       after,
		ExitCode:    exitCode,
		Stdout:      append([]byte(nil), stdout...),
		Stderr:      append([]byte(nil), stderr...),
	}
}

// ParseGateArtifact strictly decodes one bounded gate artifact.
func ParseGateArtifact(ctx context.Context, data []byte) (GateArtifact, error) {
	var artifact GateArtifact
	if err := contextErr(ctx); err != nil {
		return artifact, err
	}
	if len(data) > maxGateArtifactBytes || !utf8.Valid(data) {
		return artifact, invalid("gate artifact is oversized or not valid UTF-8")
	}
	if err := validateJSONDocument(data, maxGateJSONString); err != nil {
		return artifact, errs.Wrap(
			errs.KindValidationFailed,
			fmt.Errorf("decode gate artifact: %w", err),
		)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&artifact); err != nil {
		return artifact, errs.Wrap(
			errs.KindValidationFailed,
			fmt.Errorf("decode gate artifact: %w", err),
		)
	}
	if err := ensureEOF(decoder); err != nil {
		return artifact, err
	}
	if err := artifact.Validate(); err != nil {
		return artifact, err
	}
	return artifact, nil
}

// Validate checks structural bounds; repository binding is checked by
// internal/swarmgit against live Git evidence and the manifest receipt.
func (artifact GateArtifact) Validate() error {
	_, exists := GateArgs(artifact.Gate)
	if artifact.Version != gateArtifactVersion || !validRefSegment(artifact.Wave) || !exists ||
		!validAbsoluteExecutable(artifact.Executable) || len(artifact.Args) > maxCollection ||
		len(artifact.Environment) == 0 || len(artifact.Environment) > maxCollection ||
		!validObjectID(artifact.Candidate) || !validObjectID(artifact.Tree) ||
		!validObjectID(artifact.Before.Head) || !validObjectID(artifact.Before.Tree) ||
		!validObjectID(artifact.After.Head) || !validObjectID(artifact.After.Tree) ||
		artifact.ExitCode < -1 || artifact.ExitCode > 255 ||
		len(artifact.Stdout)+len(artifact.Stderr) > maxGateOutputBytes {
		return invalid("gate artifact structure is invalid")
	}
	for _, arg := range artifact.Args {
		if len(arg) > maxStringBytes || !isASCII(arg) || !utf8.ValidString(arg) ||
			bytes.IndexByte([]byte(arg), 0) >= 0 {
			return invalid("gate artifact argument is invalid")
		}
	}
	seenEnvironment := make(map[string]struct{}, len(artifact.Environment))
	for _, value := range artifact.Environment {
		key, _, found := strings.Cut(value, "=")
		if !found || key == "" || len(value) > maxStringBytes || !isASCII(value) ||
			!utf8.ValidString(value) || bytes.IndexByte([]byte(value), 0) >= 0 {
			return invalid("gate artifact environment is invalid")
		}
		if _, exists := seenEnvironment[key]; exists {
			return invalid("gate artifact environment keys must be unique")
		}
		seenEnvironment[key] = struct{}{}
	}
	if _, exists := seenEnvironment["PATH"]; !exists {
		return invalid("gate artifact environment must bind PATH")
	}
	return nil
}

// EncodeGateArtifact returns the exact bytes and digest written by the
// descriptor-relative artifact writer in internal/swarmgit.
func EncodeGateArtifact(ctx context.Context, artifact GateArtifact) ([]byte, Digest, error) {
	if err := artifact.Validate(); err != nil {
		return nil, "", err
	}
	if err := contextErr(ctx); err != nil {
		return nil, "", err
	}
	data, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		return nil, "", invalid(fmt.Sprintf("encode gate artifact: %v", err))
	}
	data = append(data, '\n')
	if len(data) > maxGateArtifactBytes {
		return nil, "", invalid("encoded gate artifact exceeds bounded size")
	}
	digest := sha256.Sum256(data)
	return data, Digest(hex.EncodeToString(digest[:])), nil
}

func validAbsoluteExecutable(value string) bool {
	return value != "" && len(value) <= maxPathBytes && filepath.IsAbs(value) &&
		filepath.Clean(value) == value && isASCII(value) && utf8.ValidString(value)
}

func validGateID(gate GateID) bool {
	_, exists := GateArgs(gate)
	return exists
}
