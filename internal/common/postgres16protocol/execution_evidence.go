package postgres16protocol

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"io"
)

const MaximumExecutionEvidenceBytes = 16 * 1024

// ExecutionEvidence exposes only one selected, fully reaped execution. It is
// not a process inventory, command interface, or permission to execute again.
type ExecutionEvidence struct {
	Nonce         Nonce                 `json:"nonce"`
	RequestSHA256 Digest                `json:"request_sha256"`
	State         ConfinementStateShape `json:"state"`
}

func ExecutionRequestSHA256(request Request) (Digest, error) {
	if !request.Operation.ValidRun() {
		return Digest{}, invalid("postgres execution evidence requires a client operation")
	}
	arguments, err := request.Arguments()
	if err != nil {
		return Digest{}, err
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("groundplane.postgres16.execution-request.v1\x00"))
	for _, argument := range arguments {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(argument)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(argument))
	}
	var digest Digest
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

func (evidence ExecutionEvidence) Validate() error {
	state := evidence.State
	if evidence.Nonce == (Nonce{}) || evidence.RequestSHA256 == (Digest{}) || state.Validate() != nil ||
		state.Phase != ConfinementPhaseReaped || !state.IO || state.IOEvidence == nil ||
		state.Terminal.Kind != ConfinementTerminalExited || state.Terminal.ExitCode != 0 ||
		!state.Terminal.Wait4Reaped || state.Terminal.RawWaitStatus != 0 ||
		!state.IOEvidence.Stdin.EOF || !state.IOEvidence.Stdout.EOF || !state.IOEvidence.Stderr.EOF {
		return invalid("postgres completed execution evidence is incomplete")
	}
	return nil
}

func MarshalExecutionEvidence(evidence ExecutionEvidence) ([]byte, error) {
	if err := evidence.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(evidence)
	if err != nil || len(encoded) > MaximumExecutionEvidenceBytes {
		return nil, invalid("postgres execution evidence encoding is invalid")
	}
	return encoded, nil
}

func ParseExecutionEvidence(encoded []byte) (ExecutionEvidence, error) {
	if len(encoded) == 0 || len(encoded) > MaximumExecutionEvidenceBytes {
		return ExecutionEvidence{}, invalid("postgres execution evidence size is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var evidence ExecutionEvidence
	if err := decoder.Decode(&evidence); err != nil || evidence.Validate() != nil {
		return ExecutionEvidence{}, invalid("postgres execution evidence is invalid")
	}
	var trailing json.RawMessage
	if decoder.Decode(&trailing) != io.EOF {
		return ExecutionEvidence{}, invalid("postgres execution evidence has trailing data")
	}
	canonical, err := MarshalExecutionEvidence(evidence)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return ExecutionEvidence{}, invalid("postgres execution evidence is not canonical")
	}
	return evidence, nil
}

func parseEvidenceArguments(arguments []string) (Request, error) {
	if len(arguments) != 5 || arguments[1] != protocolVersionArgument {
		return Request{}, invalid("postgres helper evidence arguments are invalid")
	}
	request := Request{Operation: OperationEvidence}
	if arguments[0] == "retire" {
		request.Operation = OperationRetire
	}
	var err error
	request.Nonce, err = ParseNonce(arguments[2])
	if err == nil {
		request.DeadlineUnixNano, err = parsePositiveUint(arguments[3])
	}
	if err == nil {
		request.RequestSHA256, err = ParseDigest(arguments[4])
	}
	if err != nil {
		return Request{}, err
	}
	return request, request.Validate()
}
