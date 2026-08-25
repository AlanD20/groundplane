package postgres16protocol

import (
	"slices"
	"testing"
)

// Rationale: zero is a canonical declared restore size, while the signed
// maximum is inclusive and the next uint64 value must be rejected.
func TestRestoreSourceSizeBoundaries(t *testing.T) {
	tests := []struct {
		name    string
		size    uint64
		wantErr bool
	}{
		{"zero", 0, false},
		{"maximum", MaximumRestoreSourceBytes, false},
		{"above maximum", MaximumRestoreSourceBytes + 1, true},
	}
	for _, test := range tests {
		request := Request{
			Operation: OperationRestoreList, Nonce: Nonce{1}, DeadlineUnixNano: 1,
			SourceSize: test.size, SourceSHA256: Digest{1},
		}
		arguments, err := request.Arguments()
		if test.wantErr {
			if err == nil {
				t.Fatalf("%s restore source accepted", test.name)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s restore source rejected: %v", test.name, err)
		}
		parsed, err := ParseArguments(arguments)
		if err != nil {
			t.Fatalf("%s canonical arguments rejected: %v", test.name, err)
		}
		if parsed.SourceSize != test.size {
			t.Fatalf("%s source size = %d, want %d", test.name, parsed.SourceSize, test.size)
		}
	}
}

// Rationale: every public operation must round-trip through the sole shared
// Docker Exec grammar so a future caller cannot invent an argument order.
func TestRequestDockerExecRoundTrip(t *testing.T) {
	nonce := testNonce(t)
	digest := testDigest(t)
	tests := []Request{
		{Operation: OperationProbePGDump, Nonce: nonce, DeadlineUnixNano: 1},
		{Operation: OperationProbePGRestore, Nonce: nonce, DeadlineUnixNano: 1},
		{Operation: OperationProbePSQL, Nonce: nonce, DeadlineUnixNano: 1},
		{Operation: OperationServerMajor, Nonce: nonce, DeadlineUnixNano: 1, Database: "app_012345"},
		{
			Operation: OperationDump, Nonce: nonce, DeadlineUnixNano: 1,
			Database: "app_012345", Role: "role_012345",
		},
		{Operation: OperationRestoreList, Nonce: nonce, DeadlineUnixNano: 1, SourceSize: 7, SourceSHA256: digest},
		{
			Operation: OperationTerminateDBConnections, Nonce: nonce, DeadlineUnixNano: 1,
			Database: "app_012345",
		},
		{
			Operation: OperationAssertZeroDBConnections, Nonce: nonce, DeadlineUnixNano: 1,
			Database: "app_012345",
		},
		{
			Operation: OperationRestoreApply, Nonce: nonce, DeadlineUnixNano: 1,
			Database: "app_012345", Role: "role_012345", SourceSize: 7, SourceSHA256: digest,
		},
		{
			Operation: OperationPostRestoreVerify, Nonce: nonce, DeadlineUnixNano: 1,
			Database: "app_012345",
		},
		{Operation: OperationStop, Nonce: nonce, DeadlineUnixNano: 1},
	}
	for _, request := range tests {
		command, err := request.DockerExecCommand()
		if err != nil {
			t.Fatalf("operation %d: DockerExecCommand() error = %v", request.Operation, err)
		}
		parsed, err := ParseDockerExecCommand(command)
		if err != nil {
			t.Fatalf("operation %d: ParseDockerExecCommand() error = %v", request.Operation, err)
		}
		if parsed != request {
			t.Fatalf("operation %d: parsed request = %#v, want %#v", request.Operation, parsed, request)
		}
	}
}

// Rationale: the four-entry environment and env-erasing prefix are ordered
// release identity, not a set that may be permuted or extended.
func TestExactEnvironmentAndPrefixRejectPermutation(t *testing.T) {
	environment := Environment()
	if !ValidEnvironment(environment) {
		t.Fatal("canonical environment rejected")
	}
	environment[0], environment[1] = environment[1], environment[0]
	if ValidEnvironment(environment) {
		t.Fatal("permuted environment accepted")
	}
	prefix := DockerExecPrefix()
	want := []string{
		"/usr/bin/env",
		"-i",
		"PATH=" + PathValue,
		"HOME=/nonexistent",
		"LC_ALL=C",
		"TZ=UTC",
		HelperPath,
	}
	if !slices.Equal(prefix, want) {
		t.Fatalf("DockerExecPrefix() = %q, want %q", prefix, want)
	}
	prefix[2] = "PATH=/tmp"
	if slices.Equal(DockerExecPrefix(), prefix) {
		t.Fatal("caller mutation changed shared prefix authority")
	}
}

// Rationale: exported Request values are directly constructible, so stop must
// reject every inapplicable field instead of relying on a preferred constructor.
func TestStopRejectsInapplicableFields(t *testing.T) {
	nonce := testNonce(t)
	digest := testDigest(t)
	tests := []Request{
		{Operation: OperationStop, Nonce: nonce, DeadlineUnixNano: 1, Database: "app_012345"},
		{Operation: OperationStop, Nonce: nonce, DeadlineUnixNano: 1, Role: "role_012345"},
		{Operation: OperationStop, Nonce: nonce, DeadlineUnixNano: 1, SourceSize: 1},
		{Operation: OperationStop, Nonce: nonce, DeadlineUnixNano: 1, SourceSHA256: digest},
	}
	for index, request := range tests {
		if _, err := request.Arguments(); err == nil {
			t.Fatalf("case %d: malformed stop request accepted", index)
		}
	}
}

// Rationale: canonical decimal and lowercase hexadecimal encodings prevent
// multiple public byte strings from naming one execution identity.
func TestParseArgumentsRejectsNoncanonicalIdentity(t *testing.T) {
	nonce := testNonce(t).String()
	tests := [][]string{
		{"stop", "1", nonce, "01"},
		{"stop", "1", nonce, "0"},
		{"stop", "1", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "1"},
		{"stop", "1", nonce, "1", "extra"},
		{"run", "1", nonce, "1", "probe-pg-dump", "016"},
	}
	for index, arguments := range tests {
		if _, err := ParseArguments(arguments); err == nil {
			t.Fatalf("case %d: noncanonical arguments accepted: %q", index, arguments)
		}
	}
}

// Rationale: helper exit meanings cross an executable boundary and must remain
// stable numeric goldens rather than declaration-order values.
func TestExitCodeNumericGoldensAndClassification(t *testing.T) {
	tests := []struct {
		code ExitCode
		want uint32
	}{
		{ExitSuccess, 0},
		{ExitRequestInvalid, 1},
		{ExitEnvironmentInvalid, 2},
		{ExitRecoveryRequired, 3},
		{ExitLaunchFailed, 4},
		{ExitDeadlineExceeded, 5},
		{ExitChildFailed, 6},
		{ExitCallerIdentity, 7},
		{ExitIOFailed, 8},
		{ExitInputIntegrity, 9},
		{ExitStreamLimitExceeded, 10},
		{ExitProofMismatch, 11},
		{ExitInternalFailure, 12},
	}
	for _, test := range tests {
		if uint32(test.code) != test.want || !test.code.Valid() {
			t.Fatalf("code %d = %d, valid=%v; want %d, true", test.code, test.code, test.code.Valid(), test.want)
		}
		classified, err := ClassifyProcessExit(true, test.want)
		if err != nil || classified != test.code {
			t.Fatalf("status %d: ClassifyProcessExit() = %d, %v; want %d", test.want, classified, err, test.code)
		}
	}
	for _, status := range []uint32{13, 125, 126, 127, 128, 255} {
		if _, err := ClassifyProcessExit(true, status); err == nil {
			t.Fatalf("reserved normal status %d accepted", status)
		}
	}
	classified, err := ClassifyProcessExit(false, 0)
	if err != nil || classified != ExitRecoveryRequired {
		t.Fatalf("non-normal exit classified as %d, %v; want recovery_required", classified, err)
	}
}

// Rationale: operation-to-client, fd, and stream mappings are a closed part of
// the managed image contract and must not fall through to generic execution.
func TestOperationPoliciesAreClosed(t *testing.T) {
	proofProfile := FDProfile{
		Version: 1, Stdin: FDReadOnlyDevNull, Stdout: FDValidatedResult, Stderr: FDBoundedDiagnostic,
	}
	dumpProfile := FDProfile{
		Version: 1, Stdin: FDReadOnlyDevNull, Stdout: FDArtifactOutput, Stderr: FDBoundedDiagnostic,
	}
	restoreProfile := FDProfile{
		Version: 1, Stdin: FDValidatedRestoreInput, Stdout: FDDiscardCount, Stderr: FDBoundedDiagnostic,
	}
	proofPolicy := StreamPolicy{
		Input: InputNone, Output: OutputProof,
		OutputLimit: DiagnosticLimitBytes, StderrLimit: DiagnosticLimitBytes,
	}
	probePolicy := StreamPolicy{
		Input: InputNone, Output: OutputProof,
		OutputLimit: ProbeStdoutLimitBytes, StderrLimit: ProbeStderrLimitBytes,
	}
	tests := []struct {
		operation Operation
		path      string
		profile   FDProfile
		policy    StreamPolicy
	}{
		{OperationProbePGDump, PGDumpPath, proofProfile, probePolicy},
		{OperationProbePGRestore, PGRestorePath, proofProfile, probePolicy},
		{OperationProbePSQL, PSQLPath, proofProfile, probePolicy},
		{OperationServerMajor, PSQLPath, proofProfile, proofPolicy},
		{
			OperationDump,
			PGDumpPath,
			dumpProfile,
			StreamPolicy{Input: InputNone, Output: OutputArtifact, StderrLimit: DiagnosticLimitBytes},
		},
		{
			OperationRestoreList,
			PGRestorePath,
			restoreProfile,
			StreamPolicy{
				Input: InputRestoreSource, Output: OutputDiscardCount, StderrLimit: DiagnosticLimitBytes,
			},
		},
		{OperationTerminateDBConnections, PSQLPath, proofProfile, proofPolicy},
		{OperationAssertZeroDBConnections, PSQLPath, proofProfile, proofPolicy},
		{
			OperationRestoreApply,
			PGRestorePath,
			restoreProfile,
			StreamPolicy{
				Input: InputRestoreSource, Output: OutputDiscardCount,
				OutputLimit: DiagnosticLimitBytes, StderrLimit: DiagnosticLimitBytes,
			},
		},
		{OperationPostRestoreVerify, PSQLPath, proofProfile, proofPolicy},
	}
	for _, test := range tests {
		operation := test.operation
		path, err := ClientPath(operation)
		if err != nil || path != test.path {
			t.Fatalf("operation %d: ClientPath() = %q, %v; want %q", operation, path, err, test.path)
		}
		profile, err := ProfileFor(operation)
		if err != nil {
			t.Fatalf("operation %d: ProfileFor() error = %v", operation, err)
		}
		if profile != test.profile {
			t.Fatalf("operation %d: ProfileFor() = %#v, want %#v", operation, profile, test.profile)
		}
		policy, err := PolicyFor(operation)
		if err != nil || policy != test.policy {
			t.Fatalf("operation %d: PolicyFor() = %#v, %v; want %#v", operation, policy, err, test.policy)
		}
	}
	for _, operation := range []Operation{0, OperationStop, 12, 255} {
		if _, err := ClientPath(operation); err == nil {
			t.Fatalf("operation %d: client path accepted", operation)
		}
		if _, err := ProfileFor(operation); err == nil {
			t.Fatalf("operation %d: fd profile accepted", operation)
		}
		if _, err := PolicyFor(operation); err == nil {
			t.Fatalf("operation %d: stream policy accepted", operation)
		}
	}
}

func testNonce(t *testing.T) Nonce {
	t.Helper()
	nonce, err := ParseNonce("0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20")
	if err != nil {
		t.Fatalf("ParseNonce() error = %v", err)
	}
	return nonce
}

func testDigest(t *testing.T) Digest {
	t.Helper()
	digest, err := ParseDigest("202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f")
	if err != nil {
		t.Fatalf("ParseDigest() error = %v", err)
	}
	return digest
}
