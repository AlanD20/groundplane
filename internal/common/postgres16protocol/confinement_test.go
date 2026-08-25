package postgres16protocol

import (
	"testing"
)

// Rationale: the helper is the root supervisor while only its gated client is
// uid 70, so accepting either identity under the other's profile breaks state isolation.
func TestConfinementIdentityProfilesRejectSubstitution(t *testing.T) {
	intent := testConfinementIntent(t)
	if err := intent.Validate(testConfinementRelease(t)); err != nil {
		t.Fatalf("valid intent rejected: %v", err)
	}
	wrongSupervisor := intent
	wrongSupervisor.Supervisor.RealUID = PostgreSQLUID
	if err := wrongSupervisor.Validate(testConfinementRelease(t)); err == nil {
		t.Fatal("uid-70 supervisor accepted")
	}
	wrongChild := intent
	wrongChild.ChildSecurity.EffectiveGID = 0
	if err := wrongChild.Validate(testConfinementRelease(t)); err == nil {
		t.Fatal("root child profile accepted")
	}
	wrongGate := intent
	wrongGate.GateFile.Mode = HelperMode
	if err := wrongGate.Validate(testConfinementRelease(t)); err == nil {
		t.Fatal("non-root-only client gate accepted")
	}
	wrongEnvelope := intent
	wrongEnvelope.Supervisor.CapabilityEff = 0
	if err := wrongEnvelope.Validate(testConfinementRelease(t)); err == nil {
		t.Fatal("root supervisor with incomplete capability envelope accepted")
	}
}

// Rationale: the intent carries raw vectors for canonical encoding, so the
// validator must still enforce exact environment order and operation fd policy.
func TestConfinementLaunchIntentRejectsOpenVectors(t *testing.T) {
	intent := testConfinementIntent(t)
	permuted := intent
	permuted.Environment = cloneByteVector(intent.Environment)
	permuted.Environment[0], permuted.Environment[1] = permuted.Environment[1], permuted.Environment[0]
	if err := permuted.Validate(testConfinementRelease(t)); err == nil {
		t.Fatal("permuted environment accepted")
	}
	wrongFD := intent
	wrongFD.FDProfile.Stdout = FDArtifactOutput
	if err := wrongFD.Validate(testConfinementRelease(t)); err == nil {
		t.Fatal("wrong operation fd profile accepted")
	}
	oversized := intent
	oversized.Arguments = [][]byte{make([]byte, confinementMaximumArgumentBytes+1)}
	if err := oversized.Validate(testConfinementRelease(t)); err == nil {
		t.Fatal("oversized argv item accepted")
	}
	generic := intent
	generic.Arguments = [][]byte{[]byte("sh"), []byte("-c"), []byte("true")}
	if err := generic.Validate(testConfinementRelease(t)); err == nil {
		t.Fatal("generic executable argv accepted")
	}
}

// Rationale: READY and PROFILE_APPLIED are ordered evidence, while a FATAL
// sequence is derived solely from its fixed algorithm stage.
func TestConfinementGateStatusSequenceValidation(t *testing.T) {
	intent := testConfinementIntent(t)
	intentSHA := testConfinementDigest(t, 0x31)
	readyIdentity := testReadyGate(intent)
	ready := ConfinementGateStatus{
		Schema: 1, Sequence: 1, Kind: ConfinementGateStatusReady,
		Nonce: intent.Nonce, IntentSHA256: intentSHA, Ready: &readyIdentity,
	}
	if err := ready.Validate(intent, testConfinementRelease(t), intentSHA, intent.Supervisor); err != nil {
		t.Fatalf("valid READY rejected: %v", err)
	}
	ready.Sequence = 2
	if err := ready.Validate(intent, testConfinementRelease(t), intentSHA, intent.Supervisor); err == nil {
		t.Fatal("out-of-sequence READY accepted")
	}
	ready.Sequence = 1
	ready.Ready.SeccompMode = 2
	if err := ready.Validate(intent, testConfinementRelease(t), intentSHA, intent.Supervisor); err == nil {
		t.Fatal("READY after seccomp application accepted")
	}
	ready.Ready.SeccompMode = 0
	changedParent := intent.Supervisor
	changedParent.StartTicks++
	if err := ready.Validate(intent, testConfinementRelease(t), intentSHA, changedParent); err == nil {
		t.Fatal("READY with changed parent lifetime accepted")
	}
	for stage := ConfinementFatalSetProcessGroup; stage <= ConfinementFatalExec; stage++ {
		status := ConfinementGateStatus{
			Schema: 1, Sequence: stage.Sequence(), Kind: ConfinementGateStatusFatal,
			Nonce: intent.Nonce, IntentSHA256: intentSHA, FatalStage: stage, FatalErrno: 1,
		}
		if err := status.Validate(intent, testConfinementRelease(t), intentSHA, intent.Supervisor); err != nil {
			t.Fatalf("stage %d: valid FATAL rejected: %v", stage, err)
		}
		status.Sequence++
		if err := status.Validate(intent, testConfinementRelease(t), intentSHA, intent.Supervisor); err == nil {
			t.Fatalf("stage %d: wrong FATAL sequence accepted", stage)
		}
	}
	zeroErrno := ConfinementGateStatus{
		Schema: 1, Sequence: 1, Kind: ConfinementGateStatusFatal,
		Nonce: intent.Nonce, IntentSHA256: intentSHA, FatalStage: ConfinementFatalSetProcessGroup,
	}
	if err := zeroErrno.Validate(intent, testConfinementRelease(t), intentSHA, intent.Supervisor); err == nil {
		t.Fatal("zero-errno FATAL accepted")
	}
}

// Rationale: every operation has one exact client argv; accepting even one
// alternate switch, SQL program, or executable name would recreate a generic gate.
func TestConfinementClientArgumentsAreExact(t *testing.T) {
	tests := []struct {
		operation Operation
		arguments []string
	}{
		{OperationProbePGDump, []string{"pg_dump", "--version"}},
		{OperationProbePGRestore, []string{"pg_restore", "--version"}},
		{OperationProbePSQL, []string{"psql", "--version"}},
		{
			OperationServerMajor,
			testPSQLArguments("SELECT pg_catalog.current_setting('server_version_num')::integer / 10000;"),
		},
		{
			OperationDump,
			[]string{
				"pg_dump", "--format=custom", "--compress=0", "--no-owner", "--no-acl",
				"--host=/var/run/postgresql", "--username=postgres", "--no-password",
				"--role=role_012345", "--dbname=app_012345",
			},
		},
		{OperationRestoreList, []string{"pg_restore", "--list", "--no-password"}},
		{
			OperationTerminateDBConnections,
			testPSQLArguments(
				"SELECT pg_catalog.coalesce(pg_catalog.bool_and(" +
					"pg_catalog.pg_terminate_backend(a.pid)), true) " +
					"FROM pg_catalog.pg_stat_activity AS a " +
					"WHERE a.datname = pg_catalog.current_database() " +
					"AND a.pid <> pg_catalog.pg_backend_pid();",
			),
		},
		{
			OperationAssertZeroDBConnections,
			testPSQLArguments(
				"SELECT pg_catalog.count(*) FROM pg_catalog.pg_stat_activity AS a " +
					"WHERE a.datname = pg_catalog.current_database() " +
					"AND a.pid <> pg_catalog.pg_backend_pid();",
			),
		},
		{
			OperationRestoreApply,
			[]string{
				"pg_restore", "--clean", "--if-exists", "--no-owner", "--no-acl", "--exit-on-error",
				"--single-transaction", "--host=/var/run/postgresql", "--username=postgres", "--no-password",
				"--role=role_012345", "--dbname=app_012345",
			},
		},
		{
			OperationPostRestoreVerify,
			testPSQLArguments("SELECT pg_catalog.current_database();"),
		},
	}
	for _, test := range tests {
		intent := testConfinementIntent(t)
		intent.Operation = test.operation
		clientPath, err := ClientPath(test.operation)
		if err != nil {
			t.Fatalf("operation %d: ClientPath() error = %v", test.operation, err)
		}
		intent.ClientFile.Path = clientPath
		intent.FDProfile, err = ProfileFor(test.operation)
		if err != nil {
			t.Fatalf("operation %d: ProfileFor() error = %v", test.operation, err)
		}
		intent.Arguments = byteArguments(test.arguments)
		if err := intent.Validate(testConfinementRelease(t)); err != nil {
			t.Fatalf("operation %d: exact argv rejected: %v", test.operation, err)
		}
		intent.Arguments[0] = append(intent.Arguments[0], 'x')
		if err := intent.Validate(testConfinementRelease(t)); err == nil {
			t.Fatalf("operation %d: altered argv accepted", test.operation)
		}
	}
}

// Rationale: Wait4 is the sole reap authority, so its raw status must reproduce
// the already retained exit, signal, and core facts exactly.
func TestConfinementRawWaitStatusReproduction(t *testing.T) {
	tests := []ConfinementTerminal{
		{
			Kind: ConfinementTerminalExited, ExitCode: 7, RawWaitStatus: 7 << 8,
			Wait4Reaped: true, EvidenceSHA256: Digest{1},
		},
		{
			Kind: ConfinementTerminalSignaled, Signal: 15, RawWaitStatus: 15,
			Wait4Reaped: true, EvidenceSHA256: Digest{1},
		},
		{
			Kind: ConfinementTerminalSignaled, Signal: 11, CoreDumped: true,
			RawWaitStatus: 11 | 0x80, Wait4Reaped: true, EvidenceSHA256: Digest{1},
		},
	}
	for index, terminal := range tests {
		if err := terminal.Validate(ConfinementPhaseReaped); err != nil {
			t.Fatalf("case %d: matching wait status rejected: %v", index, err)
		}
		terminal.RawWaitStatus++
		if err := terminal.Validate(ConfinementPhaseReaped); err == nil {
			t.Fatalf("case %d: mismatched wait status accepted", index)
		}
	}
}

// Rationale: a FATAL gate lifetime never proves a PostgreSQL child or its I/O,
// even when the state high-water phase was otherwise fabricated consistently.
func TestConfinementGateFatalForbidsChildAndIO(t *testing.T) {
	terminal := ConfinementTerminal{
		Kind: ConfinementTerminalGateFatal, ExitCode: 1, RawWaitStatus: 1 << 8, Wait4Reaped: true,
	}
	fatal := ConfinementFatalEvidence{
		Stage: ConfinementFatalSetProcessGroup, Errno: 1, Sequence: 1,
		FD4EOF: true, FrameSHA256: Digest{1},
	}
	state := ConfinementStateShape{
		EncodedSizeBytes: 1, Sequence: 1, Phase: ConfinementPhaseReaped,
		ProgressPhase: ConfinementPhaseChildDurable, LaunchIntentSHA256: Digest{2},
		GateReady: true, ReadyFrameSHA256: Digest{3}, GateRelease: true,
		ProfileFrameSHA256: Digest{4},
		GateProfile:        true, GateFatal: &fatal, Child: true, ExecFD4EOF: true, ExecEvidence: 1,
		Terminal: &terminal,
	}
	setTestTerminalEvidence(t, &state)
	if err := state.Validate(); err == nil {
		t.Fatal("gate FATAL with child evidence accepted")
	}
}

// Rationale: durable phase names alone are insufficient; each phase has one
// exact evidence-presence row that blocks skipped gate or exec proof.
func TestConfinementStatePresenceRows(t *testing.T) {
	ioEvidence := ConfinementIOEvidence{}
	tests := []ConfinementStateShape{
		{
			EncodedSizeBytes: 1, Sequence: 1, Phase: ConfinementPhaseCreated,
			ProgressPhase: ConfinementPhaseCreated, LaunchIntentSHA256: Digest{1},
		},
		{
			EncodedSizeBytes: 1, Sequence: 1, Phase: ConfinementPhaseGateDurable,
			ProgressPhase: ConfinementPhaseGateDurable, LaunchIntentSHA256: Digest{1},
			GateReady: true, ReadyFrameSHA256: Digest{2},
		},
		{
			EncodedSizeBytes: 1, Sequence: 1, Phase: ConfinementPhaseGateReleased,
			ProgressPhase: ConfinementPhaseGateReleased, LaunchIntentSHA256: Digest{1},
			GateReady: true, ReadyFrameSHA256: Digest{2}, GateRelease: true,
		},
		{
			EncodedSizeBytes: 1, Sequence: 1, Phase: ConfinementPhaseProfileApplied,
			ProgressPhase: ConfinementPhaseProfileApplied, LaunchIntentSHA256: Digest{1},
			GateReady: true, ReadyFrameSHA256: Digest{2}, GateRelease: true,
			GateProfile: true, ProfileFrameSHA256: Digest{3},
		},
		{
			EncodedSizeBytes: 1, Sequence: 1, Phase: ConfinementPhaseChildDurable,
			ProgressPhase: ConfinementPhaseChildDurable, LaunchIntentSHA256: Digest{1},
			GateReady: true, ReadyFrameSHA256: Digest{2}, GateRelease: true,
			GateProfile: true, ProfileFrameSHA256: Digest{3}, Child: true,
			ExecFD4EOF: true, ExecEvidence: 1,
		},
		{
			EncodedSizeBytes: 1, Sequence: 1, Phase: ConfinementPhaseIOComplete,
			ProgressPhase: ConfinementPhaseIOComplete, LaunchIntentSHA256: Digest{1},
			GateReady: true, ReadyFrameSHA256: Digest{2}, GateRelease: true,
			GateProfile: true, ProfileFrameSHA256: Digest{3}, Child: true, IO: true,
			IOEvidence: &ioEvidence, ExecFD4EOF: true, ExecEvidence: 2,
		},
	}
	for index, state := range tests {
		if err := state.Validate(); err != nil {
			t.Fatalf("row %d: valid state rejected: %v", index, err)
		}
		state.GateReady = !state.GateReady
		if err := state.Validate(); err == nil {
			t.Fatalf("row %d: wrong presence accepted", index)
		}
	}
}

// Rationale: only linear progress, terminal/reaped completion, or explicit
// recovery retirement may advance durable state; phase shortcuts enable replay.
func TestConfinementStateTransitionsAreClosed(t *testing.T) {
	created := ConfinementStateShape{
		EncodedSizeBytes: 1, Sequence: 1, Phase: ConfinementPhaseCreated,
		ProgressPhase: ConfinementPhaseCreated, LaunchIntentSHA256: Digest{1},
	}
	gateDurable := ConfinementStateShape{
		EncodedSizeBytes: 1, Sequence: 2, Phase: ConfinementPhaseGateDurable,
		ProgressPhase: ConfinementPhaseGateDurable, LaunchIntentSHA256: Digest{1},
		GateReady: true, ReadyFrameSHA256: Digest{2},
	}
	if !ValidConfinementTransition(created, gateDurable) {
		t.Fatal("created to gate-durable transition rejected")
	}
	fatalEvidence := ConfinementFatalEvidence{
		Stage: ConfinementFatalSetProcessGroup, Errno: 1, Sequence: 1,
		FD4EOF: true, FrameSHA256: Digest{1},
	}
	terminalEvidence := ConfinementTerminal{
		Kind: ConfinementTerminalGateFatal, ExitCode: 1,
	}
	terminal := ConfinementStateShape{
		EncodedSizeBytes: 1, Sequence: 2, Phase: ConfinementPhaseTerminal,
		ProgressPhase: ConfinementPhaseCreated, LaunchIntentSHA256: Digest{1},
		GateFatal: &fatalEvidence, Terminal: &terminalEvidence,
	}
	setTestTerminalEvidence(t, &terminal)
	if !ValidConfinementTransition(created, terminal) {
		t.Fatal("created to terminal transition rejected")
	}
	recoveryEvidence := ConfinementTerminal{
		Kind: ConfinementTerminalUnavailableNotParent,
	}
	recovery := ConfinementStateShape{
		EncodedSizeBytes: 1, Sequence: 2, Phase: ConfinementPhaseRecoveryRetired,
		ProgressPhase: ConfinementPhaseCreated, LaunchIntentSHA256: Digest{1},
		Terminal: &recoveryEvidence,
	}
	setTestTerminalEvidence(t, &recovery)
	if !ValidConfinementTransition(created, recovery) {
		t.Fatal("created to recovery-retired transition rejected")
	}
	illegal := gateDurable
	illegal.Phase = ConfinementPhaseProfileApplied
	illegal.ProgressPhase = ConfinementPhaseProfileApplied
	illegal.GateRelease = true
	illegal.GateProfile = true
	illegal.ProfileFrameSHA256 = Digest{3}
	if ValidConfinementTransition(created, illegal) {
		t.Fatal("created to profile-applied shortcut accepted")
	}
}

// Rationale: release measurements are external authority, so an intent cannot
// authenticate a helper, gate, client, profile, or seccomp digest by repeating it.
func TestConfinementLaunchIntentRequiresSealedReleaseMeasurements(t *testing.T) {
	intent := testConfinementIntent(t)
	tests := []struct {
		name   string
		mutate func(*ConfinementReleaseAuthority)
	}{
		{"helper", func(authority *ConfinementReleaseAuthority) { authority.HelperSHA256[0]++ }},
		{"gate", func(authority *ConfinementReleaseAuthority) { authority.GateSHA256[0]++ }},
		{"client", func(authority *ConfinementReleaseAuthority) { authority.PGDumpSHA256[0]++ }},
		{"profile", func(authority *ConfinementReleaseAuthority) { authority.LaunchProfileSHA256[0]++ }},
		{"seccomp", func(authority *ConfinementReleaseAuthority) { authority.GateSeccompSHA256[0]++ }},
	}
	for _, test := range tests {
		authority := testConfinementRelease(t)
		test.mutate(&authority)
		if err := intent.Validate(authority); err == nil {
			t.Fatalf("%s release measurement mismatch accepted", test.name)
		}
	}
}

// Rationale: READY is the last root-privileged observation before release, so
// every parent, gate-file, argv, environment, and fd-set fact must match intent.
func TestConfinementReadyBindsFullObservedIdentity(t *testing.T) {
	intent := testConfinementIntent(t)
	intentSHA256 := testConfinementDigest(t, 0x51)
	tests := []struct {
		name   string
		mutate func(*ConfinementProcessIdentity, *ConfinementProcessIdentity)
	}{
		{"parent executable", func(_ *ConfinementProcessIdentity, parent *ConfinementProcessIdentity) {
			parent.Executable.Inode++
		}},
		{"gate file", func(gate *ConfinementProcessIdentity, _ *ConfinementProcessIdentity) {
			gate.Executable.Device++
		}},
		{"gate argv", func(gate *ConfinementProcessIdentity, _ *ConfinementProcessIdentity) {
			gate.ArgvSHA256[0]++
		}},
		{"gate environment", func(gate *ConfinementProcessIdentity, _ *ConfinementProcessIdentity) {
			gate.EnvironmentSHA256[0]++
		}},
		{"gate fd set", func(gate *ConfinementProcessIdentity, _ *ConfinementProcessIdentity) {
			gate.FDSetSHA256[0]++
		}},
	}
	for _, test := range tests {
		gate := testReadyGate(intent)
		parent := intent.Supervisor
		test.mutate(&gate, &parent)
		status := ConfinementGateStatus{
			Schema: 1, Sequence: 1, Kind: ConfinementGateStatusReady,
			Nonce: intent.Nonce, IntentSHA256: intentSHA256, Ready: &gate,
		}
		if err := status.Validate(intent, testConfinementRelease(t), intentSHA256, parent); err == nil {
			t.Fatalf("changed %s accepted", test.name)
		}
	}
}

// Rationale: PROFILE_APPLIED is the canonical post-close-range fd proof; a
// missing or merely nonzero digest cannot establish the launch intent's fd set.
func TestConfinementProfileAppliedRequiresExactFDSetDigest(t *testing.T) {
	intent := testConfinementIntent(t)
	intentSHA256 := testConfinementDigest(t, 0x51)
	profile := intent.ChildSecurity
	status := ConfinementGateStatus{
		Schema: 1, Sequence: 2, Kind: ConfinementGateStatusProfileApplied,
		Nonce: intent.Nonce, IntentSHA256: intentSHA256, Profile: &profile,
		FDSetSHA256: intent.GateProfileFDSetSHA256,
	}
	if err := status.Validate(
		intent,
		testConfinementRelease(t),
		intentSHA256,
		intent.Supervisor,
	); err != nil {
		t.Fatalf("valid PROFILE_APPLIED rejected: %v", err)
	}
	status.FDSetSHA256 = Digest{}
	if err := status.Validate(
		intent,
		testConfinementRelease(t),
		intentSHA256,
		intent.Supervisor,
	); err == nil {
		t.Fatal("zero PROFILE_APPLIED fd-set digest accepted")
	}
	status.FDSetSHA256 = testConfinementDigest(t, 0x61)
	if err := status.Validate(
		intent,
		testConfinementRelease(t),
		intentSHA256,
		intent.Supervisor,
	); err == nil {
		t.Fatal("mismatched PROFILE_APPLIED fd-set digest accepted")
	}
}

// Rationale: fatal-frame evidence and terminal evidence have different signed
// domains; conflating them drops launch, progress, I/O, and wait facts.
func TestConfinementStateBindsFinalFatalEvidence(t *testing.T) {
	fatal := ConfinementFatalEvidence{
		Stage: ConfinementFatalExec, Errno: 1, Sequence: 3,
		FD4EOF: true, FrameSHA256: testConfinementDigest(t, 0x71),
	}
	expectedTerminalEvidence := testConfinementDigestHex(
		t,
		"927252eae6dc3d2ca9d4a1cf653f884f07749c312cd27fd38fee37d534ea554a",
	)
	terminal := ConfinementTerminal{
		Kind: ConfinementTerminalGateFatal, ExitCode: 1,
		EvidenceSHA256: expectedTerminalEvidence,
	}
	state := ConfinementStateShape{
		EncodedSizeBytes: 1, Sequence: 1, Phase: ConfinementPhaseTerminal,
		ProgressPhase: ConfinementPhaseCreated, LaunchIntentSHA256: Digest{1},
		GateFatal: &fatal, Terminal: &terminal,
	}
	computed, err := state.TerminalEvidenceSHA256()
	if err != nil || computed != expectedTerminalEvidence {
		t.Fatalf("terminal evidence golden mismatch: got %x, error %v", computed, err)
	}
	if terminal.EvidenceSHA256 == fatal.FrameSHA256 {
		t.Fatal("terminal evidence reused fatal-frame digest")
	}
	if err := state.Validate(); err != nil {
		t.Fatalf("valid fatal state rejected: %v", err)
	}
	frameAsTerminal := terminal
	frameAsTerminal.EvidenceSHA256 = fatal.FrameSHA256
	frameCandidate := state
	frameCandidate.Terminal = &frameAsTerminal
	if err := frameCandidate.Validate(); err == nil {
		t.Fatal("fatal-frame digest accepted as terminal evidence")
	}
	mutations := []func(*ConfinementStateShape){
		func(value *ConfinementStateShape) { value.GateFatal.Errno = 0 },
		func(value *ConfinementStateShape) { value.GateFatal.Sequence = 2 },
		func(value *ConfinementStateShape) { value.GateFatal.FD4EOF = false },
		func(value *ConfinementStateShape) { value.GateFatal.FrameSHA256 = Digest{} },
		func(value *ConfinementStateShape) { value.Terminal.EvidenceSHA256[0]++ },
	}
	for index, mutate := range mutations {
		fatalCopy := fatal
		terminalCopy := terminal
		candidate := state
		candidate.GateFatal = &fatalCopy
		candidate.Terminal = &terminalCopy
		mutate(&candidate)
		if err := candidate.Validate(); err == nil {
			t.Fatalf("fatal mutation %d accepted", index)
		}
	}
}

// Rationale: present I/O uses the exact three-stream canonical subrecord, not
// the four-byte absent-I/O marker or a length-prefixed alternative.
func TestConfinementTerminalEvidenceCanonicalIOGolden(t *testing.T) {
	ioEvidence := ConfinementIOEvidence{
		Stdin:  ConfinementStreamEvidence{Bytes: 4, SHA256: Digest{4}, EOF: true},
		Stdout: ConfinementStreamEvidence{Bytes: 5, SHA256: Digest{5}, EOF: true},
		Stderr: ConfinementStreamEvidence{Bytes: 6, EOF: true},
	}
	expected := testConfinementDigestHex(
		t,
		"23bc6c16889da1f26b9fb072e6f5d777d5067df068fe71dbc1636f8a40d4146a",
	)
	terminal := ConfinementTerminal{Kind: ConfinementTerminalExited, EvidenceSHA256: expected}
	state := ConfinementStateShape{
		EncodedSizeBytes: 1, Sequence: 1, Phase: ConfinementPhaseTerminal,
		ProgressPhase: ConfinementPhaseIOComplete, LaunchIntentSHA256: Digest{1},
		GateReady: true, ReadyFrameSHA256: Digest{2}, GateRelease: true,
		GateProfile: true, ProfileFrameSHA256: Digest{3}, Child: true, IO: true,
		IOEvidence: &ioEvidence, Terminal: &terminal, ExecFD4EOF: true, ExecEvidence: 2,
	}
	computed, err := state.TerminalEvidenceSHA256()
	if err != nil || computed != expected {
		t.Fatalf("I/O terminal evidence golden mismatch: got %x, error %v", computed, err)
	}
	if err := state.Validate(); err != nil {
		t.Fatalf("valid I/O terminal state rejected: %v", err)
	}
	state.Terminal.EvidenceSHA256[0]++
	if err := state.Validate(); err == nil {
		t.Fatal("mismatched I/O terminal evidence accepted")
	}
}

// Rationale: Wait4 adds raw evidence but cannot rewrite the terminal facts
// already persisted before reaping.
func TestConfinementReapTransitionPreservesTerminalFacts(t *testing.T) {
	terminal := ConfinementTerminal{
		Kind: ConfinementTerminalSignaled, Signal: 11, CoreDumped: true,
		EvidenceSHA256: testConfinementDigest(t, 0x81),
	}
	current := testConfinementChildTerminal(t, 1, ConfinementPhaseTerminal, terminal)
	reapedTerminal := terminal
	reapedTerminal.RawWaitStatus = 11 | 0x80
	reapedTerminal.Wait4Reaped = true
	reapedTerminal.EvidenceSHA256 = testConfinementDigest(t, 0x82)
	next := testConfinementChildTerminal(t, 2, ConfinementPhaseReaped, reapedTerminal)
	if !ValidConfinementTransition(current, next) {
		t.Fatal("matching terminal-to-reaped transition rejected")
	}
	next.Terminal.CoreDumped = false
	next.Terminal.RawWaitStatus = 11
	setTestTerminalEvidence(t, &next)
	if ValidConfinementTransition(current, next) {
		t.Fatal("terminal-to-reaped core fact rewrite accepted")
	}
}

// Rationale: an ordinary client exit is meaningful only after durable exec
// proof; otherwise a pre-exec gate exit could masquerade as a client result.
func TestConfinementNormalTerminalRequiresChildProof(t *testing.T) {
	terminal := ConfinementTerminal{
		Kind: ConfinementTerminalExited, EvidenceSHA256: testConfinementDigest(t, 0x91),
	}
	state := ConfinementStateShape{
		EncodedSizeBytes: 1, Sequence: 1, Phase: ConfinementPhaseTerminal,
		ProgressPhase: ConfinementPhaseCreated, LaunchIntentSHA256: Digest{1},
		Terminal: &terminal,
	}
	setTestTerminalEvidence(t, &state)
	if err := state.Validate(); err == nil {
		t.Fatal("normal terminal without child proof accepted")
	}
}

// Rationale: each public operation has one client path, fd topology, and
// stream contract; a cross-operation substitution would widen the gate.
func TestConfinementOperationPathsFDsAndStreamsAreExact(t *testing.T) {
	proofFD := FDProfile{
		Version: 1, Stdin: FDReadOnlyDevNull, Stdout: FDValidatedResult,
		Stderr: FDBoundedDiagnostic,
	}
	artifactFD := FDProfile{
		Version: 1, Stdin: FDReadOnlyDevNull, Stdout: FDArtifactOutput,
		Stderr: FDBoundedDiagnostic,
	}
	restoreFD := FDProfile{
		Version: 1, Stdin: FDValidatedRestoreInput, Stdout: FDDiscardCount,
		Stderr: FDBoundedDiagnostic,
	}
	probePolicy := StreamPolicy{
		Input: InputNone, Output: OutputProof, OutputLimit: ProbeStdoutLimitBytes,
		StderrLimit: ProbeStderrLimitBytes,
	}
	proofPolicy := StreamPolicy{
		Input: InputNone, Output: OutputProof, OutputLimit: DiagnosticLimitBytes,
		StderrLimit: DiagnosticLimitBytes,
	}
	tests := []struct {
		operation Operation
		path      string
		profile   FDProfile
		policy    StreamPolicy
	}{
		{OperationProbePGDump, PGDumpPath, proofFD, probePolicy},
		{OperationProbePGRestore, PGRestorePath, proofFD, probePolicy},
		{OperationProbePSQL, PSQLPath, proofFD, probePolicy},
		{OperationServerMajor, PSQLPath, proofFD, proofPolicy},
		{
			OperationDump, PGDumpPath, artifactFD,
			StreamPolicy{Input: InputNone, Output: OutputArtifact, StderrLimit: DiagnosticLimitBytes},
		},
		{
			OperationRestoreList, PGRestorePath, restoreFD,
			StreamPolicy{
				Input: InputRestoreSource, Output: OutputDiscardCount,
				StderrLimit: DiagnosticLimitBytes,
			},
		},
		{OperationTerminateDBConnections, PSQLPath, proofFD, proofPolicy},
		{OperationAssertZeroDBConnections, PSQLPath, proofFD, proofPolicy},
		{
			OperationRestoreApply, PGRestorePath, restoreFD,
			StreamPolicy{
				Input: InputRestoreSource, Output: OutputDiscardCount,
				OutputLimit: DiagnosticLimitBytes, StderrLimit: DiagnosticLimitBytes,
			},
		},
		{OperationPostRestoreVerify, PSQLPath, proofFD, proofPolicy},
	}
	for _, test := range tests {
		path, pathErr := ClientPath(test.operation)
		profile, profileErr := ProfileFor(test.operation)
		policy, policyErr := PolicyFor(test.operation)
		if pathErr != nil || profileErr != nil || policyErr != nil {
			t.Fatalf("operation %s rejected", test.operation)
		}
		if path != test.path || profile != test.profile || policy != test.policy {
			t.Fatalf("operation %s contract mismatch", test.operation)
		}
	}
}

func testConfinementChildTerminal(
	t *testing.T,
	sequence uint64,
	phase ConfinementStatePhase,
	terminal ConfinementTerminal,
) ConfinementStateShape {
	t.Helper()
	state := ConfinementStateShape{
		EncodedSizeBytes: 1, Sequence: sequence, Phase: phase,
		ProgressPhase: ConfinementPhaseChildDurable, LaunchIntentSHA256: Digest{1},
		GateReady: true, ReadyFrameSHA256: Digest{2}, GateRelease: true,
		ProfileFrameSHA256: Digest{3},
		GateProfile:        true, Child: true, ExecFD4EOF: true, ExecEvidence: 1,
		Terminal: &terminal,
	}
	setTestTerminalEvidence(t, &state)
	return state
}

func testConfinementIntent(t *testing.T) ConfinementLaunchIntent {
	t.Helper()
	nonce, err := ParseNonce(
		"0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
	)
	if err != nil {
		t.Fatalf("ParseNonce() error = %v", err)
	}
	seccomp := testConfinementDigest(t, 0x21)
	profile, err := ProfileFor(OperationProbePGDump)
	if err != nil {
		t.Fatalf("ProfileFor() error = %v", err)
	}
	return ConfinementLaunchIntent{
		Schema: 1, Nonce: nonce, Operation: OperationProbePGDump,
		DeadlineUnixNano: 1,
		Supervisor:       testRootSupervisor(t),
		GateFile: testConfinementFile(
			ClientGatePath,
			ClientGateMode,
		),
		ClientFile:  testConfinementFile(PGDumpPath, 0o755),
		Arguments:   [][]byte{[]byte("pg_dump"), []byte("--version")},
		Environment: testConfinementEnvironment(),
		FDProfile:   profile,
		ChildSecurity: ConfinementSecurityProfile{
			RealUID: PostgreSQLUID, EffectiveUID: PostgreSQLUID,
			SavedUID: PostgreSQLUID, FilesystemUID: PostgreSQLUID,
			RealGID: PostgreSQLGID, EffectiveGID: PostgreSQLGID,
			SavedGID: PostgreSQLGID, FilesystemGID: PostgreSQLGID,
			LastCapability: 63, NoNewPrivileges: true, SeccompSHA256: seccomp, ForkSyscallsDenied: true,
		},
		NoFileLimit:            ClientNoFileLimit,
		LaunchProfileSHA256:    testConfinementDigest(t, 0x11),
		GateSeccompSHA256:      seccomp,
		GateArgvSHA256:         testConfinementDigest(t, 0x41),
		GateEnvironmentSHA256:  testConfinementDigest(t, 0x42),
		GateReadyFDSetSHA256:   testConfinementDigest(t, 0x43),
		GateProfileFDSetSHA256: testConfinementDigest(t, 0x44),
	}
}

func testConfinementRelease(t *testing.T) ConfinementReleaseAuthority {
	t.Helper()
	return ConfinementReleaseAuthority{
		HelperSHA256:        testConfinementDigest(t, 1),
		GateSHA256:          testConfinementDigest(t, 1),
		PGDumpSHA256:        testConfinementDigest(t, 1),
		PGRestoreSHA256:     testConfinementDigest(t, 1),
		PSQLSHA256:          testConfinementDigest(t, 1),
		LaunchProfileSHA256: testConfinementDigest(t, 0x11),
		GateSeccompSHA256:   testConfinementDigest(t, 0x21),
	}
}

func testRootSupervisor(t *testing.T) ConfinementProcessIdentity {
	t.Helper()
	boot := ConfinementBootID{}
	boot[0] = 1
	return ConfinementProcessIdentity{
		PID: 100, ParentPID: 1, ProcessGroupID: 90, StartTicks: 1, BootID: boot,
		CapabilityPrm: confinementRootCapabilityMask, CapabilityEff: confinementRootCapabilityMask,
		CapabilityBnd: confinementRootCapabilityMask, NoNewPrivileges: true, SeccompMode: 0, ThreadCount: 2,
		Executable: testConfinementFile(HelperPath, HelperMode),
		ArgvSHA256: testConfinementDigest(t, 1), EnvironmentSHA256: testConfinementDigest(t, 2),
		FDSetSHA256: testConfinementDigest(t, 3),
	}
}

func testReadyGate(intent ConfinementLaunchIntent) ConfinementProcessIdentity {
	gate := intent.Supervisor
	gate.PID = 200
	gate.ParentPID = intent.Supervisor.PID
	gate.ProcessGroupID = gate.PID
	gate.ThreadCount = 1
	gate.SeccompMode = 0
	gate.CapabilityPrm = confinementRootCapabilityMask
	gate.CapabilityEff = confinementRootCapabilityMask
	gate.CapabilityBnd = confinementRootCapabilityMask
	gate.Executable = intent.GateFile
	gate.ArgvSHA256 = intent.GateArgvSHA256
	gate.EnvironmentSHA256 = intent.GateEnvironmentSHA256
	gate.FDSetSHA256 = intent.GateReadyFDSetSHA256
	return gate
}

func testConfinementFile(path string, mode uint32) ConfinementFileIdentity {
	return ConfinementFileIdentity{
		Path: path, Device: 1, Inode: 1, SizeBytes: 1, SHA256: Digest{1},
		UID: 0, GID: 0, Mode: mode, Regular: true, SetUIDAbsent: true, SetGIDAbsent: true,
		FileCapabilitiesAbsent: true,
	}
}

func testConfinementEnvironment() [][]byte {
	environment := Environment()
	result := make([][]byte, len(environment))
	for index := range environment {
		result[index] = []byte(environment[index])
	}
	return result
}

func testPSQLArguments(sql string) []string {
	return []string{
		"psql",
		"--no-psqlrc",
		"--quiet",
		"--tuples-only",
		"--no-align",
		"--set=ON_ERROR_STOP=1",
		"--host=/var/run/postgresql",
		"--username=postgres",
		"--no-password",
		"--dbname=app_012345",
		"--command=" + sql,
	}
}

func byteArguments(arguments []string) [][]byte {
	result := make([][]byte, len(arguments))
	for index := range arguments {
		result[index] = []byte(arguments[index])
	}
	return result
}

func cloneByteVector(value [][]byte) [][]byte {
	result := make([][]byte, len(value))
	for index := range value {
		result[index] = append([]byte(nil), value[index]...)
	}
	return result
}

func testConfinementDigest(t *testing.T, first byte) Digest {
	t.Helper()
	digest := Digest{}
	digest[0] = first
	return digest
}

func testConfinementDigestHex(t *testing.T, value string) Digest {
	t.Helper()
	digest, err := ParseDigest(value)
	if err != nil {
		t.Fatalf("ParseDigest() error = %v", err)
	}
	return digest
}

func setTestTerminalEvidence(t *testing.T, state *ConfinementStateShape) {
	t.Helper()
	digest, err := state.TerminalEvidenceSHA256()
	if err != nil {
		t.Fatalf("TerminalEvidenceSHA256() error = %v", err)
	}
	state.Terminal.EvidenceSHA256 = digest
}
