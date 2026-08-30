package materializationproof

import (
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: proof identity must not depend on caller slice order, while the
// persisted form must have one deterministic member and source ordering.
func TestNewCanonicalizesMembersAndSources(t *testing.T) {
	t.Parallel()

	input := validInput()
	input.Members[0], input.Members[1] = input.Members[1], input.Members[0]
	input.Members[1].EntryGenerations[0], input.Members[1].EntryGenerations[1] =
		input.Members[1].EntryGenerations[1], input.Members[1].EntryGenerations[0]
	input.Members[1].ReusableSecrets[0], input.Members[1].ReusableSecrets[1] =
		input.Members[1].ReusableSecrets[1], input.Members[1].ReusableSecrets[0]

	proof, err := New(input)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	canonical, err := New(validInput())
	if err != nil {
		t.Fatalf("New(canonical): %v", err)
	}
	if !reflect.DeepEqual(proof.Record(), canonical.Record()) {
		t.Fatalf("canonical records differ:\n%#v\n%#v", proof.Record(), canonical.Record())
	}
	record := proof.Record()
	if record.MemberCount != uint32(len(record.Members)) || record.CanonicalSHA256 == "" {
		t.Fatalf("record identity is incomplete: %#v", record)
	}
	if record.Members[0].MaterializationID >= record.Members[1].MaterializationID {
		t.Fatalf("members are not sorted: %#v", record.Members)
	}
	member := record.Members[0]
	if len(member.EntryGenerations) == 0 {
		member = record.Members[1]
	}
	if member.EntryGenerations[0].EntryID >= member.EntryGenerations[1].EntryID {
		t.Fatalf("Entry generations are not sorted: %#v", member.EntryGenerations)
	}
	if member.ReusableSecrets[0].SecretID >= member.ReusableSecrets[1].SecretID {
		t.Fatalf("Secrets are not sorted: %#v", member.ReusableSecrets)
	}
}

// Rationale: generated Environment output is shared only when both Service
// fields are empty; either field alone is an ambiguous physical binding.
func TestNewRejectsMalformedGeneratedEnvironmentServiceBinding(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*MemberRecord)
	}{
		{name: "id without name", mutate: func(member *MemberRecord) { member.ServiceName = "" }},
		{name: "name without id", mutate: func(member *MemberRecord) { member.ServiceID = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := validInput()
			member := &input.Members[1]
			test.mutate(member)
			if member.ServiceName == "" {
				member.Destination = "secrets/.env." + input.EnvironmentID
			}
			if _, err := New(input); !isValidation(err) {
				t.Fatalf("New error = %v, want validation", err)
			}
		})
	}
}

// Rationale: different logical materializations cannot claim the same file
// below one Environment root, regardless of their distinct stable ids.
func TestNewRejectsDuplicatePhysicalDestination(t *testing.T) {
	t.Parallel()
	input := validInput()
	duplicate := input.Members[0]
	duplicate.MaterializationID = stableID(ids.KindConfig, 90)
	duplicate.EntryGenerations = nil
	duplicate.ReusableSecrets = nil
	input.Members = append(input.Members, duplicate)
	if _, err := New(input); !isValidation(err) {
		t.Fatalf("New error = %v, want validation", err)
	}
}

// Rationale: present and absent are independent durable outcomes; a removal
// proof must retain absence evidence instead of being inferred from file kind.
func TestNewPreservesPresentAndAbsentOutcomes(t *testing.T) {
	t.Parallel()

	proof, err := New(validInput())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	record := proof.Record()
	foundPresent, foundAbsent := false, false
	for _, member := range record.Members {
		foundPresent = foundPresent || member.Outcome == OutcomePresent && member.OutputKind == OutputSecretFile
		foundAbsent = foundAbsent || member.Outcome == OutcomeAbsent && member.OutputKind == OutputGeneratedEnvironment
	}
	if !foundPresent || !foundAbsent {
		t.Fatalf("outcomes changed: %#v", record.Members)
	}
}

// Rationale: invalid owners, content policy, digests, and cardinality must be
// rejected before an immutable proof can become persistence input.
func TestNewRejectsInvalidIdentityPolicyDigestAndLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Input)
	}{
		{name: "environment", mutate: func(input *Input) { input.EnvironmentID = "env_invalid" }},
		{name: "service", mutate: func(input *Input) { input.Members[0].ServiceID = "svc_invalid" }},
		{name: "digest", mutate: func(input *Input) { input.Members[0].AbsenceOrOwnershipSHA256 = "ABC" }},
		{name: "absent content", mutate: func(input *Input) { input.Members[1].Length = 1 }},
		{name: "mode", mutate: func(input *Input) { input.Members[1].Mode = 0o644 }},
		{
			name:   "duplicate member",
			mutate: func(input *Input) { input.Members = append(input.Members, input.Members[0]) },
		},
		{name: "duplicate materialization", mutate: func(input *Input) {
			duplicate := input.Members[0]
			duplicate.ServiceID = input.Members[1].ServiceID
			duplicate.Destination = "config/duplicate.key"
			input.Members = append(input.Members, duplicate)
		}},
		{name: "source limit", mutate: func(input *Input) {
			input.Members[0].EntryGenerations = make([]EntryGenerationRecord, MaximumSourcesPerMember+1)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := validInput()
			test.mutate(&input)
			_, err := New(input)
			if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed {
				t.Fatalf("New error kind = %v, %t; want validation", kind, ok)
			}
		})
	}
}

// Rationale: restoring storage data must require its exact declared count,
// ordering, and canonical digest rather than silently normalizing corruption.
func TestRestoreRejectsNonCanonicalOrChangedRecord(t *testing.T) {
	t.Parallel()

	proof, err := New(validInput())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*Record)
	}{
		{name: "count", mutate: func(record *Record) { record.MemberCount++ }},
		{name: "digest", mutate: func(record *Record) { record.CanonicalSHA256 = digest("changed") }},
		{name: "order", mutate: func(record *Record) {
			record.Members[0], record.Members[1] = record.Members[1], record.Members[0]
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := proof.Record()
			test.mutate(&record)
			if _, err := Restore(record); err == nil {
				t.Fatal("Restore unexpectedly accepted changed storage data")
			}
		})
	}
}

func validInput() Input {
	environmentID := stableID(ids.KindEnvironment, 1)
	serviceA := stableID(ids.KindService, 2)
	serviceB := stableID(ids.KindService, 3)
	secretA := stableID(ids.KindSecret, 4)
	secretB := stableID(ids.KindSecret, 5)
	return Input{
		EnvironmentID:     environmentID,
		AppliedRevisionID: stableID(ids.KindTask, 6),
		RenderGeneration:  9,
		ProducingTaskID:   stableID(ids.KindTask, 7),
		Members: []MemberRecord{
			{
				MaterializationID: stableID(ids.KindConfig, 8), ServiceID: serviceA,
				Destination: "config/application.key", OutputKind: OutputSecretFile,
				Outcome: OutcomePresent, UID: 1000, GID: 1001, Mode: 0o600,
				Length: 4, ContentSHA256: digest("data"), AbsenceOrOwnershipSHA256: digest("ownership-a"),
				EntryGenerations: []EntryGenerationRecord{
					{
						EntryID: stableID(ids.KindEnvEntry, 9), GenerationID: stableID(ids.KindConfig, 10),
						Storage: EntryStorageSecret, ValueSHA256: digest("entry-b"),
					},
					{
						EntryID: stableID(ids.KindEnvEntry, 8), GenerationID: stableID(ids.KindConfig, 11),
						Storage: EntryStoragePlain, ValueSHA256: digest("entry-a"),
					},
				},
				ReusableSecrets: []ReusableSecretRecord{
					{SecretID: secretA, MetadataRevision: 21, CiphertextSHA256: digest("secret-a")},
					{SecretID: secretB, MetadataRevision: 22, CiphertextSHA256: digest("secret-b")},
				},
			},
			{
				MaterializationID: stableID(ids.KindConfig, 12), ServiceID: serviceB, ServiceName: "api",
				Destination: "secrets/.env." + environmentID + ".api",
				OutputKind:  OutputGeneratedEnvironment, Outcome: OutcomeAbsent,
				UID: 0, GID: 0, Mode: 0o600, ContentSHA256: digest(""),
				AbsenceOrOwnershipSHA256: digest("absence-b"),
			},
		},
	}
}

func stableID(kind ids.Kind, seed int64) string {
	return ids.NewAt(kind, time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC), seed)
}

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
