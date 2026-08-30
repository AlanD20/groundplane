package materializationproof

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: Script planning must consume only exact required IDs, including a
// shared environment-level member, and derive Secrets only from that subset.
func TestSelectTargetServiceReturnsExactMembersAndSecretSources(t *testing.T) {
	t.Parallel()

	input := targetSelectionInput()
	proof, err := New(input)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	record := proof.Record()
	target := input.Members[0].ServiceID
	selection, err := SelectTargetService(proof, SelectionRequest{
		EnvironmentID:     input.EnvironmentID,
		AppliedRevisionID: input.AppliedRevisionID,
		RenderGeneration:  input.RenderGeneration,
		ProducingTaskID:   input.ProducingTaskID,
		CanonicalSHA256:   record.CanonicalSHA256,
		TargetServiceID:   target,
		RequiredMaterializationIDs: []string{
			input.Members[3].MaterializationID,
			input.Members[1].MaterializationID,
			input.Members[0].MaterializationID,
		},
	})
	if err != nil {
		t.Fatalf("SelectTargetService: %v", err)
	}
	members := selection.Members()
	foundShared := false
	for _, member := range members {
		foundShared = foundShared || member.ServiceID == ""
	}
	if len(members) != 3 || !uniquelySortedMembers(members) || !foundShared {
		t.Fatalf("selected members = %#v", members)
	}
	secrets := selection.ReusableSecrets()
	if len(secrets) != 2 || secrets[0].SecretID >= secrets[1].SecretID {
		t.Fatalf("selected Secrets = %#v", secrets)
	}
	withoutShared, err := SelectTargetService(proof, SelectionRequest{
		EnvironmentID:     input.EnvironmentID,
		AppliedRevisionID: input.AppliedRevisionID,
		RenderGeneration:  input.RenderGeneration,
		ProducingTaskID:   input.ProducingTaskID,
		CanonicalSHA256:   record.CanonicalSHA256,
		TargetServiceID:   target,
		RequiredMaterializationIDs: []string{
			input.Members[0].MaterializationID,
			input.Members[1].MaterializationID,
		},
	})
	if err != nil || len(withoutShared.Members()) != 2 {
		t.Fatalf("selection without shared member = %#v, %v", withoutShared.Members(), err)
	}
	for _, member := range withoutShared.Members() {
		if member.ServiceID == "" {
			t.Fatalf("unrequired shared member was selected: %#v", member)
		}
	}
}

// Rationale: stale identity, missing members, and a materialization bound to a
// different nonempty Service must all fail closed.
func TestSelectTargetServiceRejectsStaleMissingExtraAndWrongBinding(t *testing.T) {
	t.Parallel()

	input := targetSelectionInput()
	proof, err := New(input)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	record := proof.Record()
	base := SelectionRequest{
		EnvironmentID: input.EnvironmentID, AppliedRevisionID: input.AppliedRevisionID,
		RenderGeneration: input.RenderGeneration, ProducingTaskID: input.ProducingTaskID,
		CanonicalSHA256: record.CanonicalSHA256, TargetServiceID: input.Members[0].ServiceID,
		RequiredMaterializationIDs: []string{input.Members[0].MaterializationID, input.Members[1].MaterializationID},
	}
	tests := []struct {
		name   string
		mutate func(*SelectionRequest)
	}{
		{name: "older generation", mutate: func(request *SelectionRequest) { request.RenderGeneration++ }},
		{name: "changed revision", mutate: func(request *SelectionRequest) {
			request.AppliedRevisionID = stableID(ids.KindTask, 90)
		}},
		{
			name:   "changed digest",
			mutate: func(request *SelectionRequest) { request.CanonicalSHA256 = digest("other-proof") },
		},
		{name: "missing", mutate: func(request *SelectionRequest) {
			request.RequiredMaterializationIDs[1] = stableID(ids.KindConfig, 91)
		}},
		{name: "extra target", mutate: func(request *SelectionRequest) {
			request.RequiredMaterializationIDs = request.RequiredMaterializationIDs[:1]
		}},
		{name: "other service", mutate: func(request *SelectionRequest) {
			request.RequiredMaterializationIDs = append(
				request.RequiredMaterializationIDs,
				input.Members[2].MaterializationID,
			)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := base
			request.RequiredMaterializationIDs = append([]string(nil), base.RequiredMaterializationIDs...)
			test.mutate(&request)
			_, err := SelectTargetService(proof, request)
			if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStateConflict {
				t.Fatalf("error kind = %v, %t; want state conflict", kind, ok)
			}
		})
	}
}

// Rationale: duplicate required IDs make the Script input ambiguous even when
// the complete proof itself is globally unique.
func TestSelectTargetServiceRejectsDuplicateRequiredMaterialization(t *testing.T) {
	t.Parallel()

	input := targetSelectionInput()
	proof, err := New(input)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	request := SelectionRequest{
		EnvironmentID:     input.EnvironmentID,
		AppliedRevisionID: input.AppliedRevisionID,
		RenderGeneration:  input.RenderGeneration,
		ProducingTaskID:   input.ProducingTaskID,
		CanonicalSHA256:   proof.CanonicalSHA256(),
		TargetServiceID:   input.Members[0].ServiceID,
		RequiredMaterializationIDs: []string{
			input.Members[0].MaterializationID,
			input.Members[0].MaterializationID,
		},
	}
	if _, err := SelectTargetService(proof, request); !isValidation(err) {
		t.Fatalf("SelectTargetService error = %v, want validation", err)
	}
}

func isValidation(err error) bool {
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindValidationFailed
}

func uniquelySortedMembers(members []MemberRecord) bool {
	for index := 1; index < len(members); index++ {
		if members[index-1].MaterializationID >= members[index].MaterializationID {
			return false
		}
	}
	return true
}

func targetSelectionInput() Input {
	input := validInput()
	target := input.Members[0].ServiceID
	shared := input.Members[0].ReusableSecrets[1]
	input.Members[1] = MemberRecord{
		MaterializationID: stableID(ids.KindConfig, 30), ServiceID: target,
		Destination: "config/secondary.key", OutputKind: OutputSecretFile,
		Outcome: OutcomePresent, UID: 1000, GID: 1001, Mode: 0o600,
		Length: 4, ContentSHA256: digest("more"), AbsenceOrOwnershipSHA256: digest("ownership-more"),
		ReusableSecrets: []ReusableSecretRecord{shared},
	}
	input.Members = append(input.Members,
		MemberRecord{
			MaterializationID: stableID(ids.KindConfig, 31), ServiceID: stableID(ids.KindService, 32),
			Destination: "config/other", OutputKind: OutputPlainFile, Outcome: OutcomePresent,
			UID: 1000, GID: 1000, Mode: 0o444, Length: 1,
			ContentSHA256: digest("x"), AbsenceOrOwnershipSHA256: digest("ownership-other"),
		},
		MemberRecord{
			MaterializationID: stableID(ids.KindConfig, 33),
			Destination:       "config/environment", OutputKind: OutputPlainFile, Outcome: OutcomePresent,
			UID: 0, GID: 0, Mode: 0o444, Length: 1,
			ContentSHA256: digest("y"), AbsenceOrOwnershipSHA256: digest("ownership-environment"),
		},
	)
	return input
}
