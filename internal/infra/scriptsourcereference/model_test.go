package scriptsourcereference

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestCanonicalMembersRejectsConflictingStagedAuthorityForOneSourceKey(t *testing.T) {
	stage := StageIdentity{
		EnvironmentID:     "env_test",
		RevisionID:        "task_test",
		RenderGeneration:  1,
		FixedReadRevision: 2,
	}
	members := []Member{
		{
			Reference: Reference{
				OperationID: "op_test", ScriptExecutionID: "exec_network",
				Source:        SourceIdentity{Kind: SourceNetwork, NetworkID: "net_test"},
				SourceOwnerID: "env_test", SourceDigest: "network-digest",
			},
			SourceKey: "/v1/records/environment-projections/env_test", Mode: EvidenceStaged,
			Stage: stage, StagedValue: []byte("network projection"),
		},
		{
			Reference: Reference{
				OperationID: "op_test", ScriptExecutionID: "exec_volume",
				Source:        SourceIdentity{Kind: SourceVolume, VolumeID: "vol_test"},
				SourceOwnerID: "env_test", SourceDigest: "volume-digest",
			},
			SourceKey: "/v1/records/environment-projections/env_test", Mode: EvidenceStaged,
			Stage: stage, StagedValue: []byte("volume projection"),
		},
	}
	for index := range members {
		digest := sha256.Sum256(members[index].StagedValue)
		members[index].Stage.CanonicalValueSHA256 = hex.EncodeToString(digest[:])
	}
	if _, _, _, err := canonicalMembers("op_test", members); err == nil {
		t.Fatal("canonicalMembers(conflicting staged source key) error = nil")
	}
}

// Rationale: each candidate execution owns an immutable runner snapshot even when its logical Network and Volume
// sources are shared with another execution.
func TestCanonicalMembersAcceptsSharedNetworkAndVolumeWithExecutionSnapshots(t *testing.T) {
	operationID := "op_shared_sources"
	firstSnapshotDigest := sha256.Sum256([]byte("first canonical runner snapshot"))
	secondSnapshotDigest := sha256.Sum256([]byte("second canonical runner snapshot"))
	sharedNetwork := SourceIdentity{Kind: SourceNetwork, NetworkID: "net_shared"}
	sharedVolume := SourceIdentity{Kind: SourceVolume, VolumeID: "vol_shared"}
	member := func(executionID, snapshotID string, revision int64, digest [sha256.Size]byte, source SourceIdentity) Member {
		return Member{
			Reference: Reference{
				OperationID: operationID, ScriptExecutionID: executionID, Source: source,
				SourceOwnerID: "env_test", SourceModRevision: revision,
				SourceDigest: hex.EncodeToString(digest[:]),
			},
			SourceKey: "/v1/script-runner-snapshots/" + snapshotID, Mode: EvidenceExisting,
		}
	}
	members := []Member{
		member("exec_first", "snp_first", 101, firstSnapshotDigest, sharedNetwork),
		member("exec_first", "snp_first", 101, firstSnapshotDigest, sharedVolume),
		member("exec_second", "snp_second", 202, secondSnapshotDigest, sharedNetwork),
		member("exec_second", "snp_second", 202, secondSnapshotDigest, sharedVolume),
	}

	canonical, _, _, err := canonicalMembers(operationID, members)
	if err != nil {
		t.Fatalf("canonicalMembers(shared Network and Volume) error = %v", err)
	}
	if len(canonical) != 4 {
		t.Fatalf("canonicalMembers(shared Network and Volume) membership count = %d, want 4", len(canonical))
	}
	if canonical[0].Reference.Source != canonical[1].Reference.Source ||
		canonical[0].Reference.Source != sharedNetwork {
		t.Fatalf(
			"canonical Network identities = %#v and %#v, want shared %#v",
			canonical[0].Reference.Source,
			canonical[1].Reference.Source,
			sharedNetwork,
		)
	}
	if canonical[2].Reference.Source != canonical[3].Reference.Source ||
		canonical[2].Reference.Source != sharedVolume {
		t.Fatalf(
			"canonical Volume identities = %#v and %#v, want shared %#v",
			canonical[2].Reference.Source,
			canonical[3].Reference.Source,
			sharedVolume,
		)
	}
}

// Rationale: execution-scoped runner-snapshot evidence must not permit two authorities for one exact membership.
func TestCanonicalMembersRejectsConflictingNetworkEvidenceForSameExecution(t *testing.T) {
	source := SourceIdentity{Kind: SourceNetwork, NetworkID: "net_shared"}
	members := []Member{
		{
			Reference: Reference{
				OperationID: "op_test", ScriptExecutionID: "exec_test", Source: source,
				SourceOwnerID: "env_test", SourceModRevision: 101, SourceDigest: "first-digest",
			},
			SourceKey: "/v1/script-runner-snapshots/snp_first", Mode: EvidenceExisting,
		},
		{
			Reference: Reference{
				OperationID: "op_test", ScriptExecutionID: "exec_test", Source: source,
				SourceOwnerID: "env_test", SourceModRevision: 202, SourceDigest: "second-digest",
			},
			SourceKey: "/v1/script-runner-snapshots/snp_second", Mode: EvidenceExisting,
		},
	}

	_, _, _, err := canonicalMembers("op_test", members)
	if err == nil || err.Error() != "validation: source membership conflicts" {
		t.Fatalf("canonicalMembers(conflicting same-execution Network) error = %v, want membership conflict", err)
	}
}

// Rationale: a Network identity has one stable owner even when multiple executions retain distinct runner snapshots.
func TestCanonicalMembersRejectsConflictingNetworkEvidenceAcrossOwners(t *testing.T) {
	source := SourceIdentity{Kind: SourceNetwork, NetworkID: "net_shared"}
	members := []Member{
		{
			Reference: Reference{
				OperationID: "op_test", ScriptExecutionID: "exec_first", Source: source,
				SourceOwnerID: "env_first", SourceModRevision: 101, SourceDigest: "first-digest",
			},
			SourceKey: "/v1/script-runner-snapshots/snp_first", Mode: EvidenceExisting,
		},
		{
			Reference: Reference{
				OperationID: "op_test", ScriptExecutionID: "exec_second", Source: source,
				SourceOwnerID: "env_second", SourceModRevision: 202, SourceDigest: "second-digest",
			},
			SourceKey: "/v1/script-runner-snapshots/snp_second", Mode: EvidenceExisting,
		},
	}

	_, _, _, err := canonicalMembers("op_test", members)
	if err == nil || err.Error() != "validation: source identity has conflicting evidence" {
		t.Fatalf("canonicalMembers(cross-owner Network) error = %v, want source identity conflict", err)
	}
}

// Rationale: the execution-specific physical-source exception is limited to existing immutable runner snapshots.
func TestCanonicalMembersRejectsStagedPhysicalSourceEvidenceAcrossExecutions(t *testing.T) {
	for _, source := range []SourceIdentity{
		{Kind: SourceNetwork, NetworkID: "net_shared"},
		{Kind: SourceVolume, VolumeID: "vol_shared"},
	} {
		source := source
		t.Run(string(source.Kind), func(t *testing.T) {
			firstValue := []byte("first staged value")
			secondValue := []byte("second staged value")
			firstDigest := sha256.Sum256(firstValue)
			secondDigest := sha256.Sum256(secondValue)
			firstStage := StageIdentity{
				EnvironmentID: "env_test", RevisionID: "rev_test", RenderGeneration: 1,
				FixedReadRevision: 17, CanonicalValueSHA256: hex.EncodeToString(firstDigest[:]),
			}
			secondStage := firstStage
			secondStage.CanonicalValueSHA256 = hex.EncodeToString(secondDigest[:])
			members := []Member{
				{
					Reference: Reference{
						OperationID: "op_test", ScriptExecutionID: "exec_first", Source: source,
						SourceOwnerID: "env_test", SourceDigest: "first-digest",
					},
					SourceKey: "/v1/staging/first", Mode: EvidenceStaged,
					Stage: firstStage, StagedValue: firstValue,
				},
				{
					Reference: Reference{
						OperationID: "op_test", ScriptExecutionID: "exec_second", Source: source,
						SourceOwnerID: "env_test", SourceDigest: "second-digest",
					},
					SourceKey: "/v1/staging/second", Mode: EvidenceStaged,
					Stage: secondStage, StagedValue: secondValue,
				},
			}

			_, _, _, err := canonicalMembers("op_test", members)
			if err == nil || err.Error() != "validation: source identity has conflicting evidence" {
				t.Fatalf("canonicalMembers(staged %s) error = %v, want source identity conflict", source.Kind, err)
			}
		})
	}
}

// Rationale: only Network and Volume identities derive evidence from each execution's runner snapshot.
func TestCanonicalMembersRejectsConflictingEvidenceForSharedService(t *testing.T) {
	source := SourceIdentity{Kind: SourceService, ServiceID: "svc_shared"}
	members := []Member{
		{
			Reference: Reference{
				OperationID: "op_test", ScriptExecutionID: "exec_first", Source: source,
				SourceOwnerID: "env_test", SourceModRevision: 101, SourceDigest: "first-digest",
			},
			SourceKey: "/v1/records/services/svc_shared", Mode: EvidenceExisting,
		},
		{
			Reference: Reference{
				OperationID: "op_test", ScriptExecutionID: "exec_second", Source: source,
				SourceOwnerID: "env_test", SourceModRevision: 202, SourceDigest: "second-digest",
			},
			SourceKey: "/v1/records/services/svc_shared", Mode: EvidenceExisting,
		},
	}

	_, _, _, err := canonicalMembers("op_test", members)
	if err == nil || err.Error() != "validation: source identity has conflicting evidence" {
		t.Fatalf("canonicalMembers(conflicting shared Service) error = %v, want source identity conflict", err)
	}
}
