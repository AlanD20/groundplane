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
