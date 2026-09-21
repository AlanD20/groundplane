package etcd

import (
	"strconv"
	"strings"
	"testing"

	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
)

// Rationale: staging must measure the actual protobuf request after prefix
// expansion; logical-key lengths alone cannot prove ADR0049's physical bound.
func TestVolumeRemovalEvidenceTransactionSizeBounds(t *testing.T) {
	backend := &store{root: "/groundplane"}
	key := func(ordinal int) string {
		suffix := strconv.Itoa(ordinal)
		return "/" + strings.Repeat("k", 512-len(backend.root)-1-len(suffix)) + suffix
	}
	conditions := []testkeyvalue.Condition{{Key: key(0), ModRevision: 1}, {Key: key(1), ModRevision: 2}}
	mutations := []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key(1), Value: make([]byte, 16*1024)}}
	for index := 0; index < 44; index++ {
		conditions = append(conditions, testkeyvalue.Condition{Key: key(index + 2)})
		mutations = append(
			mutations,
			testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: key(index + 2), Value: make([]byte, 16*1024)},
		)
	}
	size, err := backend.VolumeRemovalEvidenceTransactionSize(conditions, mutations)
	if err != nil || len(conditions)+len(mutations) != 91 || size != 808973 || size > 900*1024 {
		t.Fatal("maximum staging shape", size, err)
	}
	t.Logf(
		"44-row physical maximum: %d comparisons, %d mutations, %d protobuf bytes",
		len(conditions),
		len(mutations),
		size,
	)
	conditions[0].Key += "x"
	if _, err := backend.VolumeRemovalEvidenceTransactionSize(conditions, mutations); err == nil {
		t.Fatal("accepted a 513-byte physical comparison key")
	}
	conditions[0].Key = key(0)
	mutations[0].Key += "x"
	if _, err := backend.VolumeRemovalEvidenceTransactionSize(conditions, mutations); err == nil {
		t.Fatal("accepted a 513-byte physical mutation key")
	}
	mutations[0].Key = key(1)
	mutations[0].Value = make([]byte, removalrecord.EvidenceRecordBytes+1)
	if _, err := backend.VolumeRemovalEvidenceTransactionSize(conditions, mutations); err == nil {
		t.Fatal("accepted an oversized evidence record")
	}
	mutations[0].Value = make([]byte, removalrecord.EvidenceRecordBytes)
	for index := 0; index < 6; index++ {
		conditions = append(conditions, testkeyvalue.Condition{Key: key(100 + index)})
	}
	if _, err := backend.VolumeRemovalEvidenceTransactionSize(conditions, mutations); err == nil {
		t.Fatal("accepted more than 96 compare-and-mutation operations")
	}
}
