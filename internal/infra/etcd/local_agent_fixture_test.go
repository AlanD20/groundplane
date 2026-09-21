package etcd

import (
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/ids"
	testlocalagents "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
)

func localAgentTestRecord(
	token [agentprotocol.RawTokenBytes]byte,
	now time.Time,
) testlocalagents.LocalAgentRecord {
	digest := sha256.Sum256(token[:])
	return testlocalagents.LocalAgentRecord{
		ID:               ids.NewAt(ids.KindAgent, now, 41),
		EnrollmentTaskID: ids.NewAt(ids.KindTask, now, 40),
		Image:            "ghcr.io/groundplane/agent@sha256:" + strings.Repeat("a", 64),
		Generation:       1, Phase: testlocalagents.LocalAgentPhaseProvisioning,
		Config: testlocalagents.LocalAgentConfig{
			PullIntervalSeconds: 5, MaxConcurrentTasks: 4,
			Labels: map[string]string{"role": "local"},
		},
		EncryptedToken: []byte("age-encrypted-token"),
		TokenDigest:    base64.RawURLEncoding.EncodeToString(digest[:]),
		CreatedAt:      now, TokenUpdatedAt: now,
	}
}

func localAgentTestToken(seed byte) [agentprotocol.RawTokenBytes]byte {
	var token [agentprotocol.RawTokenBytes]byte
	for index := range token {
		token[index] = seed
	}
	return token
}
