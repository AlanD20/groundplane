package agentregistration

import (
	bytes "bytes"
	context "context"
	sha256 "crypto/sha256"
	base64 "encoding/base64"
	errors "errors"
	agentprotocol "github.com/AlanD20/groundplane/internal/common/agentprotocol"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testlocalagents "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	strings "strings"
	testing "testing"
	time "time"
)

func TestLocalAgentRepositoryLifecycleAndAuthentication(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newRepository(store)
	if err != nil {
		t.Fatalf("newLocalAgentRepository() error = %v", err)
	}
	token := localAgentTestToken(7)
	record := localAgentTestRecord(token, taskJournalTime())

	created, err := repository.CreateSingleton(ctx, record)
	if err != nil {
		t.Fatalf("CreateSingleton() error = %v", err)
	}
	if created.Revision <= 0 || created.Record.Phase != testlocalagents.LocalAgentPhaseProvisioning {
		t.Fatalf("CreateSingleton() = %#v", created)
	}
	for _, key := range testlocalagents.LocalAgentKeys(record.ID, record.TokenDigest) {
		assertTaskLifecycleValue(t, store, key, true)
	}

	stored, err := repository.GetSingleton(ctx)
	if err != nil {
		t.Fatalf("GetSingleton() error = %v", err)
	}
	if stored.Record.ID != record.ID || stored.Record.TokenDigest != record.TokenDigest ||
		!bytes.Equal(stored.Record.EncryptedToken, record.EncryptedToken) ||
		stored.Record.Config.MaxConcurrentTasks != record.Config.MaxConcurrentTasks {
		t.Fatalf("GetSingleton() = %#v", stored)
	}
	authorization, err := repository.ResolveAgentChannel(ctx, record.ID, token)
	if err != nil {
		t.Fatalf("ResolveAgentChannel() error = %v", err)
	}
	if authorization.AgentID != record.ID || authorization.Generation != record.Generation ||
		authorization.Config.PullIntervalSeconds != record.Config.PullIntervalSeconds {
		t.Fatalf("ResolveAgentChannel() = %#v", authorization)
	}
	wrong := localAgentTestToken(8)
	if _, err := repository.ResolveAgentChannel(ctx, record.ID, wrong); !errors.Is(
		err,
		errs.New(errs.KindAgentNotFound, ""),
	) {
		t.Fatalf("ResolveAgentChannel(wrong) error = %v, want agent.not_found", err)
	}

	readyAt := record.CreatedAt.Add(time.Second)
	ready, err := repository.MarkReady(ctx, record.ID, record.Generation, stored.Revision, readyAt)
	if err != nil {
		t.Fatalf("MarkReady() error = %v", err)
	}
	if ready.Record.Phase != testlocalagents.LocalAgentPhaseReady || !ready.Record.ReadyAt.Equal(readyAt) ||
		ready.Revision <= stored.Revision {
		t.Fatalf("MarkReady() = %#v", ready)
	}
	deleting, err := repository.BeginDelete(ctx, record.ID, record.Generation, ready.Revision)
	if err != nil {
		t.Fatalf("BeginDelete() error = %v", err)
	}
	if deleting.Record.Phase != testlocalagents.LocalAgentPhaseDeleting || deleting.Record.TokenDigest != "" {
		t.Fatalf("BeginDelete() = %#v", deleting)
	}
	assertTaskLifecycleValue(t, store, testlocalagents.LocalAgentDigestKey(record.TokenDigest), false)
	if _, err := repository.ResolveAgentChannel(ctx, record.ID, token); !errors.Is(
		err,
		errs.New(errs.KindAgentNotFound, ""),
	) {
		t.Fatalf("ResolveAgentChannel(revoked) error = %v, want agent.not_found", err)
	}
	if err := repository.Delete(ctx, record.ID, record.Generation, deleting.Revision); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := repository.GetSingleton(ctx); !errors.Is(err, errs.New(errs.KindAgentNotFound, "")) {
		t.Fatalf("GetSingleton(deleted) error = %v, want agent.not_found", err)
	}
	if err := repository.Delete(ctx, record.ID, record.Generation, deleting.Revision); err != nil {
		t.Fatalf("Delete(replay) error = %v", err)
	}
}

func TestLocalAgentRepositoryAtomicallyRotatesReplacementGeneration(t *testing.T) {

	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newRepository(store)
	if err != nil {
		t.Fatalf("newLocalAgentRepository() error = %v", err)
	}
	oldToken := localAgentTestToken(31)
	record := localAgentTestRecord(oldToken, taskJournalTime())
	created, err := repository.CreateSingleton(ctx, record)
	if err != nil {
		t.Fatalf("CreateSingleton() error = %v", err)
	}
	firstReadyAt := record.CreatedAt.Add(time.Second)
	ready, err := repository.MarkReady(ctx, record.ID, record.Generation, created.Revision, firstReadyAt)
	if err != nil {
		t.Fatalf("MarkReady() error = %v", err)
	}

	newToken := localAgentTestToken(32)
	newDigestBytes := sha256.Sum256(newToken[:])
	newDigest := base64.RawURLEncoding.EncodeToString(newDigestBytes[:])
	newImage := "ghcr.io/groundplane/agent@sha256:" + strings.Repeat("b", 64)
	replaced, err := repository.ReplaceGeneration(
		ctx,
		ready,
		newImage,
		[]byte("new-age-encrypted-token"),
		newDigest,
		firstReadyAt.Add(time.Second),
	)
	if err != nil {
		t.Fatalf("ReplaceGeneration() error = %v", err)
	}
	if replaced.Record.Image != newImage || replaced.Record.Generation != record.Generation+1 ||
		replaced.Record.Phase != testlocalagents.LocalAgentPhaseUpdating ||
		!replaced.Record.ReadyAt.Equal(firstReadyAt) ||
		replaced.Record.Config.MaxConcurrentTasks != record.Config.MaxConcurrentTasks {
		t.Fatalf("ReplaceGeneration() = %#v", replaced)
	}
	assertTaskLifecycleValue(t, store, testlocalagents.LocalAgentDigestKey(record.TokenDigest), false)
	assertTaskLifecycleValue(t, store, testlocalagents.LocalAgentDigestKey(newDigest), true)
	if _, err := repository.ResolveAgentChannel(ctx, record.ID, oldToken); !errors.Is(
		err,
		errs.New(errs.KindAgentNotFound, ""),
	) {
		t.Fatalf("ResolveAgentChannel(old token) error = %v, want agent.not_found", err)
	}
	authorization, err := repository.ResolveAgentChannel(ctx, record.ID, newToken)
	if err != nil || authorization.Generation != record.Generation+1 {
		t.Fatalf("ResolveAgentChannel(new token) = %#v, %v", authorization, err)
	}

	completed, err := repository.MarkReplacementReady(
		ctx,
		record.ID,
		replaced.Record.Generation,
		replaced.Revision,
	)
	if err != nil {
		t.Fatalf("MarkReplacementReady() error = %v", err)
	}
	if completed.Record.Phase != testlocalagents.LocalAgentPhaseReady ||
		!completed.Record.ReadyAt.Equal(firstReadyAt) {
		t.Fatalf("MarkReplacementReady() = %#v", completed)
	}
}

func TestLocalAgentRepositoryEnforcesOneAtomicSingleton(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newRepository(store)
	if err != nil {
		t.Fatalf("newLocalAgentRepository() error = %v", err)
	}
	first := localAgentTestRecord(localAgentTestToken(11), taskJournalTime())
	if _, err := repository.CreateSingleton(ctx, first); err != nil {
		t.Fatalf("CreateSingleton(first) error = %v", err)
	}
	second := localAgentTestRecord(localAgentTestToken(12), taskJournalTime().Add(time.Second))
	if _, err := repository.CreateSingleton(ctx, second); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("CreateSingleton(second) error = %v, want state.conflict", err)
	}
	assertTaskLifecycleValue(t, store, testlocalagents.LocalAgentPrimaryKey(second.ID), false)
	assertTaskLifecycleValue(t, store, testlocalagents.LocalAgentTokenKey(second.ID), false)
}

func TestLocalAgentRepositoryPreservesUnknownCreateOutcome(t *testing.T) {
	store := newMemoryTaskStore()
	repository, err := newRepository(store)
	if err != nil {
		t.Fatalf("newLocalAgentRepository() error = %v", err)
	}
	record := localAgentTestRecord(localAgentTestToken(21), taskJournalTime())
	store.failAfterCommit(errs.New(errs.KindStorageUnavailable, "unknown outcome"))
	if _, err := repository.CreateSingleton(context.Background(), record); !errors.Is(
		err,
		errs.New(errs.KindStorageUnavailable, ""),
	) {
		t.Fatalf("CreateSingleton(unknown) error = %v, want storage.unavailable", err)
	}
	if _, err := repository.GetSingleton(context.Background()); err != nil {
		t.Fatalf("GetSingleton(after unknown commit) error = %v", err)
	}
}

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
