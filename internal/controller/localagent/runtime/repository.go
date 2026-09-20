package runtime

import (
	"context"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type localAgentRecords interface {
	CreateSingleton(context.Context, etcd.LocalAgentRecord) (etcdstore.Versioned[etcd.LocalAgentRecord], error)
	GetSingleton(context.Context) (etcdstore.Versioned[etcd.LocalAgentRecord], error)
	UpdateConfigIdempotent(
		context.Context,
		etcdstore.Versioned[etcd.LocalAgentRecord],
		etcd.LocalAgentConfig,
		idempotencyrecord.IdempotencyMarker,
	) (etcdstore.Versioned[etcd.LocalAgentRecord], etcd.IdempotencyTransactionResult, error)
	MarkReady(context.Context, string, uint64, int64, time.Time) (etcdstore.Versioned[etcd.LocalAgentRecord], error)
	ReplaceGeneration(
		context.Context,
		etcdstore.Versioned[etcd.LocalAgentRecord],
		string,
		[]byte,
		string,
		time.Time,
	) (etcdstore.Versioned[etcd.LocalAgentRecord], error)
	FenceReplacementAttempt(
		context.Context,
		string,
		uint64,
		int64,
	) (etcdstore.Versioned[etcd.LocalAgentRecord], error)
	MarkReplacementReady(
		context.Context,
		string,
		uint64,
		int64,
	) (etcdstore.Versioned[etcd.LocalAgentRecord], error)
	BeginDelete(context.Context, string, uint64, int64) (etcdstore.Versioned[etcd.LocalAgentRecord], error)
	Delete(context.Context, string, uint64, int64) error
}

// localAgentRepositoryAdapter owns all translation between the lifecycle
// aggregate and its etcd representation. Secret-bearing byte slices and label
// maps are copied at both boundaries so neither package shares mutable storage.
type localAgentRepositoryAdapter struct {
	repository  localAgentRecords
	idempotency localAgentConfigIdempotency
	now         func() time.Time
}

func NewRepository(
	repository localAgentRecords,
	idempotency localAgentConfigIdempotency,
) (*localAgentRepositoryAdapter, error) {
	if repository == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "local agent durable repository and config idempotency are required")
	}
	return &localAgentRepositoryAdapter{repository: repository, idempotency: idempotency, now: time.Now}, nil
}

func (adapter *localAgentRepositoryAdapter) CreateSingleton(
	ctx context.Context,
	record localagent.Record,
) (localagent.StoredRecord, error) {
	durable, err := localAgentRecordToDurable(record)
	if err != nil {
		return localagent.StoredRecord{}, err
	}
	stored, err := adapter.repository.CreateSingleton(ctx, durable)
	if err != nil {
		return localagent.StoredRecord{}, err
	}
	return localAgentRecordFromDurable(stored)
}

func (adapter *localAgentRepositoryAdapter) GetSingleton(
	ctx context.Context,
) (localagent.StoredRecord, error) {
	stored, err := adapter.repository.GetSingleton(ctx)
	if err != nil {
		return localagent.StoredRecord{}, err
	}
	return localAgentRecordFromDurable(stored)
}

func (adapter *localAgentRepositoryAdapter) MarkReady(
	ctx context.Context,
	id string,
	generation uint64,
	revision int64,
	readyAt time.Time,
) (localagent.StoredRecord, error) {
	stored, err := adapter.repository.MarkReady(ctx, id, generation, revision, readyAt)
	if err != nil {
		return localagent.StoredRecord{}, err
	}
	return localAgentRecordFromDurable(stored)
}

func (adapter *localAgentRepositoryAdapter) BeginReplacement(
	ctx context.Context,
	current localagent.StoredRecord,
	image string,
	credential localagent.Credential,
	updatedAt time.Time,
) (localagent.StoredRecord, error) {
	durable, err := localAgentRecordToDurable(current.Record)
	if err != nil {
		return localagent.StoredRecord{}, err
	}
	stored, err := adapter.repository.ReplaceGeneration(
		ctx,
		etcdstore.Versioned[etcd.LocalAgentRecord]{
			Record: durable, Revision: current.Revision, ReadRevision: current.Revision,
		},
		image,
		append([]byte(nil), credential.EncryptedToken...),
		credential.Digest,
		updatedAt,
	)
	if err != nil {
		return localagent.StoredRecord{}, err
	}
	return localAgentRecordFromDurable(stored)
}

func (adapter *localAgentRepositoryAdapter) FenceReplacementAttempt(
	ctx context.Context, id string, generation uint64, revision int64,
) (localagent.StoredRecord, error) {
	stored, err := adapter.repository.FenceReplacementAttempt(ctx, id, generation, revision)
	if err != nil {
		return localagent.StoredRecord{}, err
	}
	return localAgentRecordFromDurable(stored)
}

func (adapter *localAgentRepositoryAdapter) MarkReplacementReady(
	ctx context.Context,
	id string,
	generation uint64,
	revision int64,
) (localagent.StoredRecord, error) {
	stored, err := adapter.repository.MarkReplacementReady(ctx, id, generation, revision)
	if err != nil {
		return localagent.StoredRecord{}, err
	}
	return localAgentRecordFromDurable(stored)
}

func (adapter *localAgentRepositoryAdapter) BeginDelete(
	ctx context.Context,
	id string,
	generation uint64,
	revision int64,
) (localagent.StoredRecord, error) {
	stored, err := adapter.repository.BeginDelete(ctx, id, generation, revision)
	if err != nil {
		return localagent.StoredRecord{}, err
	}
	return localAgentRecordFromDurable(stored)
}

func (adapter *localAgentRepositoryAdapter) Delete(
	ctx context.Context,
	id string,
	generation uint64,
	revision int64,
) error {
	return adapter.repository.Delete(ctx, id, generation, revision)
}

func localAgentRecordToDurable(record localagent.Record) (etcd.LocalAgentRecord, error) {
	phase, err := localAgentPhaseToDurable(record.Phase)
	if err != nil {
		return etcd.LocalAgentRecord{}, err
	}
	return etcd.LocalAgentRecord{
		ID: record.ID, EnrollmentTaskID: record.EnrollmentTaskID,
		Image: record.Image, Generation: record.Generation, Phase: phase,
		Config: etcd.LocalAgentConfig{
			PullIntervalSeconds: record.Config.PullIntervalSeconds,
			MaxConcurrentTasks:  record.Config.MaxConcurrentTasks,
			Labels:              cloneLocalAgentLabels(record.Config.Labels),
		},
		EncryptedToken: append([]byte(nil), record.Credential.EncryptedToken...),
		TokenDigest:    record.Credential.Digest,
		CreatedAt:      record.CreatedAt,
		ReadyAt:        record.ReadyAt,
		TokenUpdatedAt: record.CreatedAt,
	}, nil
}

func localAgentRecordFromDurable(
	stored etcdstore.Versioned[etcd.LocalAgentRecord],
) (localagent.StoredRecord, error) {
	phase, err := localAgentPhaseFromDurable(stored.Record.Phase)
	if err != nil {
		return localagent.StoredRecord{}, err
	}
	return localagent.StoredRecord{
		Record: localagent.Record{
			ID: stored.Record.ID, EnrollmentTaskID: stored.Record.EnrollmentTaskID,
			Image:      stored.Record.Image,
			Generation: stored.Record.Generation, Phase: phase,
			Config: localagent.Config{
				PullIntervalSeconds: stored.Record.Config.PullIntervalSeconds,
				MaxConcurrentTasks:  stored.Record.Config.MaxConcurrentTasks,
				Labels:              cloneLocalAgentLabels(stored.Record.Config.Labels),
			},
			Credential: localagent.Credential{
				EncryptedToken: append([]byte(nil), stored.Record.EncryptedToken...),
				Digest:         stored.Record.TokenDigest,
			},
			CreatedAt: stored.Record.CreatedAt,
			ReadyAt:   stored.Record.ReadyAt,
		},
		Revision: stored.Revision,
	}, nil
}

func localAgentPhaseToDurable(phase localagent.Phase) (etcd.LocalAgentPhase, error) {
	switch phase {
	case localagent.PhaseProvisioning:
		return etcd.LocalAgentPhaseProvisioning, nil
	case localagent.PhaseReady:
		return etcd.LocalAgentPhaseReady, nil
	case localagent.PhaseUpdating:
		return etcd.LocalAgentPhaseUpdating, nil
	case localagent.PhaseDeleting:
		return etcd.LocalAgentPhaseDeleting, nil
	default:
		return "", errs.New(errs.KindInternal, "local agent lifecycle phase is invalid")
	}
}

func localAgentPhaseFromDurable(phase etcd.LocalAgentPhase) (localagent.Phase, error) {
	switch phase {
	case etcd.LocalAgentPhaseProvisioning:
		return localagent.PhaseProvisioning, nil
	case etcd.LocalAgentPhaseReady:
		return localagent.PhaseReady, nil
	case etcd.LocalAgentPhaseUpdating:
		return localagent.PhaseUpdating, nil
	case etcd.LocalAgentPhaseDeleting:
		return localagent.PhaseDeleting, nil
	default:
		return "", errs.New(errs.KindInternal, "durable local agent phase is invalid")
	}
}

func cloneLocalAgentLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	cloned := make(map[string]string, len(labels))
	for key, value := range labels {
		cloned[key] = value
	}
	return cloned
}

var _ localagent.Repository = (*localAgentRepositoryAdapter)(nil)
