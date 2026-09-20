package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ReleaseGroupExecutionAuthority struct {
	store       etcdstore.Store
	checkpoints *ReleaseCheckpointAuthority
}

func NewReleaseGroupExecutionAuthority(
	store etcdstore.Store,
	checkpoints *ReleaseCheckpointAuthority,
) (*ReleaseGroupExecutionAuthority, error) {
	if store == nil || checkpoints == nil || checkpoints.store == nil {
		return nil, errs.New(errs.KindInternal, "release group execution dependencies are not configured")
	}
	return &ReleaseGroupExecutionAuthority{store: store, checkpoints: checkpoints}, nil
}

type ReleaseGroupMemberCheckpoint struct {
	PublicationID string
	OperationID   string
	EnvironmentID string
	Member        domain.MemberResult
	Evidence      domain.EffectEvidence
	Now           time.Time
}

func (authority *ReleaseGroupExecutionAuthority) RecordMemberServing(
	ctx context.Context,
	input ReleaseGroupMemberCheckpoint,
) (domain.GroupProgress, error) {
	if ctx == nil || authority == nil || input.Member.Outcome != domain.MemberServing {
		return domain.GroupProgress{}, errs.New(
			errs.KindValidationFailed,
			"release group member serving checkpoint is invalid",
		)
	}
	return authority.advanceProgress(ctx, input, domain.StateServing, false)
}

func (authority *ReleaseGroupExecutionAuthority) RecordMemberFailure(
	ctx context.Context,
	input ReleaseGroupMemberCheckpoint,
) (domain.GroupProgress, error) {
	if ctx == nil || authority == nil || input.Member.Outcome != domain.MemberFailed {
		return domain.GroupProgress{}, errs.New(
			errs.KindValidationFailed,
			"release group member failure checkpoint is invalid",
		)
	}
	return authority.advanceProgress(ctx, input, domain.StateFailed, true)
}

func (authority *ReleaseGroupExecutionAuthority) advanceProgress(
	ctx context.Context,
	input ReleaseGroupMemberCheckpoint,
	checkpointState domain.State,
	failure bool,
) (domain.GroupProgress, error) {
	prepared, err := authority.checkpoints.prepareAdvance(ctx, ReleaseCheckpointAdvance{
		PublicationID: input.PublicationID, OperationID: input.OperationID,
		EnvironmentID: input.EnvironmentID, ReleaseID: input.Member.ReleaseID,
		NextState: checkpointState, Evidence: &input.Evidence, Now: input.Now,
	})
	if err != nil {
		return domain.GroupProgress{}, err
	}
	defer prepared.clear()
	head := prepared.head
	if head.Progress == nil || head.ReleaseGroupID == "" {
		return domain.GroupProgress{}, corruptReleaseRecord()
	}
	manifest := domain.GroupManifest{
		OperationID: head.OperationID, ReleaseGroupID: head.ReleaseGroupID,
		EnvironmentID: head.EnvironmentID, FailurePolicy: head.FailurePolicy,
		Members: head.Members, ConfiguredTimeoutSeconds: head.ConfiguredTimeoutSeconds,
		ComputedBudgetSeconds: head.ComputedBudgetSeconds,
	}
	executor, err := domain.NewGroupExecutor(manifest)
	if err != nil {
		return domain.GroupProgress{}, corruptReleaseRecord()
	}
	var next domain.GroupProgress
	if failure {
		next, err = executor.RecordFailure(*head.Progress, input.Member, input.Now)
	} else {
		next, err = executor.RecordServing(*head.Progress, input.Member, input.Now)
	}
	if err != nil {
		return domain.GroupProgress{}, err
	}
	if prepared.duplicate {
		for _, result := range head.Progress.Results {
			if result.Ordinal == input.Member.Ordinal && result == input.Member {
				return *head.Progress, nil
			}
		}
		return domain.GroupProgress{}, corruptReleaseRecord()
	}
	nextHead := cloneReleaseOperationHead(head)
	nextHead.Progress = &next
	nextHead.UpdatedAt = input.Now
	if failure && next.Compensating {
		nextHead.State = domain.StateCompensating
	}
	headValue, err := encodeReleaseRecord("release-operation", nextHead)
	if err != nil {
		return domain.GroupProgress{}, err
	}
	defer clear(headValue)
	prepared.mutations = append(prepared.mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: releaseOperationKey(input.OperationID), Value: headValue,
	})
	result, err := authority.store.Transact(ctx, prepared.conditions, prepared.mutations)
	if err != nil {
		return domain.GroupProgress{}, err
	}
	if !result.Succeeded {
		return domain.GroupProgress{}, errs.New(errs.KindStateConflict, "release group progress changed")
	}
	return next, nil
}
