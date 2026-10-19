package edgechecks

import (
	"time"

	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (record Record) settlementTime(now time.Time) time.Time {
	if now.Before(record.CreatedAt) {
		now = record.CreatedAt
	}
	if record.StartedAt != nil && now.Before(*record.StartedAt) {
		now = *record.StartedAt
	}
	if record.PublicStartedAt != nil && now.Before(*record.PublicStartedAt) {
		now = *record.PublicStartedAt
	}
	return now
}

func (record *Record) start(now, deadline time.Time) error {
	if record.Phase != Ready || !recordcodec.IsCanonicalUTC(now) {
		return errs.New(errs.KindStateConflict, "edge diagnostic has already started")
	}
	now = record.settlementTime(now)
	if !now.Before(deadline) {
		return errs.New(errs.KindStateConflict, "edge diagnostic deadline expired before execution")
	}
	record.Phase, record.StartedAt, record.CleanupPending = PrivateChecking, &now, true
	return nil
}

func (record *Record) observePrivate(result *contract.PrivateResult, cleanupPending bool, failure string) error {
	if record.Phase != PrivateChecking || !validFailure(failure) || result == nil && failure == "" {
		return errs.New(errs.KindStateConflict, "edge private result has no forward owner")
	}
	if result != nil {
		if result.Validate(record.Source.Request) != nil {
			return invalidRecord()
		}
		record.Private = Clone(Record{Private: result}).Private
	}
	record.PrivateFailure, record.CleanupPending = failure, cleanupPending
	record.Phase = PublicChecking
	if cleanupPending {
		record.Phase = Cleaning
	}
	return nil
}

func (record *Record) startPublic(now, deadline time.Time) error {
	if record.Phase != PublicChecking || record.PublicStartedAt != nil || !recordcodec.IsCanonicalUTC(now) {
		return errs.New(
			errs.KindStateConflict,
			"public Route probe has already started or has no settled private check",
		)
	}
	now = record.settlementTime(now)
	if !now.Before(deadline) {
		return errs.New(errs.KindStateConflict, "public Route check deadline expired before execution")
	}
	record.PublicStartedAt = &now
	return nil
}

func (record *Record) observePublic(result *contract.PublicResult, failure string) error {
	if record.Phase != PublicChecking || record.PublicStartedAt == nil || record.Public != nil ||
		record.PublicFailure != "" ||
		!validFailure(failure) ||
		result == nil && failure == "" {
		return errs.New(errs.KindStateConflict, "public Route result has no forward owner")
	}
	if result != nil {
		if result.Validate(record.Source.Request.Target) != nil {
			return invalidRecord()
		}
		record.Public = Clone(Record{Public: result}).Public
	}
	record.PublicFailure = failure
	return nil
}

func (record *Record) finish(now time.Time) error {
	if record.Phase != PublicChecking || record.CleanupPending || record.PublicStartedAt == nil ||
		record.Public == nil && record.PublicFailure == "" ||
		!recordcodec.IsCanonicalUTC(now) {
		return errs.New(errs.KindStateConflict, "edge diagnostic has no complete result to settle")
	}
	now = record.settlementTime(now)
	record.Outcome, record.Failure = observationOutcome(*record)
	record.Phase, record.FinishedAt = Finished, &now
	return nil
}

// Stop joins native Task terminal handling. It preserves any recorded stages
// and all helper references; timeout is not proof that the helper is absent.
func (record *Record) Stop(failure string, now time.Time) error {
	if record.Phase != Ready && record.Phase != PrivateChecking && record.Phase != PublicChecking &&
		record.Phase != Cleaning ||
		!recordcodec.IsCanonicalUTC(now) ||
		!validFailure(failure) ||
		failure == "" {
		return invalidRecord()
	}
	now = record.settlementTime(now)
	record.Outcome, record.Failure, record.FinishedAt = "interrupted", failure, &now
	record.Phase = Finished
	if record.CleanupPending {
		record.Phase = CleanupRequired
	}
	return record.Validate()
}

func (record *Record) resumeCleanup() error {
	if (record.Phase != PrivateChecking && record.Phase != Cleaning && record.Phase != CleanupRequired) ||
		!record.CleanupPending {
		return errs.New(errs.KindStateConflict, "edge diagnostic has no interrupted helper to clean up")
	}
	record.Phase = Cleaning
	record.Outcome, record.Failure, record.FinishedAt = "", "", nil
	return nil
}

func (record *Record) finishCleanup(now time.Time) error {
	if record.Phase != Cleaning || !record.CleanupPending || !recordcodec.IsCanonicalUTC(now) {
		return errs.New(errs.KindStateConflict, "edge diagnostic cleanup ownership changed")
	}
	now = record.settlementTime(now)
	record.CleanupPending, record.Phase, record.Outcome, record.Failure, record.FinishedAt = false, Finished, "interrupted", "interrupted", &now
	return nil
}
