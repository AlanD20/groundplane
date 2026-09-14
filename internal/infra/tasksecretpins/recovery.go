package tasksecretpins

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// RecoverPreparations runs before admitting new requests or dispatching Tasks.
// Each bounded cleanup still compares the Task and active-operation absences.
func (repository *Repository) RecoverPreparations(ctx context.Context) error {
	if ctx == nil {
		return validation("Secret pin recovery context is missing")
	}
	for {
		page, err := repository.rangeValues(ctx, preparationPrefix, 1)
		if err != nil || len(page.Values) == 0 {
			return err
		}
		value := page.Values[0]
		record, err := decodeSet(value.Value)
		if err != nil || value.Key != PreparationKey(record.OperationID) {
			return corruption("Secret pin preparation index is corrupt")
		}
		if err := repository.Abandon(ctx, record.OperationID); err != nil {
			return err
		}
	}
}

// ResumeRelease drains at most one already-authorized release. It never grants
// release authority; BeginRelease must have joined the owning lifecycle commit.
func (repository *Repository) ResumeRelease(ctx context.Context) (bool, error) {
	if ctx == nil {
		return false, validation("Secret pin release context is missing")
	}
	page, err := repository.rangeValues(ctx, releasingPrefix, 1)
	if err != nil || len(page.Values) == 0 {
		return false, err
	}
	value := page.Values[0]
	operationID := strings.TrimPrefix(value.Key, releasingPrefix)
	if ids.Validate(ids.KindOperation, operationID) != nil || string(value.Value) != operationID ||
		value.Key != releaseKey(operationID) {
		return false, corruption("Secret pin release index is corrupt")
	}
	for {
		progressed, done, err := repository.ReleaseBatch(ctx, operationID)
		if err != nil {
			return false, err
		}
		if done {
			return true, nil
		}
		if !progressed {
			return false, conflict("Secret pin release made no progress")
		}
	}
}
