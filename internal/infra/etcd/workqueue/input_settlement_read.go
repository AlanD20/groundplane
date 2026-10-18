package workqueue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// ReadInputSettlement exposes the saved outcome of preparation, not an inferred
// failure from a missing attempt. The work's result digest authenticates the
// retained reason; callers use the same snapshot as the surrounding Task page.
func (repository *Repository) ReadInputSettlement(ctx context.Context, view View) (*InputSettlement, error) {
	task, item := view.Task.Task, view.Item.Item
	if view.ReadRevision <= 0 || view.Task.Revision <= 0 || view.Item.Revision <= 0 ||
		task.Validate() != nil || item.Validate() != nil || item.TaskID != task.ID {
		return nil, invalid("input settlement read requires current Task and work snapshots")
	}
	if item.Input != nil || item.Phase != workexecution.Settled || item.AttemptNumber != 0 || item.Result == nil {
		return nil, nil
	}
	read, err := repository.read(ctx, []string{inputSettlementKey(item.ID)}, view.ReadRevision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		return nil, corrupt()
	}
	result, err := recordcodec.Decode[InputSettlement](value.Value, "execution_input_settlement")
	if err != nil || result.TaskID != task.ID || result.WorkID != item.ID || result.Outcome != item.Outcome ||
		!result.At.Equal(item.UpdatedAt) ||
		result.Reason == "" || len(result.Reason) > 512 || strings.TrimSpace(result.Reason) != result.Reason ||
		result.DependencyID != "" && !slices.Contains(item.Dependencies, result.DependencyID) ||
		result.Outcome == workexecution.Succeeded && result.Authority == nil ||
		result.Authority != nil && (result.Authority.Validate() != nil || *result.Authority != task.Intent || result.DependencyID != "") {
		return nil, corrupt()
	}
	digest := sha256.Sum256(value.Value)
	expected := workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, "input-settlement"), SHA256: hex.EncodeToString(digest[:])}
	if *item.Result != expected {
		return nil, corrupt()
	}
	return &result, nil
}
