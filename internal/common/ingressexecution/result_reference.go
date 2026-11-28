package ingressexecution

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResultReference binds acknowledgement and replay to the exact reported
// runtime evidence on this attempt.
func ResultReference(attempt workexecution.Attempt, report Report) (workexecution.Input, error) {
	if err := attempt.Validate(); err != nil {
		return workexecution.Input{}, err
	}
	if err := report.Validate(); err != nil {
		return workexecution.Input{}, err
	}
	raw, err := json.Marshal(report)
	if err != nil {
		return workexecution.Input{}, errs.Wrap(errs.KindInternal, err)
	}
	sum := sha256.Sum256(raw)
	return workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, attempt.AssignedAt, attempt.ID, "public-ingress-result"),
		SHA256: hex.EncodeToString(sum[:]),
	}, nil
}
