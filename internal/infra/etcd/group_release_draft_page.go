package etcd

import (
	"context"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CollectAbandonedGroupReleaseDraftPage scans at most eight stored leaves. It
// selects only headers and delegates deletion to the exact absent-Task/root CAS;
// encountering an accepted capture never authorizes its collection.
func (ledger *ReleaseLedger) CollectAbandonedGroupReleaseDraftPage(ctx context.Context, after string, at time.Time) (string, error) {
	if after != "" && !strings.HasPrefix(after, groupReleaseDraftPrefix) {
		return "", errs.New(errs.KindValidationFailed, "Group draft collection cursor is outside its namespace")
	}
	page, err := ledger.store.Range(ctx, keyvalue.RangeRequest{Prefix: groupReleaseDraftPrefix, StartExclusive: after, Limit: 8})
	if err != nil {
		return after, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) > 8 || page.More && len(page.Values) == 0 {
		return after, errs.New(errs.KindStateConflict, "Group draft collection page is incomplete")
	}
	for _, value := range page.Values {
		defer clear(value.Value)
		if !strings.HasPrefix(value.Key, groupReleaseDraftPrefix) || value.Key <= after {
			return after, errs.New(errs.KindStateConflict, "Group draft collection left its ordered namespace")
		}
		after = value.Key
		parts := strings.Split(strings.TrimPrefix(value.Key, groupReleaseDraftPrefix), "/")
		if len(parts) != 2 || parts[1] != "header" {
			continue
		}
		if ids.Validate(ids.KindTask, parts[0]) != nil {
			return after, errs.New(errs.KindStateConflict, "Group draft header has invalid Task identity")
		}
		if _, err := ledger.CollectAbandonedGroupReleaseDraft(ctx, parts[0], at); err != nil {
			return after, err
		}
	}
	if !page.More {
		return "", nil
	}
	return after, nil
}
