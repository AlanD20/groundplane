package attachments

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
)

// ListAttachesByBackingProject pages the existing ownership index at one MVCC
// revision. It does not scan unrelated consumer Environments or expose facts.
func (repository *Reader) ListAttachesByBackingProject(ctx context.Context, projectID string,
	request etcdstore.PageRequest) (etcdstore.Page[Record], error) {
	if err := recordcodec.ValidateID(ids.KindProject, projectID); err != nil {
		return etcdstore.Page[Record]{}, err
	}
	return recordquery.ListIndex(ctx, repository.store, "attaches", "backing-project", projectID,
		AttachBackingProjectPrefix(projectID), AttachKey, ids.KindAttach, request,
		DecodeAttachRecord, func(record Record) string { return record.ID },
		func(record Record) bool { return record.BackingProjectID == projectID })
}
