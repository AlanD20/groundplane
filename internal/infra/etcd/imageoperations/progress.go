package imageoperations

import (
	"context"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

func ProgressKey(attemptID string) string { return "/v1/diagnostics/image-progress/" + attemptID }

// Report replaces a bounded diagnostic record instead of rewriting execution
// input or appending every Docker layer update to Task history.
func (journal *Journal) Report(ctx context.Context, progress imagefetch.Progress) error {
	if progress.DownloadedBytes < 0 || progress.TotalBytes < 0 ||
		len(progress.Phase) > 64 || len(progress.ErrorCode) > 128 || len(progress.ErrorDetail) > 2048 ||
		!utf8.ValidString(
			progress.Phase,
		) || !utf8.ValidString(progress.ErrorCode) || !utf8.ValidString(progress.ErrorDetail) {
		return invalid()
	}
	owned, err := journal.read(ctx, false)
	if err != nil {
		return err
	}
	value, err := recordcodec.Encode("image_progress", progress)
	if err != nil {
		return err
	}
	defer clear(value)
	result, err := journal.repository.store.Transact(ctx, owned.conditions,
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: ProgressKey(journal.attemptID), Value: value}})
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return conflict()
	}
	return nil
}
