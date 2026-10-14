package taskjournal

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// AttemptArchiveCursor is small mutable metadata. Summaries are stored
// individually, so observing one attempt never rewrites every other attempt.
type AttemptArchiveCursor struct {
	TaskID           string `json:"task_id"`
	ThroughSequence  uint64 `json:"through_sequence"`
	DroppedSummaries uint64 `json:"dropped_summaries"`
	SummaryCount     int    `json:"summary_count"`
}

func TaskAttemptSummaryPrefix(taskID string) string {
	return "/v1/runtime/task-attempt-summaries/" + taskID + "/"
}

func TaskAttemptSummaryKey(taskID string, sequence uint64) string {
	return TaskAttemptSummaryPrefix(taskID) + fmt.Sprintf("%020d", sequence)
}

func EncodeAttemptArchiveCursor(cursor AttemptArchiveCursor) ([]byte, error) {
	if validateAttemptArchiveCursor(cursor) != nil {
		return nil, recordcodec.CorruptRecord()
	}
	return recordcodec.Encode("task-attempt-history-cursor", cursor)
}

func DecodeAttemptArchiveCursor(value []byte) (AttemptArchiveCursor, error) {
	if len(value) > 1024 {
		return AttemptArchiveCursor{}, recordcodec.CorruptRecord()
	}
	cursor, err := recordcodec.Decode[AttemptArchiveCursor](value, "task-attempt-history-cursor")
	if err != nil || validateAttemptArchiveCursor(cursor) != nil {
		return AttemptArchiveCursor{}, recordcodec.CorruptRecord()
	}
	return cursor, nil
}

func validateAttemptArchiveCursor(cursor AttemptArchiveCursor) error {
	if ids.Validate(ids.KindTask, cursor.TaskID) != nil || cursor.ThroughSequence == 0 ||
		cursor.DroppedSummaries > cursor.ThroughSequence || cursor.SummaryCount < 1 ||
		cursor.SummaryCount > MaximumTaskEvents {
		return recordcodec.CorruptRecord()
	}
	return nil
}

func EncodeAttemptSummary(summary AttemptSummary) ([]byte, error) {
	if ValidateAttemptHistory(AttemptHistory{TaskID: summary.Identity.TaskID,
		ThroughSequence: summary.LastSequence, Summaries: []AttemptSummary{summary}}) != nil {
		return nil, recordcodec.CorruptRecord()
	}
	value, err := recordcodec.Encode("task-attempt-summary", summary)
	if err != nil {
		return nil, err
	}
	defer clear(value)
	if len(value) > MaximumAttemptHistoryBytes {
		return nil, recordcodec.CorruptRecord()
	}
	var output bytes.Buffer
	writer := gzip.NewWriter(&output)
	if _, err := writer.Write(value); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func DecodeAttemptSummary(value []byte) (AttemptSummary, error) {
	if len(value) == 0 || len(value) > MaximumAttemptHistoryBytes {
		return AttemptSummary{}, recordcodec.CorruptRecord()
	}
	source := bytes.NewReader(value)
	reader, err := gzip.NewReader(source)
	if err != nil {
		return AttemptSummary{}, recordcodec.CorruptRecord()
	}
	reader.Multistream(false)
	decoded, readErr := io.ReadAll(io.LimitReader(reader, MaximumAttemptHistoryBytes+1))
	closeErr := reader.Close()
	defer clear(decoded)
	if readErr != nil || closeErr != nil || source.Len() != 0 || len(decoded) > MaximumAttemptHistoryBytes {
		return AttemptSummary{}, recordcodec.CorruptRecord()
	}
	summary, err := recordcodec.Decode[AttemptSummary](decoded, "task-attempt-summary")
	if err != nil || ValidateAttemptHistory(AttemptHistory{TaskID: summary.Identity.TaskID,
		ThroughSequence: summary.LastSequence, Summaries: []AttemptSummary{summary}}) != nil {
		return AttemptSummary{}, recordcodec.CorruptRecord()
	}
	return summary, nil
}
