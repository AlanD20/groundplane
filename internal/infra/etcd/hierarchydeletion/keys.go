package hierarchydeletion

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	hierarchyDeletionRuntimeRoot         = "/v1/runtime/"
	hierarchyDeletionActionOrdinalWidth  = 20
	hierarchyDeletionMaximumKeyPartBytes = 512
)

func hierarchyDeletionDynamicSegment(value string) (string, error) {
	if value == "" || !utf8.ValidString(value) || len(value) > hierarchyDeletionMaximumKeyPartBytes {
		return "", errs.New(errs.KindValidationFailed, "hierarchy deletion key segment is invalid")
	}
	return "~" + base64.RawURLEncoding.EncodeToString([]byte(value)), nil
}

func decodeHierarchyDeletionDynamicSegment(value string) (string, error) {
	if !strings.HasPrefix(value, "~") || len(value) == 1 {
		return "", errs.New(errs.KindInternal, "hierarchy deletion key segment is corrupt")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(value, "~"))
	if err != nil || len(decoded) == 0 || !utf8.Valid(decoded) || len(decoded) > hierarchyDeletionMaximumKeyPartBytes {
		return "", errs.New(errs.KindInternal, "hierarchy deletion key segment is corrupt")
	}
	canonical, encodeErr := hierarchyDeletionDynamicSegment(string(decoded))
	if encodeErr != nil || canonical != value {
		return "", errs.New(errs.KindInternal, "hierarchy deletion key segment is not canonical")
	}
	return string(decoded), nil
}

func hierarchyDeletionOrdinalSegment(ordinal int64) (string, error) {
	if ordinal < 0 {
		return "", errs.New(errs.KindValidationFailed, "hierarchy deletion ordinal must not be negative")
	}
	return hierarchyDeletionDynamicSegment(fmt.Sprintf("%020d", ordinal))
}

func decodeHierarchyDeletionOrdinalSegment(value string) (int64, error) {
	decoded, err := decodeHierarchyDeletionDynamicSegment(value)
	if err != nil || len(decoded) != hierarchyDeletionActionOrdinalWidth {
		return 0, errs.New(errs.KindInternal, "hierarchy deletion ordinal segment is corrupt")
	}
	ordinal, parseErr := strconv.ParseInt(decoded, 10, 64)
	if parseErr != nil || ordinal < 0 || fmt.Sprintf("%020d", ordinal) != decoded {
		return 0, errs.New(errs.KindInternal, "hierarchy deletion ordinal segment is corrupt")
	}
	return ordinal, nil
}

func hierarchyDeletionOperationKey(collection string, operationID string) (string, error) {
	segment, err := hierarchyDeletionDynamicSegment(operationID)
	if err != nil {
		return "", err
	}
	return hierarchyDeletionRuntimeRoot + collection + "/" + segment, nil
}

func hierarchyDeletionOrdinalKey(collection string, operationID string, ordinal int64) (string, error) {
	operationSegment, err := hierarchyDeletionDynamicSegment(operationID)
	if err != nil {
		return "", err
	}
	ordinalSegment, err := hierarchyDeletionOrdinalSegment(ordinal)
	if err != nil {
		return "", err
	}
	return hierarchyDeletionRuntimeRoot + collection + "/" + operationSegment + "/" + ordinalSegment, nil
}

func hierarchyDeletionOrdinalPrefix(collection string, operationID string) (string, error) {
	key, err := hierarchyDeletionOperationKey(collection, operationID)
	if err != nil {
		return "", err
	}
	return key + "/", nil
}

func HierarchyDeletionTombstoneKey(targetKind string, targetID string) string {
	return hierarchyDeletionRuntimeRoot + "deletions/" + targetKind + "/" + targetID
}

func HierarchyDeletionLockKey(targetKind string, targetID string) string {
	return hierarchyDeletionRuntimeRoot + "deletion-locks/" + targetKind + "/" + targetID
}

func HierarchyCoordinationKey(targetKind string, targetID string) string {
	return hierarchyDeletionRuntimeRoot + "hierarchy-coordination/" + targetKind + "/" + targetID
}

func HierarchyDeletionReplayTargetKey(operationID string) (string, error) {
	return hierarchyDeletionOperationKey("deletion-replay-targets", operationID)
}

func HierarchyDeletionCleanupFenceKey(operationID string) (string, error) {
	return hierarchyDeletionOperationKey("deletion-cleanup-fences", operationID)
}

func HierarchyDeletionIntentKey(operationID string) (string, error) {
	return hierarchyDeletionOperationKey("deletion-intents", operationID)
}

func HierarchyDeletionActionKey(operationID string, ordinal int64) (string, error) {
	return hierarchyDeletionOrdinalKey("deletion-actions", operationID, ordinal)
}

func HierarchyDeletionActionPrefix(operationID string) (string, error) {
	return hierarchyDeletionOrdinalPrefix("deletion-actions", operationID)
}

func HierarchyDeletionCompletionKey(operationID string, ordinal int64) (string, error) {
	return hierarchyDeletionOrdinalKey("deletion-completions", operationID, ordinal)
}

func HierarchyDeletionCompletionPrefix(operationID string) (string, error) {
	return hierarchyDeletionOrdinalPrefix("deletion-completions", operationID)
}

func HierarchyDeletionChildKey(operationID string, childOperationID string) (string, error) {
	parent, err := hierarchyDeletionDynamicSegment(operationID)
	if err != nil {
		return "", err
	}
	child, err := hierarchyDeletionDynamicSegment(childOperationID)
	if err != nil {
		return "", err
	}
	return hierarchyDeletionRuntimeRoot + "deletion-children/" + parent + "/" + child, nil
}

func HierarchyDeletionChildPrefix(operationID string) (string, error) {
	return hierarchyDeletionOrdinalPrefix("deletion-children", operationID)
}

func HierarchyDeletionReceiptSummaryKey(operationID string) (string, error) {
	return hierarchyDeletionOperationKey("deletion-receipt-summaries", operationID)
}

func HierarchyDeletionCompletionSummaryKey(operationID string) (string, error) {
	return hierarchyDeletionOperationKey("deletion-completion-summaries", operationID)
}

func hierarchyDeletionTaskScopedKey(collection string, operationID string, taskID string) (string, error) {
	operation, err := hierarchyDeletionDynamicSegment(operationID)
	if err != nil {
		return "", err
	}
	return hierarchyDeletionRuntimeRoot + collection + "/" + operation + "/" + taskID, nil
}

func HierarchyDeletionReceiptScanCursorKey(operationID string, taskID string) (string, error) {
	return hierarchyDeletionTaskScopedKey("deletion-receipt-scan-cursors", operationID, taskID)
}

func HierarchyDeletionCompletionScanCursorKey(operationID string, taskID string) (string, error) {
	return hierarchyDeletionTaskScopedKey("deletion-completion-scan-cursors", operationID, taskID)
}

func HierarchyDeletionPruneIntentKey(operationID string) (string, error) {
	return hierarchyDeletionOperationKey("deletion-prune-intents", operationID)
}
