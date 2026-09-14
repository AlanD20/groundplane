package tasksecretpins

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

const setSchema = 1

func encodeSet(record setRecord) ([]byte, error) {
	if validateSet(record) != nil {
		return nil, validation("secret pin set record is invalid")
	}
	value, err := json.Marshal(record)
	if err != nil {
		return nil, corruption("secret pin set record cannot be encoded")
	}
	if len(value) > 2048 {
		return nil, validation("secret pin set record is too large")
	}
	return value, nil
}

func decodeSet(value []byte) (setRecord, error) {
	if len(value) == 0 || len(value) > 2048 {
		return setRecord{}, corruption("secret pin set record is corrupt")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var record setRecord
	if decoder.Decode(&record) != nil || requireEOF(decoder) != nil || validateSet(record) != nil {
		return setRecord{}, corruption("secret pin set record is corrupt")
	}
	canonical, err := json.Marshal(record)
	if err != nil || !bytes.Equal(canonical, value) {
		return setRecord{}, corruption("secret pin set record is not canonical")
	}
	return record, nil
}

func validateSet(record setRecord) error {
	if record.Schema != setSchema || ids.Validate(ids.KindOperation, record.OperationID) != nil ||
		ids.Validate(ids.KindTask, record.TaskID) != nil || record.MembershipCount == 0 ||
		record.MembershipCount > MaximumPins || !validSHA256(record.MembershipSHA256) ||
		record.PreparationCursor > record.MembershipCount || record.ReleaseCursor > record.PreparationCursor {
		return corruption("secret pin set fields are invalid")
	}
	if record.Phase == phaseActive || record.Phase == phaseReleasing {
		if ids.Validate(ids.KindTask, record.AttemptID) != nil {
			return corruption("Secret pin active attempt identity is invalid")
		}
	} else if record.AttemptID != "" {
		return corruption("unpublished Secret pin set has an active attempt")
	}
	switch record.Phase {
	case phasePreparing:
		if record.ReleaseCursor != 0 {
			return corruption("preparing Secret pin set has a release cursor")
		}
	case phaseSealed, phaseActive:
		if record.PreparationCursor != record.MembershipCount || record.ReleaseCursor != 0 {
			return corruption("sealed Secret pin set cursors are invalid")
		}
	case phaseAbandoning, phaseReleasing:
	default:
		return corruption("secret pin set phase is invalid")
	}
	return nil
}

func validatePrepared(prepared Prepared) error {
	if prepared.revision <= 0 || prepared.record.Phase != phaseSealed ||
		prepared.record.PreparationCursor != prepared.record.MembershipCount {
		return validation("prepared Secret pin set is invalid")
	}
	return validateSet(prepared.record)
}

func validateActive(root ActiveRoot) error {
	if root.revision <= 0 || (root.record.Phase != phaseActive && root.record.Phase != phaseReleasing) {
		return validation("active Secret pin root is invalid")
	}
	return validateSet(root.record)
}

func ordinalKey(ordinal uint64) string {
	return leftPad(strconv.FormatUint(ordinal, 10), 8)
}

func parseOrdinal(key, operationID string) (uint64, error) {
	prefix := ReversePrefix(operationID)
	if len(key) != len(prefix)+8 || !bytes.HasPrefix([]byte(key), []byte(prefix)) {
		return 0, corruption("secret pin reverse membership key is invalid")
	}
	ordinal, err := strconv.ParseUint(key[len(prefix):], 10, 64)
	if err != nil || ordinal == 0 || ordinalKey(ordinal) != key[len(prefix):] {
		return 0, corruption("secret pin reverse membership ordinal is invalid")
	}
	return ordinal, nil
}

func leftPad(value string, width int) string {
	if len(value) >= width {
		return value
	}
	result := make([]byte, width)
	for index := 0; index < width-len(value); index++ {
		result[index] = '0'
	}
	copy(result[width-len(value):], value)
	return string(result)
}

func requireEOF(decoder *json.Decoder) error {
	var trailing struct{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		return corruption("secret pin set record has trailing data")
	}
	return nil
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}
