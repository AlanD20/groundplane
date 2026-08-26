package volumeremoval

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func environmentVolumeRemovalPathRequestDigest(record EnvironmentVolumeRemovalPendingPath) [sha256.Size]byte {
	writer := volumeRemovalWriter{}
	writer.string(record.OperationID)
	writer.string(record.VolumeID)
	writer.string(record.Key)
	writer.digest(record.IntentSHA256)
	writer.uint64(record.RequestOrdinal)
	writer.uint32(record.MutationBudget)
	writer.strings(record.ComponentStack)
	writer.bytes(record.Cursor)
	return sha256.Sum256(writer.value)
}

func environmentVolumeRemovalPathResponseDigest(record EnvironmentVolumeRemovalPathCompletion) [sha256.Size]byte {
	return sha256.Sum256(environmentVolumeRemovalPathResponseEncoding(record))
}

func environmentVolumeRemovalPathResponseBytes(record EnvironmentVolumeRemovalPathCompletion) uint32 {
	return uint32(len(environmentVolumeRemovalPathResponseEncoding(record)))
}

func environmentVolumeRemovalPathResponseEncoding(record EnvironmentVolumeRemovalPathCompletion) []byte {
	writer := volumeRemovalWriter{}
	writer.string(record.OperationID)
	writer.uint64(record.RequestOrdinal)
	writer.digest(record.RequestSHA256)
	writer.uint32(record.MutationCount)
	writer.boolean(record.DirectoryAbsent)
	writer.strings(record.NextComponentStack)
	writer.bytes(record.NextCursor)
	return writer.value
}

type volumeRemovalWriter struct {
	value []byte
	err   error
}

func (writer *volumeRemovalWriter) uint8(value uint8) { writer.value = append(writer.value, value) }
func (writer *volumeRemovalWriter) uint16(value uint16) {
	encoded := make([]byte, 2)
	binary.BigEndian.PutUint16(encoded, value)
	writer.value = append(writer.value, encoded...)
}
func (writer *volumeRemovalWriter) uint32(value uint32) {
	encoded := make([]byte, 4)
	binary.BigEndian.PutUint32(encoded, value)
	writer.value = append(writer.value, encoded...)
}
func (writer *volumeRemovalWriter) uint64(value uint64) {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	writer.value = append(writer.value, encoded...)
}
func (writer *volumeRemovalWriter) boolean(value bool) {
	if value {
		writer.uint8(1)
	} else {
		writer.uint8(0)
	}
}
func (writer *volumeRemovalWriter) digest(value [sha256.Size]byte) {
	writer.value = append(writer.value, value[:]...)
}
func (writer *volumeRemovalWriter) bytes(value []byte) {
	if writer.err != nil {
		return
	}
	if len(value) > math.MaxUint32 {
		writer.err = errs.New(errs.KindValidationFailed, "Environment Volume removal field is too large")
		return
	}
	writer.uint32(uint32(len(value)))
	writer.value = append(writer.value, value...)
}
func (writer *volumeRemovalWriter) string(value string) {
	if !utf8.ValidString(value) {
		writer.err = errs.New(errs.KindValidationFailed, "Environment Volume removal string is not UTF-8")
		return
	}
	writer.bytes([]byte(value))
}
func (writer *volumeRemovalWriter) strings(values []string) {
	if len(values) > math.MaxUint16 {
		writer.err = errs.New(errs.KindValidationFailed, "Environment Volume removal component count is too large")
		return
	}
	writer.uint16(uint16(len(values)))
	for _, value := range values {
		writer.string(value)
	}
}
func (writer *volumeRemovalWriter) timestamp(value time.Time) {
	if etcd.ValidateCapabilityTimestamp("Environment Volume removal timestamp", value) != nil || value.UnixNano() < 0 {
		writer.err = errs.New(errs.KindValidationFailed, "Environment Volume removal timestamp is invalid")
		return
	}
	writer.uint64(uint64(value.UnixNano()))
}

type volumeRemovalReader struct {
	value  []byte
	offset int
	err    error
}

func (reader *volumeRemovalReader) take(length int) []byte {
	if reader.err != nil || length < 0 || reader.offset > len(reader.value)-length {
		reader.err = corruptEnvironmentVolumeRemovalRuntime()
		return nil
	}
	value := reader.value[reader.offset : reader.offset+length]
	reader.offset += length
	return value
}
func (reader *volumeRemovalReader) uint8() uint8 {
	value := reader.take(1)
	if len(value) == 0 {
		return 0
	}
	return value[0]
}
func (reader *volumeRemovalReader) uint16() uint16 {
	value := reader.take(2)
	if len(value) != 2 {
		return 0
	}
	return binary.BigEndian.Uint16(value)
}
func (reader *volumeRemovalReader) uint32() uint32 {
	value := reader.take(4)
	if len(value) != 4 {
		return 0
	}
	return binary.BigEndian.Uint32(value)
}
func (reader *volumeRemovalReader) uint64() uint64 {
	value := reader.take(8)
	if len(value) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(value)
}
func (reader *volumeRemovalReader) boolean() bool {
	value := reader.uint8()
	if value > 1 {
		reader.err = corruptEnvironmentVolumeRemovalRuntime()
	}
	return value == 1
}
func (reader *volumeRemovalReader) digest() [sha256.Size]byte {
	var value [sha256.Size]byte
	copy(value[:], reader.take(sha256.Size))
	return value
}
func (reader *volumeRemovalReader) bytes(maximum int) []byte {
	length := reader.uint32()
	if reader.err != nil || uint64(length) > uint64(maximum) {
		reader.err = corruptEnvironmentVolumeRemovalRuntime()
		return nil
	}
	return append([]byte(nil), reader.take(int(length))...)
}
func (reader *volumeRemovalReader) string(maximum int) string {
	value := reader.bytes(maximum)
	if reader.err != nil || !utf8.Valid(value) {
		reader.err = corruptEnvironmentVolumeRemovalRuntime()
		return ""
	}
	return string(value)
}
func (reader *volumeRemovalReader) strings(maximumCount, maximumBytes int) []string {
	count := int(reader.uint16())
	if reader.err != nil || count > maximumCount {
		reader.err = corruptEnvironmentVolumeRemovalRuntime()
		return nil
	}
	values := make([]string, count)
	for index := range values {
		values[index] = reader.string(maximumBytes)
	}
	return values
}
func (reader *volumeRemovalReader) timestamp() time.Time {
	value := reader.uint64()
	if value > math.MaxInt64 {
		reader.err = corruptEnvironmentVolumeRemovalRuntime()
		return time.Time{}
	}
	return time.Unix(0, int64(value)).UTC()
}
func (reader *volumeRemovalReader) done() error {
	if reader.err != nil || reader.offset != len(reader.value) {
		return corruptEnvironmentVolumeRemovalRuntime()
	}
	return nil
}

func encodeEnvironmentVolumeRemovalRuntime(record EnvironmentVolumeRemovalRuntimeRecord) ([]byte, error) {
	if err := validateEnvironmentVolumeRemovalRuntime(record); err != nil {
		return nil, err
	}
	writer := volumeRemovalWriter{value: []byte("GVRR")}
	writer.uint16(1)
	writer.string(record.OperationID)
	writer.string(record.EnvironmentID)
	writer.string(record.VolumeID)
	writer.string(record.Key)
	writer.string(record.DesiredRevisionID)
	writer.uint64(record.DesiredGeneration)
	writer.digest(record.ImpactSHA256)
	writer.digest(record.EvidenceManifestSHA256)
	writer.digest(record.IntentSHA256)
	encodeVolumeRemovalLocator(&writer, record.RootLocator)
	writer.digest(record.RootResponseSHA256)
	writer.string(record.OriginTaskID)
	writer.string(record.CurrentTaskID)
	writer.string(record.PredecessorTaskID)
	writer.uint32(record.AttemptOrdinal)
	writer.string(record.StepID)
	writer.uint8(uint8(record.Checkpoint))
	writer.timestamp(record.CreatedAt)
	writer.timestamp(record.UpdatedAt)
	return finishVolumeRemovalEncoding(writer)
}

func decodeEnvironmentVolumeRemovalRuntime(value []byte) (EnvironmentVolumeRemovalRuntimeRecord, error) {
	reader, err := newVolumeRemovalReader(value, "GVRR")
	if err != nil {
		return EnvironmentVolumeRemovalRuntimeRecord{}, err
	}
	record := EnvironmentVolumeRemovalRuntimeRecord{
		OperationID: reader.string(128), EnvironmentID: reader.string(128), VolumeID: reader.string(128), Key: reader.string(255),
		DesiredRevisionID: reader.string(128), DesiredGeneration: reader.uint64(),
		ImpactSHA256: reader.digest(), EvidenceManifestSHA256: reader.digest(), IntentSHA256: reader.digest(),
		RootLocator: decodeVolumeRemovalLocator(&reader), RootResponseSHA256: reader.digest(),
		OriginTaskID: reader.string(128), CurrentTaskID: reader.string(128), PredecessorTaskID: reader.string(128),
		AttemptOrdinal: reader.uint32(), StepID: reader.string(128), Checkpoint: EnvironmentVolumeRemovalCheckpoint(reader.uint8()),
		CreatedAt: reader.timestamp(), UpdatedAt: reader.timestamp(),
	}
	if reader.done() != nil || validateEnvironmentVolumeRemovalRuntime(record) != nil {
		return EnvironmentVolumeRemovalRuntimeRecord{}, corruptEnvironmentVolumeRemovalRuntime()
	}
	return record, nil
}

func encodeEnvironmentVolumeRemovalAttempt(record EnvironmentVolumeRemovalAttemptRecord) ([]byte, error) {
	if err := validateEnvironmentVolumeRemovalAttempt(record); err != nil {
		return nil, err
	}
	writer := volumeRemovalWriter{value: []byte("GVRA")}
	writer.uint16(1)
	writer.string(record.OperationID)
	writer.string(record.OriginTaskID)
	writer.string(record.TaskID)
	writer.string(record.PredecessorTaskID)
	writer.uint32(record.Ordinal)
	writer.timestamp(record.CreatedAt)
	return finishVolumeRemovalEncoding(writer)
}

func decodeEnvironmentVolumeRemovalAttempt(value []byte) (EnvironmentVolumeRemovalAttemptRecord, error) {
	reader, err := newVolumeRemovalReader(value, "GVRA")
	if err != nil {
		return EnvironmentVolumeRemovalAttemptRecord{}, err
	}
	record := EnvironmentVolumeRemovalAttemptRecord{OperationID: reader.string(128), OriginTaskID: reader.string(128), TaskID: reader.string(128), PredecessorTaskID: reader.string(128), Ordinal: reader.uint32(), CreatedAt: reader.timestamp()}
	if reader.done() != nil || validateEnvironmentVolumeRemovalAttempt(record) != nil {
		return EnvironmentVolumeRemovalAttemptRecord{}, corruptEnvironmentVolumeRemovalRuntime()
	}
	return record, nil
}

func encodeEnvironmentVolumeRemovalProgress(record EnvironmentVolumeRemovalPathProgress) ([]byte, error) {
	if err := validateEnvironmentVolumeRemovalProgress(record); err != nil {
		return nil, err
	}
	writer := volumeRemovalWriter{value: []byte("GVRP")}
	writer.uint16(1)
	writer.string(record.OperationID)
	writer.uint64(record.NextRequestOrdinal)
	writer.strings(record.ComponentStack)
	writer.bytes(record.Cursor)
	writer.boolean(record.DirectoryAbsent)
	writer.timestamp(record.UpdatedAt)
	return finishVolumeRemovalEncoding(writer)
}

func decodeEnvironmentVolumeRemovalProgress(value []byte) (EnvironmentVolumeRemovalPathProgress, error) {
	reader, err := newVolumeRemovalReader(value, "GVRP")
	if err != nil {
		return EnvironmentVolumeRemovalPathProgress{}, err
	}
	record := EnvironmentVolumeRemovalPathProgress{OperationID: reader.string(128), NextRequestOrdinal: reader.uint64(), ComponentStack: reader.strings(128, 255), Cursor: reader.bytes(environmentVolumeRemovalCursorBytes), DirectoryAbsent: reader.boolean(), UpdatedAt: reader.timestamp()}
	if reader.done() != nil || validateEnvironmentVolumeRemovalProgress(record) != nil {
		return EnvironmentVolumeRemovalPathProgress{}, corruptEnvironmentVolumeRemovalRuntime()
	}
	return record, nil
}

func encodeEnvironmentVolumeRemovalPendingPath(record EnvironmentVolumeRemovalPendingPath) ([]byte, error) {
	if err := validateEnvironmentVolumeRemovalPendingPath(record); err != nil {
		return nil, err
	}
	writer := volumeRemovalWriter{value: []byte("GVRQ")}
	writer.uint16(1)
	writer.string(record.OperationID)
	writer.string(record.VolumeID)
	writer.string(record.Key)
	writer.digest(record.IntentSHA256)
	writer.uint64(record.RequestOrdinal)
	writer.uint32(record.MutationBudget)
	writer.strings(record.ComponentStack)
	writer.bytes(record.Cursor)
	writer.digest(record.RequestSHA256)
	writer.string(record.TaskID)
	writer.string(record.AssignmentID)
	writer.string(record.AgentID)
	writer.uint64(record.AgentGeneration)
	writer.timestamp(record.CreatedAt)
	return finishVolumeRemovalEncoding(writer)
}

func decodeEnvironmentVolumeRemovalPendingPath(value []byte) (EnvironmentVolumeRemovalPendingPath, error) {
	reader, err := newVolumeRemovalReader(value, "GVRQ")
	if err != nil {
		return EnvironmentVolumeRemovalPendingPath{}, err
	}
	record := EnvironmentVolumeRemovalPendingPath{OperationID: reader.string(128), VolumeID: reader.string(128), Key: reader.string(255), IntentSHA256: reader.digest(), RequestOrdinal: reader.uint64(), MutationBudget: reader.uint32(), ComponentStack: reader.strings(128, 255), Cursor: reader.bytes(environmentVolumeRemovalCursorBytes), RequestSHA256: reader.digest(), TaskID: reader.string(128), AssignmentID: reader.string(128), AgentID: reader.string(128), AgentGeneration: reader.uint64(), CreatedAt: reader.timestamp()}
	if reader.done() != nil || validateEnvironmentVolumeRemovalPendingPath(record) != nil {
		return EnvironmentVolumeRemovalPendingPath{}, corruptEnvironmentVolumeRemovalRuntime()
	}
	return record, nil
}

func encodeEnvironmentVolumeRemovalCompletion(record EnvironmentVolumeRemovalPathCompletion) ([]byte, error) {
	if err := validateEnvironmentVolumeRemovalCompletion(record); err != nil {
		return nil, err
	}
	writer := volumeRemovalWriter{value: []byte("GVRC")}
	writer.uint16(1)
	writer.string(record.OperationID)
	writer.uint64(record.RequestOrdinal)
	writer.digest(record.RequestSHA256)
	writer.digest(record.ResponseSHA256)
	writer.uint32(record.ResponseBytes)
	writer.uint32(record.MutationCount)
	writer.strings(record.NextComponentStack)
	writer.bytes(record.NextCursor)
	writer.boolean(record.DirectoryAbsent)
	writer.timestamp(record.CompletedAt)
	return finishVolumeRemovalEncoding(writer)
}

func decodeEnvironmentVolumeRemovalCompletion(value []byte) (EnvironmentVolumeRemovalPathCompletion, error) {
	reader, err := newVolumeRemovalReader(value, "GVRC")
	if err != nil {
		return EnvironmentVolumeRemovalPathCompletion{}, err
	}
	record := EnvironmentVolumeRemovalPathCompletion{OperationID: reader.string(128), RequestOrdinal: reader.uint64(), RequestSHA256: reader.digest(), ResponseSHA256: reader.digest(), ResponseBytes: reader.uint32(), MutationCount: reader.uint32(), NextComponentStack: reader.strings(128, 255), NextCursor: reader.bytes(environmentVolumeRemovalCursorBytes), DirectoryAbsent: reader.boolean(), CompletedAt: reader.timestamp()}
	if reader.done() != nil || validateEnvironmentVolumeRemovalCompletion(record) != nil {
		return EnvironmentVolumeRemovalPathCompletion{}, corruptEnvironmentVolumeRemovalRuntime()
	}
	return record, nil
}

func encodeVolumeRemovalLocator(writer *volumeRemovalWriter, locator etcd.IdempotencyLocator) {
	writer.string(string(locator.ScopeKind))
	writer.string(locator.ScopeID)
	writer.string(locator.Method)
	writer.string(locator.Route)
	writer.string(locator.Key)
}

func decodeVolumeRemovalLocator(reader *volumeRemovalReader) etcd.IdempotencyLocator {
	return etcd.IdempotencyLocator{ScopeKind: etcd.IdempotencyScopeKind(reader.string(32)), ScopeID: reader.string(128), Method: reader.string(16), Route: reader.string(1024), Key: reader.string(1024)}
}

func newVolumeRemovalReader(value []byte, magic string) (volumeRemovalReader, error) {
	if len(value) < 6 || string(value[:4]) != magic {
		return volumeRemovalReader{}, corruptEnvironmentVolumeRemovalRuntime()
	}
	reader := volumeRemovalReader{value: value[4:]}
	if reader.uint16() != 1 {
		return volumeRemovalReader{}, corruptEnvironmentVolumeRemovalRuntime()
	}
	return reader, nil
}

func finishVolumeRemovalEncoding(writer volumeRemovalWriter) ([]byte, error) {
	if writer.err != nil {
		clear(writer.value)
		return nil, writer.err
	}
	if len(writer.value) > 64*1024 {
		clear(writer.value)
		return nil, errs.New(errs.KindValidationFailed, "Environment Volume removal record exceeds 64 KiB")
	}
	return writer.value, nil
}

func corruptEnvironmentVolumeRemovalRuntime() error {
	return errs.New(errs.KindInternal, "Environment Volume removal runtime is corrupt")
}
