package etcd

import (
	"context"
	"crypto/sha256"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"
)

func BuildDesiredRevisionStreams(request EnvironmentBlueprintStageRequest) (EnvironmentBlueprintStreams, error) {
	return buildEnvironmentBlueprintStreams(request)
}

func CloneDesiredRevisionClaim(claim EnvironmentBlueprintStageClaim) EnvironmentBlueprintStageClaim {
	return cloneEnvironmentBlueprintStageClaim(claim)
}

func CorruptDesiredRevisionStage() error { return corruptEnvironmentBlueprintStage() }

func DecodeDesiredRevisionKeySegment(segment string) ([]byte, error) {
	return decodeBlueprintDynamicBytes(segment)
}

func DecodeDesiredRevisionChunk(value []byte) (EnvironmentBlueprintChunk, error) {
	return decodeEnvironmentBlueprintChunk(value)
}

func EncodeDesiredRevisionChunk(value EnvironmentBlueprintChunk) ([]byte, error) {
	return encodeEnvironmentBlueprintChunk(value)
}

func DecodeDesiredRevisionSeal(value []byte) (EnvironmentBlueprintSeal, error) {
	return decodeEnvironmentBlueprintSeal(value)
}

func EncodeDesiredRevisionSeal(value EnvironmentBlueprintSeal) ([]byte, error) {
	return encodeEnvironmentBlueprintSeal(value)
}

func DecodeDesiredRevisionDescriptor(value []byte) (EnvironmentBlueprintStageDescriptor, error) {
	return decodeEnvironmentBlueprintStageDescriptor(value)
}

func EncodeDesiredRevisionDescriptor(value EnvironmentBlueprintStageDescriptor) ([]byte, error) {
	return encodeEnvironmentBlueprintStageDescriptor(value)
}

func DecodeDesiredRevisionLocator(value []byte) (string, [sha256.Size]byte, error) {
	return decodeEnvironmentBlueprintStageLocator(value)
}

func EncodeDesiredRevisionLocator(descriptorID string, digest [sha256.Size]byte) ([]byte, error) {
	return encodeEnvironmentBlueprintStageLocator(descriptorID, digest)
}

func DecodeDesiredRevisionProjection(value []byte) (EnvironmentComposeProjection, error) {
	return decodeEnvironmentComposeProjection(value)
}

func DesiredRevisionChunkKey(environmentID, revisionID string, family uint8, index uint32) string {
	return environmentBlueprintChunkKeyFor(environmentID, revisionID, family, index)
}

func DesiredRevisionDescriptorKey(descriptorID string) string {
	return environmentBlueprintDescriptorKeyByID(descriptorID)
}

func DesiredRevisionLocatorKey(locator idempotencyrecord.IdempotencyLocator) (string, [sha256.Size]byte, error) {
	return environmentBlueprintLocatorKey(locator)
}

func DesiredRevisionPrefix(environmentID, revisionID string) string {
	return environmentBlueprintRevisionPrefixFinal(environmentID, revisionID)
}

func DesiredRevisionRootKey(environmentID, revisionID string) string {
	return environmentBlueprintRootKey(environmentID, revisionID)
}

func SameDesiredRevisionClaim(left, right EnvironmentBlueprintStageClaim) bool {
	return sameEnvironmentBlueprintStageClaim(left, right)
}

func ValidDesiredRevisionTime(value time.Time) bool { return validBlueprintRecordTime(value) }

func ValidateDesiredRevisionClaim(claim EnvironmentBlueprintStageClaim) error {
	return validateEnvironmentBlueprintStageClaim(claim)
}

func ValidateDesiredRevisionTransaction(
	store interface {
		Get(context.Context, string) (*etcdstore.GetResult, error)
		GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
		Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
		MeasureTransaction(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionBudget, error)
		Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
	},
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	maximumOperations int,
	maximumBytes int,
) error {
	return validateBlueprintTransaction(store, conditions, mutations, maximumOperations, maximumBytes)
}

func NextDesiredRevisionProgressTime(previous time.Time) time.Time {
	return nextBlueprintProgressTime(previous)
}

func ProtectedDesiredRevisionIntentDigest(intent idempotencyrecord.ProtectedIntentRecord) ([sha256.Size]byte, error) {
	return protectedBlueprintIntentDigest(intent)
}

func DesiredRevisionSealFromDescriptor(descriptor EnvironmentBlueprintStageDescriptor) EnvironmentBlueprintSeal {
	return environmentBlueprintSealFromDescriptor(descriptor)
}

func DesiredRevisionChunkKeys(descriptor EnvironmentBlueprintStageDescriptor) []string {
	return environmentBlueprintChunkKeys(descriptor)
}

func VerifyDesiredRevisionChunks(descriptor EnvironmentBlueprintStageDescriptor, values []*etcdstore.KeyValue) error {
	return verifyEnvironmentBlueprintChunks(descriptor, values)
}

func MatchingDesiredRevisionChunk(
	chunk EnvironmentBlueprintChunk,
	family uint8,
	sequence uint32,
	data []byte,
) bool {
	return matchingEnvironmentBlueprintChunk(chunk, family, sequence, data)
}

func SameDesiredRevisionStreams(left, right EnvironmentBlueprintStageDescriptor) bool {
	return sameEnvironmentBlueprintStageStreams(left, right)
}
