package etcd

import (
	"context"
	"crypto/sha256"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"
)

func BuildDesiredRevisionStreams(request blueprints.EnvironmentBlueprintStageRequest) (blueprints.EnvironmentBlueprintStreams, error) {
	return blueprints.BuildEnvironmentBlueprintStreams(request)
}

func CloneDesiredRevisionClaim(claim blueprints.EnvironmentBlueprintStageClaim) blueprints.EnvironmentBlueprintStageClaim {
	return blueprints.CloneEnvironmentBlueprintStageClaim(claim)
}

func CorruptDesiredRevisionStage() error { return blueprints.CorruptEnvironmentBlueprintStage() }

func DecodeDesiredRevisionKeySegment(segment string) ([]byte, error) {
	return blueprints.DecodeBlueprintDynamicBytes(segment)
}

func DecodeDesiredRevisionChunk(value []byte) (blueprints.EnvironmentBlueprintChunk, error) {
	return blueprints.DecodeEnvironmentBlueprintChunk(value)
}

func EncodeDesiredRevisionChunk(value blueprints.EnvironmentBlueprintChunk) ([]byte, error) {
	return blueprints.EncodeEnvironmentBlueprintChunk(value)
}

func DecodeDesiredRevisionSeal(value []byte) (blueprints.EnvironmentBlueprintSeal, error) {
	return blueprints.DecodeEnvironmentBlueprintSeal(value)
}

func EncodeDesiredRevisionSeal(value blueprints.EnvironmentBlueprintSeal) ([]byte, error) {
	return blueprints.EncodeEnvironmentBlueprintSeal(value)
}

func DecodeDesiredRevisionDescriptor(value []byte) (blueprints.EnvironmentBlueprintStageDescriptor, error) {
	return blueprints.DecodeEnvironmentBlueprintStageDescriptor(value)
}

func EncodeDesiredRevisionDescriptor(value blueprints.EnvironmentBlueprintStageDescriptor) ([]byte, error) {
	return blueprints.EncodeEnvironmentBlueprintStageDescriptor(value)
}

func DecodeDesiredRevisionLocator(value []byte) (string, [sha256.Size]byte, error) {
	return blueprints.DecodeEnvironmentBlueprintStageLocator(value)
}

func EncodeDesiredRevisionLocator(descriptorID string, digest [sha256.Size]byte) ([]byte, error) {
	return blueprints.EncodeEnvironmentBlueprintStageLocator(descriptorID, digest)
}

func DecodeDesiredRevisionProjection(value []byte) (projectionrecord.EnvironmentComposeProjection, error) {
	return projectionrecord.DecodeEnvironmentComposeProjectionStorage(value)
}

func DesiredRevisionChunkKey(environmentID, revisionID string, family uint8, index uint32) string {
	return blueprints.EnvironmentBlueprintChunkKeyFor(environmentID, revisionID, family, index)
}

func DesiredRevisionDescriptorKey(descriptorID string) string {
	return blueprints.EnvironmentBlueprintDescriptorKeyByID(descriptorID)
}

func DesiredRevisionLocatorKey(locator idempotencyrecord.IdempotencyLocator) (string, [sha256.Size]byte, error) {
	return blueprints.EnvironmentBlueprintLocatorKey(locator)
}

func DesiredRevisionPrefix(environmentID, revisionID string) string {
	return blueprints.EnvironmentBlueprintRevisionPrefixFinal(environmentID, revisionID)
}

func DesiredRevisionRootKey(environmentID, revisionID string) string {
	return blueprints.EnvironmentBlueprintRootKey(environmentID, revisionID)
}

func SameDesiredRevisionClaim(left, right blueprints.EnvironmentBlueprintStageClaim) bool {
	return blueprints.SameEnvironmentBlueprintStageClaim(left, right)
}

func ValidDesiredRevisionTime(value time.Time) bool {
	return blueprints.ValidBlueprintRecordTime(value)
}

func ValidateDesiredRevisionClaim(claim blueprints.EnvironmentBlueprintStageClaim) error {
	return blueprints.ValidateEnvironmentBlueprintStageClaim(claim)
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

func DesiredRevisionSealFromDescriptor(descriptor blueprints.EnvironmentBlueprintStageDescriptor) blueprints.EnvironmentBlueprintSeal {
	return environmentBlueprintSealFromDescriptor(descriptor)
}

func DesiredRevisionChunkKeys(descriptor blueprints.EnvironmentBlueprintStageDescriptor) []string {
	return environmentBlueprintChunkKeys(descriptor)
}

func VerifyDesiredRevisionChunks(descriptor blueprints.EnvironmentBlueprintStageDescriptor, values []*etcdstore.KeyValue) error {
	return verifyEnvironmentBlueprintChunks(descriptor, values)
}

func MatchingDesiredRevisionChunk(
	chunk blueprints.EnvironmentBlueprintChunk,
	family uint8,
	sequence uint32,
	data []byte,
) bool {
	return matchingEnvironmentBlueprintChunk(chunk, family, sequence, data)
}

func SameDesiredRevisionStreams(left, right blueprints.EnvironmentBlueprintStageDescriptor) bool {
	return sameEnvironmentBlueprintStageStreams(left, right)
}
