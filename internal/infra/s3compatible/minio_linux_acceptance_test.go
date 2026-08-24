//go:build linux && s3compatible_minio_acceptance

package s3compatible

import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const minioAcceptanceTimeout = 15 * time.Minute

// Rationale: release acceptance must prove the provider-neutral immutable
// Backup object contract against a real Linux MinIO endpoint without making
// live credentials or infrastructure a default unit-test dependency.
func TestMinIOLiveBackupObjectLifecycle(t *testing.T) {
	config := minioAcceptanceConfig(t)
	instant := time.Now().UTC()
	config.Prefix = fmt.Sprintf(
		"groundplane-acceptance/%s-%d/",
		instant.Format("20060102T150405.000000000"),
		os.Getpid(),
	)
	adapter, err := New(config)
	if err != nil {
		t.Fatalf("construct MinIO adapter: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), minioAcceptanceTimeout)
	defer cancel()

	t.Run("single_put_head_get_delete", func(t *testing.T) {
		body := []byte("groundplane MinIO single-object acceptance payload")
		digest := sha256.Sum256(body)
		artifact := minioAcceptanceArtifact(
			config.Prefix,
			ids.NewAt(ids.KindEnvironment, instant, 1),
			ids.NewAt(ids.KindBackupSource, instant, 2),
			ids.NewAt(ids.KindRecoveryPoint, instant, 3),
			uint64(len(body)),
			digest,
		)
		exerciseMinioLifecycle(t, ctx, adapter, artifact, newAcceptanceBytesReader(body))
	})

	t.Run("multipart_put_head_get_delete", func(t *testing.T) {
		size := singlePutMaximum + 1
		source := acceptancePatternReaderAt{size: size}
		digest := acceptanceReaderDigest(t, source, size)
		artifact := minioAcceptanceArtifact(
			config.Prefix,
			ids.NewAt(ids.KindEnvironment, instant, 4),
			ids.NewAt(ids.KindBackupSource, instant, 5),
			ids.NewAt(ids.KindRecoveryPoint, instant, 6),
			size,
			digest,
		)
		exerciseMinioLifecycle(t, ctx, adapter, artifact, source)
	})

	t.Run("exact_key_multipart_cleanup", func(t *testing.T) {
		digest := sha256.Sum256(nil)
		artifact := minioAcceptanceArtifact(
			config.Prefix,
			ids.NewAt(ids.KindEnvironment, instant, 7),
			ids.NewAt(ids.KindBackupSource, instant, 8),
			ids.NewAt(ids.KindRecoveryPoint, instant, 9),
			0,
			digest,
		)
		exactUploadID := startMinioAcceptanceUpload(t, ctx, adapter, artifact.Key, artifact.Metadata())
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			if err := adapter.abortOne(cleanupCtx, artifact.Key, exactUploadID); err != nil {
				t.Errorf("clean exact acceptance upload: %v", err)
			}
		})

		neighborKey := artifact.Key + ".operator"
		neighborUploadID := startMinioAcceptanceUpload(t, ctx, adapter, neighborKey, artifact.Metadata())
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			if err := adapter.abortOne(cleanupCtx, neighborKey, neighborUploadID); err != nil {
				t.Errorf("clean neighbor acceptance upload: %v", err)
			}
		})

		if err := adapter.cleanupExactUploads(ctx, artifact.Key); err != nil {
			t.Fatalf("cleanup exact multipart uploads: %v", err)
		}
		exactRemaining, err := adapter.listExactUploads(ctx, artifact.Key)
		if err != nil {
			t.Fatalf("list exact multipart uploads after cleanup: %v", err)
		}
		neighborRemaining, err := adapter.listExactUploads(ctx, neighborKey)
		if err != nil {
			t.Fatalf("list neighbor multipart uploads after cleanup: %v", err)
		}
		if len(exactRemaining) != 0 || len(neighborRemaining) != 1 || neighborRemaining[0] != neighborUploadID {
			t.Fatalf("multipart cleanup counts = exact %d neighbor %d", len(exactRemaining), len(neighborRemaining))
		}
	})
}

func minioAcceptanceConfig(t *testing.T) Config {
	t.Helper()
	values := map[string]string{
		"GROUNDPLANE_MINIO_ENDPOINT":   os.Getenv("GROUNDPLANE_MINIO_ENDPOINT"),
		"GROUNDPLANE_MINIO_BUCKET":     os.Getenv("GROUNDPLANE_MINIO_BUCKET"),
		"GROUNDPLANE_MINIO_ACCESS_KEY": os.Getenv("GROUNDPLANE_MINIO_ACCESS_KEY"),
		"GROUNDPLANE_MINIO_SECRET_KEY": os.Getenv("GROUNDPLANE_MINIO_SECRET_KEY"),
	}
	missing := make([]string, 0, len(values))
	for name, value := range values {
		if value == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) != 0 {
		t.Skipf("MinIO acceptance is closed; set all required variables (missing: %s)", strings.Join(missing, ", "))
	}
	return Config{
		Endpoint:  values["GROUNDPLANE_MINIO_ENDPOINT"],
		Bucket:    values["GROUNDPLANE_MINIO_BUCKET"],
		Region:    "us-east-1",
		PathStyle: true,
		AccessKey: values["GROUNDPLANE_MINIO_ACCESS_KEY"],
		SecretKey: values["GROUNDPLANE_MINIO_SECRET_KEY"],
	}
}

func minioAcceptanceArtifact(
	prefix string,
	environmentID string,
	sourceID string,
	recoveryPointID string,
	size uint64,
	digest [sha256.Size]byte,
) backupobject.Artifact {
	artifact := backupobject.Artifact{
		EnvironmentID:   environmentID,
		SourceID:        sourceID,
		RecoveryPointID: recoveryPointID,
		SourceFormat:    backupobject.SourceFormatVolumeTar,
		Encryption:      backupobject.EncryptionNone,
		Evidence: backupobject.Evidence{
			SourceSizeBytes: size,
			SourceSHA256:    digest,
			StoredSizeBytes: size,
			StoredSHA256:    digest,
		},
	}
	artifact.Key = artifact.ExpectedKey(prefix)
	return artifact
}

func exerciseMinioLifecycle(
	t *testing.T,
	ctx context.Context,
	adapter *Adapter,
	artifact backupobject.Artifact,
	source io.ReaderAt,
) {
	t.Helper()
	object, err := adapter.PutExact(ctx, artifact, source)
	if err != nil {
		t.Fatalf("PutExact: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		if err := adapter.DeleteExact(cleanupCtx, object); err != nil {
			t.Errorf("clean acceptance object: %v", err)
		}
	})
	if err := object.Validate(); err != nil {
		t.Fatalf("PutExact discriminator: %v", err)
	}

	head, err := adapter.HeadExact(ctx, artifact, &object.Discriminator)
	if err != nil {
		t.Fatalf("HeadExact: %v", err)
	}
	if !head.Present || head.Object != object {
		t.Fatalf("HeadExact did not return exact metadata and discriminator")
	}

	destination := &acceptanceDigestWriter{hash: sha256.New()}
	if err := adapter.GetExact(ctx, object, destination); err != nil {
		t.Fatalf("GetExact: %v", err)
	}
	if destination.size != artifact.Evidence.StoredSizeBytes ||
		destination.sum() != artifact.Evidence.StoredSHA256 {
		t.Fatalf("GetExact evidence mismatch: size %d", destination.size)
	}

	if err := adapter.DeleteExact(ctx, object); err != nil {
		t.Fatalf("DeleteExact: %v", err)
	}
	absent, err := adapter.HeadExact(ctx, artifact, &object.Discriminator)
	if err != nil {
		t.Fatalf("HeadExact after delete: %v", err)
	}
	if absent.Present {
		t.Fatal("HeadExact after delete still reports the exact object present")
	}
}

func startMinioAcceptanceUpload(
	t *testing.T,
	ctx context.Context,
	adapter *Adapter,
	key string,
	metadata map[string]string,
) string {
	t.Helper()
	output, err := adapter.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:   aws.String(adapter.config.Bucket),
		Key:      aws.String(key),
		Metadata: metadata,
	}, oneAttempt)
	if err != nil {
		t.Fatalf("create acceptance multipart upload: %v", providerError(err))
	}
	if output == nil || aws.ToString(output.UploadId) == "" {
		t.Fatal("create acceptance multipart upload returned no upload identity")
	}
	return aws.ToString(output.UploadId)
}

type acceptancePatternReaderAt struct {
	size uint64
}

func (reader acceptancePatternReaderAt) ReadAt(destination []byte, offset int64) (int, error) {
	if offset < 0 || uint64(offset) >= reader.size {
		return 0, io.EOF
	}
	length := min(uint64(len(destination)), reader.size-uint64(offset))
	for index := range int(length) {
		destination[index] = byte((uint64(offset) + uint64(index)) % 251)
	}
	if length != uint64(len(destination)) {
		return int(length), io.EOF
	}
	return int(length), nil
}

func acceptanceReaderDigest(
	t *testing.T,
	reader io.ReaderAt,
	size uint64,
) [sha256.Size]byte {
	t.Helper()
	hasher := sha256.New()
	written, err := io.Copy(hasher, io.NewSectionReader(reader, 0, int64(size)))
	if err != nil || uint64(written) != size {
		t.Fatalf("prepare acceptance digest: wrote %d of %d bytes", written, size)
	}
	var digest [sha256.Size]byte
	copy(digest[:], hasher.Sum(nil))
	return digest
}

type acceptanceDigestWriter struct {
	hash hash.Hash
	size uint64
}

func (writer *acceptanceDigestWriter) Write(value []byte) (int, error) {
	written, err := writer.hash.Write(value)
	writer.size += uint64(written)
	return written, err
}

func (writer *acceptanceDigestWriter) sum() [sha256.Size]byte {
	var digest [sha256.Size]byte
	copy(digest[:], writer.hash.Sum(nil))
	return digest
}

func newAcceptanceBytesReader(value []byte) io.ReaderAt {
	return strings.NewReader(string(value))
}
