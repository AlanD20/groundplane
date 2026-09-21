package s3compatible

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	singlePutMaximum uint64 = 100 * 1024 * 1024
	partQuantum      uint64 = 1024 * 1024
	maximumParts     uint64 = 10_000
)

type s3API interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	CreateMultipartUpload(
		context.Context,
		*s3.CreateMultipartUploadInput,
		...func(*s3.Options),
	) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(
		context.Context,
		*s3.CompleteMultipartUploadInput,
		...func(*s3.Options),
	) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(
		context.Context,
		*s3.AbortMultipartUploadInput,
		...func(*s3.Options),
	) (*s3.AbortMultipartUploadOutput, error)
	ListMultipartUploads(
		context.Context,
		*s3.ListMultipartUploadsInput,
		...func(*s3.Options),
	) (*s3.ListMultipartUploadsOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

type Adapter struct {
	config Config
	client s3API
}

var _ backupobject.Store = (*Adapter)(nil)
