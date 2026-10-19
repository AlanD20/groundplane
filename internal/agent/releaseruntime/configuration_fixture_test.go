package releaseruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/docker/materializerrunner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type nativeConfigurationExchange struct {
	reference *agentpb.WorkConfigurationReference
	content   []byte
	scenario  string
	buffers   [][]byte
}

func (exchange *nativeConfigurationExchange) file() *agentpb.ReleaseConfigurationFile {
	kind := "plain_file"
	if exchange.reference == nil {
		kind = "remove_plain_file"
	}
	digest := sha256.Sum256(exchange.content)
	return &agentpb.ReleaseConfigurationFile{Destination: "config/application.yaml", OutputKind: kind,
		Uid: 1000, Gid: 1000, Mode: 0o444, Length: uint64(len(exchange.content)), Sha256: digest[:]}
}

func (exchange *nativeConfigurationExchange) Configuration(_ context.Context,
	request *agentpb.ReleaseConfigurationRequest,
) (*agentpb.ReleaseConfigurationManifest, error) {
	manifest := &agentpb.ReleaseConfigurationManifest{Attempt: proto.CloneOf(request.Attempt), StageId: request.StageId,
		Configuration: proto.CloneOf(exchange.reference),
		Files:         []*agentpb.ReleaseConfigurationFile{exchange.file()}}
	if exchange.scenario == "wrong snapshot" {
		manifest.Configuration.ConfigurationId = ids.New(ids.KindConfig)
	}
	if exchange.scenario == "wrong destination" {
		manifest.Files[0].Destination = "config/foreign.yaml"
	}
	return manifest, nil
}

func (exchange *nativeConfigurationExchange) ConfigurationFile(_ context.Context,
	request *agentpb.ReleaseConfigurationFileRequest,
) (*agentpb.ReleaseConfigurationFileDelivery, error) {
	content := bytes.Clone(exchange.content)
	if exchange.scenario == "corrupt content" {
		content[0] ^= 1
	}
	exchange.buffers = append(exchange.buffers, content)
	return &agentpb.ReleaseConfigurationFileDelivery{Attempt: proto.CloneOf(request.Attempt), StageId: request.StageId,
		Configuration: proto.CloneOf(exchange.reference),
		File:          exchange.file(), Content: content}, nil
}

type nativeConfigurationFileHelper struct {
	present    bool
	content    []byte
	events     []string
	failWrite  bool
	failVerify bool
}

func (helper *nativeConfigurationFileHelper) StopCaptured(_ context.Context, _ materializerrunner.Ownership) error {
	helper.events = append(helper.events, "stop")
	return nil
}

func (helper *nativeConfigurationFileHelper) Run(ctx context.Context, request materializerrunner.Request) error {
	var content []byte
	header, err := entrymaterialization.Decode(ctx, request.Stream, entrymaterialization.Limits{
		MaxContentBytes: entrymaterialization.MaximumContentBytes, MaxDestinationBytes: entrymaterialization.MaximumDestinationBytes},
		func(_ context.Context, _ entrymaterialization.Header, source io.Reader) error {
			var err error
			content, err = io.ReadAll(source)
			return err
		})
	if err != nil {
		return err
	}
	present := header.OutputKind() != entrymaterialization.OutputRemovePlainFile
	if request.VerifyOnly {
		helper.events = append(helper.events, "verify")
		if helper.failVerify || helper.present != present || !bytes.Equal(helper.content, content) {
			return errs.New(errs.KindStateConflict, "captured file differs from the observed file")
		}
		return nil
	}
	helper.events = append(helper.events, "write")
	helper.present, helper.content = present, bytes.Clone(content)
	if helper.failWrite {
		helper.failWrite = false
		helper.content = helper.content[:2]
		return errs.New(errs.KindRequestFailed, "injected interrupted file write")
	}
	return nil
}
