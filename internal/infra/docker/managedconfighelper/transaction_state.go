package managedconfighelper

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"os"
	"path"
	"syscall"

	"google.golang.org/protobuf/proto"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func responseFor(
	root *os.Root,
	request *agentpb.ManagedConfigHelperRequest,
	previous []byte,
	previousExists bool,
	disposition agentpb.ManagedConfigReplayDisposition,
) (*agentpb.ManagedConfigHelperResponse, error) {
	response := &agentpb.ManagedConfigHelperResponse{
		Schema:        SchemaVersion,
		TransactionId: request.TransactionId,
		Operation:     request.Operation,
		Disposition:   disposition,
	}
	if previousExists {
		digest := sha256.Sum256(previous)
		response.PreviousSha256 = append([]byte(nil), digest[:]...)
	}
	live, found, err := readRegular(root, request.RelativePath)
	if err != nil {
		return nil, err
	}
	defer clear(live)
	if found {
		digest := sha256.Sum256(live)
		response.LiveSha256 = append([]byte(nil), digest[:]...)
		file, openErr := root.Open(request.RelativePath)
		if openErr != nil {
			return nil, errs.Wrap(errs.KindInternal, openErr)
		}
		info, statErr := file.Stat()
		closeErr := file.Close()
		if statErr != nil || closeErr != nil {
			return nil, errs.Wrap(errs.KindInternal, errors.Join(statErr, closeErr))
		}
		if stat, ok := info.Sys().(*syscall.Stat_t); ok {
			response.LiveDevice, response.LiveInode = uint64(stat.Dev), stat.Ino
		}
	}
	return response, nil
}

func readManifest(root *os.Root, tx string) (*agentpb.ManagedConfigHelperRequest, error) {
	encoded, found, err := readRegular(root, path.Join(tx, "request.pb"))
	if err != nil || !found {
		return nil, errs.New(errs.KindStateConflict, "managed-config transaction manifest is missing")
	}
	defer clear(encoded)
	manifest := &agentpb.ManagedConfigHelperRequest{}
	if err := proto.Unmarshal(encoded, manifest); err != nil {
		return nil, errs.New(errs.KindStateConflict, "managed-config transaction manifest is invalid")
	}
	return manifest, nil
}

func readPrevious(root *os.Root, tx string) ([]byte, bool, error) {
	if exists(root, path.Join(tx, "previous.absent")) {
		return nil, false, nil
	}
	value, found, err := readRegular(root, path.Join(tx, "previous"))
	if err != nil || !found {
		return nil, false, errs.New(errs.KindStateConflict, "managed-config predecessor is missing")
	}
	return value, true, nil
}

func sameTransaction(manifest, request *agentpb.ManagedConfigHelperRequest) bool {
	return manifest != nil && request != nil && manifest.Schema == request.Schema &&
		manifest.ArtifactId == request.ArtifactId &&
		manifest.RelativePath == request.RelativePath &&
		manifest.TransactionId == request.TransactionId &&
		manifest.Generation == request.Generation &&
		subtle.ConstantTimeCompare(manifest.Sha256, request.Sha256) == 1 &&
		subtle.ConstantTimeCompare(manifest.ExpectedPreviousSha256, request.ExpectedPreviousSha256) == 1
}
