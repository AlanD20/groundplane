package handlers

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"maps"
	"mime"
	"mime/multipart"
	"net/http"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const blueprintManifestMaxBytes = 256 * 1024

type blueprintMultipartManifest struct {
	Root           string                   `json:"root"`
	ComposeSources []string                 `json:"compose_sources"`
	Interpolation  map[string]string        `json:"interpolation"`
	Files          []blueprintMultipartFile `json:"files"`
}

type blueprintMultipartFile struct {
	Path   string `json:"path"`
	Part   string `json:"part"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// decodeBlueprintMultipart authenticates the closed multipart namespace while
// streaming each declared file through its bounded digest check. The manifest
// must be first so no undeclared bytes are buffered before their identity and
// limits are known.
func decodeBlueprintMultipart(request *http.Request) (core.BlueprintBundle, error) {
	if request == nil {
		return core.BlueprintBundle{}, errs.New(errs.KindInternal, "Blueprint request is required")
	}
	reader, err := request.MultipartReader()
	if err != nil {
		return core.BlueprintBundle{}, malformedBlueprintMultipart()
	}

	part, err := reader.NextPart()
	if err != nil {
		return core.BlueprintBundle{}, malformedBlueprintMultipart()
	}
	defer part.Close()
	if part.FormName() != "manifest" || part.FileName() != "" ||
		!blueprintPartContentType(part, "application/json") {
		return core.BlueprintBundle{}, malformedBlueprintMultipart()
	}
	manifestBody, err := io.ReadAll(io.LimitReader(part, blueprintManifestMaxBytes+1))
	if err != nil {
		return core.BlueprintBundle{}, malformedBlueprintMultipart()
	}
	if len(manifestBody) > blueprintManifestMaxBytes {
		return core.BlueprintBundle{}, errs.New(errs.KindRequestTooLarge, "Blueprint manifest exceeds its size limit")
	}
	manifest, err := decodeBlueprintManifest(manifestBody)
	if err != nil {
		return core.BlueprintBundle{}, err
	}

	bundle := core.BlueprintBundle{
		RootPath:       manifest.Root,
		ComposeSources: append([]string(nil), manifest.ComposeSources...),
		Interpolation:  maps.Clone(manifest.Interpolation),
		Files:          make([]core.BlueprintFile, 0, len(manifest.Files)),
	}
	if len(manifest.Files) == 0 || len(manifest.Files) > core.BlueprintBundleMaxFiles {
		return core.BlueprintBundle{}, invalidBlueprintBundle()
	}
	totalSize := int64(0)
	for index, declared := range manifest.Files {
		expectedPart := blueprintFilePartName(index)
		if declared.Part != expectedPart || declared.Size < 0 ||
			declared.Size > core.BlueprintBundleMaxFileBytes {
			return core.BlueprintBundle{}, invalidBlueprintBundle()
		}
		totalSize += declared.Size
		if totalSize > core.BlueprintBundleMaxTotalBytes {
			return core.BlueprintBundle{}, invalidBlueprintBundle()
		}
		digest, err := decodeBlueprintDigest(declared.SHA256)
		if err != nil {
			return core.BlueprintBundle{}, invalidBlueprintBundle()
		}

		part, err = reader.NextPart()
		if err != nil {
			return core.BlueprintBundle{}, malformedBlueprintMultipart()
		}
		if part.FormName() != expectedPart || part.FileName() != "" ||
			!blueprintPartContentType(part, "application/octet-stream") {
			_ = part.Close()
			return core.BlueprintBundle{}, malformedBlueprintMultipart()
		}
		content, readErr := io.ReadAll(io.LimitReader(part, declared.Size+1))
		closeErr := part.Close()
		if readErr != nil || closeErr != nil || int64(len(content)) != declared.Size {
			return core.BlueprintBundle{}, malformedBlueprintMultipart()
		}
		actual := sha256.Sum256(content)
		if !bytes.Equal(actual[:], digest) {
			return core.BlueprintBundle{}, malformedBlueprintMultipart()
		}
		bundle.Files = append(bundle.Files, core.BlueprintFile{
			Path: declared.Path, Content: content,
		})
	}
	if trailing, err := reader.NextPart(); err != io.EOF {
		if trailing != nil {
			_ = trailing.Close()
		}
		return core.BlueprintBundle{}, malformedBlueprintMultipart()
	}
	if err := bundle.Validate(); err != nil {
		return core.BlueprintBundle{}, invalidBlueprintBundle()
	}
	return bundle, nil
}

func decodeBlueprintManifest(value []byte) (blueprintMultipartManifest, error) {
	if err := rejectBlueprintDuplicateJSON(value); err != nil {
		return blueprintMultipartManifest{}, malformedBlueprintMultipart()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var manifest blueprintMultipartManifest
	if err := decoder.Decode(&manifest); err != nil {
		return blueprintMultipartManifest{}, malformedBlueprintMultipart()
	}
	if err := requireBlueprintJSONEOF(decoder); err != nil {
		return blueprintMultipartManifest{}, malformedBlueprintMultipart()
	}
	return manifest, nil
}

func rejectBlueprintDuplicateJSON(value []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(value))
	if err := walkBlueprintJSON(decoder); err != nil {
		return err
	}
	return requireBlueprintJSONEOF(decoder)
}

func walkBlueprintJSON(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return io.ErrUnexpectedEOF
			}
			if _, exists := seen[key]; exists {
				return errs.New(errs.KindMalformedRequest, "Blueprint manifest contains a duplicate member")
			}
			seen[key] = struct{}{}
			if err := walkBlueprintJSON(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return io.ErrUnexpectedEOF
		}
	case '[':
		for decoder.More() {
			if err := walkBlueprintJSON(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return io.ErrUnexpectedEOF
		}
	default:
		return io.ErrUnexpectedEOF
	}
	return nil
}

func requireBlueprintJSONEOF(decoder *json.Decoder) error {
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return errs.New(errs.KindMalformedRequest, "Blueprint manifest has trailing data")
		}
		return err
	}
	return nil
}

func blueprintPartContentType(part *multipart.Part, expected string) bool {
	mediaType, parameters, err := mime.ParseMediaType(part.Header.Get("Content-Type"))
	return err == nil && mediaType == expected && len(parameters) == 0
}

func blueprintFilePartName(index int) string {
	const digits = 6
	value := index + 1
	name := []byte("file-000000")
	for offset := len(name) - 1; offset >= len(name)-digits; offset-- {
		name[offset] = byte('0' + value%10)
		value /= 10
	}
	return string(name)
}

func decodeBlueprintDigest(value string) ([]byte, error) {
	digest, err := hex.DecodeString(value)
	if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != value {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint file digest is invalid")
	}
	return digest, nil
}

func malformedBlueprintMultipart() error {
	return errs.New(errs.KindMalformedRequest, "Blueprint multipart request is malformed")
}

func invalidBlueprintBundle() error {
	return errs.New(errs.KindValidationFailed, "Blueprint bundle is invalid")
}
