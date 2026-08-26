package swarmcheck

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadManifest reads one strict, bounded JSON manifest. Duplicate keys,
// unknown fields, trailing values, invalid UTF-8, and oversized structures are
// rejected before normal decoding can silently normalize them.
func ReadManifest(ctx context.Context, filePath string) (Manifest, error) {
	var manifest Manifest
	if err := contextErr(ctx); err != nil {
		return manifest, err
	}
	file, err := os.Open(filePath)
	if err != nil {
		return manifest, invalid(fmt.Sprintf("read swarm manifest: %v", err))
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxManifestBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return manifest, invalid(fmt.Sprintf("read swarm manifest: %v", readErr))
	}
	if closeErr != nil {
		return manifest, invalid(fmt.Sprintf("close swarm manifest: %v", closeErr))
	}
	if len(data) > maxManifestBytes {
		return manifest, invalid(fmt.Sprintf("swarm manifest exceeds %d bytes", maxManifestBytes))
	}
	if !utf8.Valid(data) {
		return manifest, invalid("swarm manifest is not valid UTF-8")
	}
	if err := validateJSONDocument(data, maxStringBytes); err != nil {
		return manifest, errs.Wrap(
			errs.KindValidationFailed,
			fmt.Errorf("decode swarm manifest: %w", err),
		)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return manifest, errs.Wrap(
			errs.KindValidationFailed,
			fmt.Errorf("decode swarm manifest: %w", err),
		)
	}
	if err := ensureEOF(decoder); err != nil {
		return manifest, err
	}
	if err := manifest.Validate(ctx); err != nil {
		return manifest, err
	}
	return manifest, nil
}

// WriteManifest writes a strict manifest representation with private file
// permissions. Callers must be the primary integration owner.
func WriteManifest(ctx context.Context, filePath string, manifest Manifest) error {
	if err := manifest.Validate(ctx); err != nil {
		return err
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return invalid(fmt.Sprintf("encode swarm manifest: %v", err))
	}
	if len(data) > maxManifestBytes {
		return invalid(fmt.Sprintf("swarm manifest exceeds %d bytes", maxManifestBytes))
	}
	if err := contextErr(ctx); err != nil {
		return err
	}
	directory := filepath.Dir(filePath)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return invalid(fmt.Sprintf("create manifest directory: %v", err))
	}
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(filePath)+".tmp-")
	if err != nil {
		return invalid(fmt.Sprintf("create manifest temporary file: %v", err))
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			// Best-effort cleanup cannot replace the write error.
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		// Best-effort cleanup cannot replace the permission error.
		_ = temporary.Close()
		return invalid(fmt.Sprintf("set manifest temporary permissions: %v", err))
	}
	if _, err := temporary.Write(append(data, '\n')); err != nil {
		// Best-effort cleanup cannot replace the write error.
		_ = temporary.Close()
		return invalid(fmt.Sprintf("write swarm manifest: %v", err))
	}
	if err := contextErr(ctx); err != nil {
		// Best-effort cleanup cannot replace cancellation.
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		// Best-effort cleanup cannot replace the sync error.
		_ = temporary.Close()
		return invalid(fmt.Sprintf("sync swarm manifest: %v", err))
	}
	if err := temporary.Close(); err != nil {
		return invalid(fmt.Sprintf("close swarm manifest: %v", err))
	}
	if err := os.Rename(temporaryPath, filePath); err != nil {
		return invalid(fmt.Sprintf("replace swarm manifest: %v", err))
	}
	removeTemporary = false
	directoryFile, err := os.Open(directory)
	if err != nil {
		return invalid(fmt.Sprintf("open manifest directory: %v", err))
	}
	if err := directoryFile.Sync(); err != nil {
		// Best-effort cleanup cannot replace the directory sync error.
		_ = directoryFile.Close()
		return invalid(fmt.Sprintf("sync manifest directory: %v", err))
	}
	if err := directoryFile.Close(); err != nil {
		return invalid(fmt.Sprintf("close manifest directory: %v", err))
	}
	return nil
}

func validateJSONDocument(data []byte, stringLimit int) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := walkJSONValue(decoder, 0, stringLimit); err != nil {
		return err
	}
	return ensureEOF(decoder)
}

func walkJSONValue(decoder *json.Decoder, depth, stringLimit int) error {
	if depth > maxJSONDepth {
		return invalid(fmt.Sprintf("JSON nesting exceeds %d", maxJSONDepth))
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if value, ok := token.(string); ok && len(value) > stringLimit {
		return invalid(fmt.Sprintf("JSON string exceeds %d bytes", stringLimit))
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]struct{}{}
		count := 0
		for decoder.More() {
			if count >= maxCollection {
				return invalid("JSON object exceeds collection bound")
			}
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok || len(key) > maxStringBytes {
				return invalid("JSON object key is invalid")
			}
			if _, duplicate := seen[key]; duplicate {
				return invalid(fmt.Sprintf("duplicate JSON object field %q", key))
			}
			seen[key] = struct{}{}
			if err := walkJSONValue(decoder, depth+1, stringLimit); err != nil {
				return err
			}
			count++
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return invalid("JSON object does not terminate")
		}
	case '[':
		count := 0
		for decoder.More() {
			if count >= maxCollection {
				return invalid("JSON array exceeds collection bound")
			}
			if err := walkJSONValue(decoder, depth+1, stringLimit); err != nil {
				return err
			}
			count++
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return invalid("JSON array does not terminate")
		}
	default:
		return invalid(fmt.Sprintf("unexpected JSON delimiter %q", delim))
	}
	return nil
}

func ensureEOF(decoder *json.Decoder) error {
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return invalid("trailing JSON document")
		}
		return invalid(fmt.Sprintf("trailing JSON: %v", err))
	}
	return nil
}
