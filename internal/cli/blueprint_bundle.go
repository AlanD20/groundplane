package cli

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

const (
	blueprintCLIMaxFiles      = 64
	blueprintCLIMaxFileBytes  = 256 * 1024
	blueprintCLIMaxTotalBytes = 768 * 1024
	blueprintCLIMaxPathBytes  = 240
)

var blueprintCLIInterpolationKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type blueprintCLIManifest struct {
	Root           string                     `json:"root"`
	ComposeSources []string                   `json:"compose_sources"`
	Interpolation  map[string]string          `json:"interpolation"`
	Files          []blueprintCLIManifestFile `json:"files"`
}

type blueprintCLIManifestFile struct {
	Path   string `json:"path"`
	Part   string `json:"part"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type blueprintCLIFile struct {
	path    string
	content []byte
}

func buildBlueprintMultipart(
	bundleDirectory string,
	rootPath string,
	additionalComposeSources []string,
	variables []string,
) ([]byte, string, error) {
	rootPath, err := normalizeBlueprintCLIPath(rootPath)
	if err != nil {
		return nil, "", err
	}
	files, err := readBlueprintCLIDirectory(bundleDirectory)
	if err != nil {
		return nil, "", err
	}
	declared := make(map[string]struct{}, len(files))
	for _, file := range files {
		declared[file.path] = struct{}{}
	}
	if _, exists := declared[rootPath]; !exists {
		return nil, "", errs.New(errs.KindValidationFailed, "Blueprint root is not present in the bundle directory")
	}

	composeSources := []string{rootPath}
	seenSources := map[string]struct{}{rootPath: {}}
	for _, source := range additionalComposeSources {
		normalized, err := normalizeBlueprintCLIPath(source)
		if err != nil {
			return nil, "", err
		}
		if _, exists := declared[normalized]; !exists {
			return nil, "", errs.New(
				errs.KindValidationFailed,
				"Blueprint Compose source is not present in the bundle directory",
			)
		}
		if _, exists := seenSources[normalized]; exists {
			return nil, "", errs.New(errs.KindValidationFailed, "Blueprint Compose sources must be unique")
		}
		seenSources[normalized] = struct{}{}
		composeSources = append(composeSources, normalized)
	}
	interpolation, err := parseBlueprintCLIInterpolation(variables)
	if err != nil {
		return nil, "", err
	}

	manifest := blueprintCLIManifest{
		Root: rootPath, ComposeSources: composeSources, Interpolation: interpolation,
		Files: make([]blueprintCLIManifestFile, len(files)),
	}
	for index, file := range files {
		digest := sha256.Sum256(file.content)
		manifest.Files[index] = blueprintCLIManifestFile{
			Path: file.path, Part: blueprintCLIFilePartName(index), Size: int64(len(file.content)),
			SHA256: hex.EncodeToString(digest[:]),
		}
	}
	manifestBody, err := json.Marshal(manifest)
	if err != nil {
		return nil, "", errs.Wrap(errs.KindInternal, err)
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	manifestHeader := make(textproto.MIMEHeader)
	manifestHeader.Set("Content-Disposition", `form-data; name="manifest"`)
	manifestHeader.Set("Content-Type", "application/json")
	manifestPart, err := writer.CreatePart(manifestHeader)
	if err != nil {
		return nil, "", errs.Wrap(errs.KindInternal, err)
	}
	if _, err := manifestPart.Write(manifestBody); err != nil {
		return nil, "", errs.Wrap(errs.KindInternal, err)
	}
	for index, file := range files {
		header := make(textproto.MIMEHeader)
		header.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"`, blueprintCLIFilePartName(index)))
		header.Set("Content-Type", "application/octet-stream")
		part, err := writer.CreatePart(header)
		if err != nil {
			return nil, "", errs.Wrap(errs.KindInternal, err)
		}
		if _, err := part.Write(file.content); err != nil {
			return nil, "", errs.Wrap(errs.KindInternal, err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, "", errs.Wrap(errs.KindInternal, err)
	}
	return body.Bytes(), writer.FormDataContentType(), nil
}

func readBlueprintCLIDirectory(directory string) ([]blueprintCLIFile, error) {
	if directory == "" {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint bundle directory is required")
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint bundle directory is invalid")
	}
	rootInfo, err := os.Lstat(absolute)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint bundle directory must be a real directory")
	}

	files := make([]blueprintCLIFile, 0)
	totalBytes := int64(0)
	err = filepath.WalkDir(absolute, func(filename string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return errs.New(errs.KindValidationFailed, "Blueprint bundle cannot be read")
		}
		if filename == absolute {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errs.New(errs.KindValidationFailed, "Blueprint bundle must not contain symlinks")
		}
		if entry.IsDir() {
			return nil
		}
		info, err := os.Lstat(filename)
		if err != nil || !info.Mode().IsRegular() {
			return errs.New(errs.KindValidationFailed, "Blueprint bundle must contain only regular files")
		}
		if len(files) == blueprintCLIMaxFiles || info.Size() > blueprintCLIMaxFileBytes {
			return errs.New(errs.KindValidationFailed, "Blueprint bundle exceeds its file limits")
		}
		relative, err := filepath.Rel(absolute, filename)
		if err != nil {
			return errs.New(errs.KindValidationFailed, "Blueprint bundle path is invalid")
		}
		relative, err = normalizeBlueprintCLIPath(filepath.ToSlash(relative))
		if err != nil {
			return err
		}

		opened, err := os.Open(filename)
		if err != nil {
			return errs.New(errs.KindValidationFailed, "Blueprint bundle file cannot be read")
		}
		openedInfo, statErr := opened.Stat()
		content, readErr := io.ReadAll(io.LimitReader(opened, blueprintCLIMaxFileBytes+1))
		closeErr := opened.Close()
		if statErr != nil || !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) ||
			readErr != nil || closeErr != nil || len(content) > blueprintCLIMaxFileBytes ||
			int64(len(content)) != openedInfo.Size() {
			return errs.New(errs.KindValidationFailed, "Blueprint bundle file changed while it was read")
		}
		totalBytes += int64(len(content))
		if totalBytes > blueprintCLIMaxTotalBytes {
			return errs.New(errs.KindValidationFailed, "Blueprint bundle exceeds its total size limit")
		}
		files = append(files, blueprintCLIFile{path: relative, content: content})
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint bundle must contain at least one file")
	}
	sort.Slice(files, func(left, right int) bool { return files[left].path < files[right].path })
	return files, nil
}

func normalizeBlueprintCLIPath(value string) (string, error) {
	if value == "" || !utf8.ValidString(value) || strings.ContainsRune(value, 0) ||
		len(value) > blueprintCLIMaxPathBytes || strings.Contains(value, `\`) || path.IsAbs(value) {
		return "", errs.New(errs.KindValidationFailed, "Blueprint bundle path is invalid")
	}
	cleaned := path.Clean(value)
	if cleaned == "." || cleaned != value || strings.HasPrefix(cleaned, "../") {
		return "", errs.New(errs.KindValidationFailed, "Blueprint bundle path is invalid")
	}
	return cleaned, nil
}

func parseBlueprintCLIInterpolation(values []string) (map[string]string, error) {
	result := make(map[string]string, len(values))
	for _, assignment := range values {
		key, value, exists := strings.Cut(assignment, "=")
		if !exists || !blueprintCLIInterpolationKey.MatchString(key) || !utf8.ValidString(value) ||
			strings.ContainsRune(value, 0) {
			return nil, errs.New(
				errs.KindValidationFailed,
				"Blueprint interpolation must use NUL-free KEY=VALUE entries",
			)
		}
		if _, duplicate := result[key]; duplicate {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint interpolation keys must be unique")
		}
		result[key] = value
	}
	return result, nil
}

func blueprintCLIFilePartName(index int) string {
	return fmt.Sprintf("file-%06d", index+1)
}

func runBlueprintApply(cmd *cobra.Command, environmentID string, body []byte, contentType string) error {
	app := fromContext(cmd)
	var response struct {
		TaskID string `json:"task_id"`
	}
	request := app.Client.NewEncodedRequest(
		http.MethodPut, "/api/v1/environments/"+environmentID+"/blueprint",
		nil, body, contentType, http.StatusAccepted,
	)
	if err := app.Client.Do(cmd.Context(), request, &response); err != nil {
		return err
	}
	if response.TaskID == "" {
		return errs.New(errs.KindInternal, "Blueprint apply response is missing task_id")
	}
	_, err := fmt.Fprintf(
		cmd.OutOrStdout(),
		"task %s dispatched - `groundplane task show %s` to follow\n",
		response.TaskID,
		response.TaskID,
	)
	return err
}
