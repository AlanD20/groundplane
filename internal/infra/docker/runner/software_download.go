package runner

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/runnersoftware"
	"github.com/AlanD20/groundplane/pkg/errs"
)

var packageNames = []string{"bin", "externals", "run.sh", "run-helper.sh", "config.sh", "env.sh", "safe_sleep.sh"}

func validPackageName(name string) bool { return slices.Contains(packageNames, name) }

func (updater *SoftwareUpdater) Prepare(
	ctx context.Context,
	plan runnerallocation.RuntimePlan,
	request runnersoftware.Request,
) (result error) {
	if err := validateCall(ctx, plan); err != nil {
		return err
	}
	if request.Validate() != nil || request.RunnerID != plan.RunnerID || request.RuntimeEpoch != plan.RuntimeEpoch ||
		request.PlanSHA256 != plan.Digest() ||
		request.RunnerHome != plan.Paths.RunnerHome {
		return errs.New(errs.KindValidationFailed, "Runner update does not match its captured runtime")
	}
	if err := observeSlotClaim(plan); err != nil {
		return err
	}
	root := softwareDirectory(plan, request)
	for _, dir := range []string{"/var/lib/groundplane/runner-software", filepath.Dir(root), root} {
		if err := ensureOwnedDirectory(dir, 0, 0, 0o700); err != nil {
			return err
		}
	}
	if _, err := os.Lstat(filepath.Join(root, "state.json")); err == nil {
		state, err := loadSoftwareJournal(root)
		if err != nil {
			return err
		}
		if state.Request != request {
			return errs.New(errs.KindStateConflict, "Runner update directory has another owner")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return errs.Wrap(errs.KindInternal, err)
	}
	archive := filepath.Join(root, "download.tar.gz")
	if err := os.Remove(archive); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := os.RemoveAll(filepath.Join(root, "package")); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer func() {
		os.Remove(archive)
		if result != nil {
			os.RemoveAll(filepath.Join(root, "package"))
		}
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, request.Release.URL, nil)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	client := &http.Client{
		Timeout: 5 * time.Minute,
		CheckRedirect: func(next *http.Request, prior []*http.Request) error {
			if len(prior) > 5 || next.URL.Scheme != "https" ||
				!(next.URL.Hostname() == "github.com" || strings.HasSuffix(next.URL.Hostname(), ".githubusercontent.com")) {
				return errs.New(errs.KindValidationFailed, "Runner artifact redirected outside GitHub HTTPS")
			}
			return nil
		},
	}
	response, err := client.Do(req)
	if err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errs.Newf(errs.KindStorageUnavailable, "GitHub Runner download returned HTTP %d", response.StatusCode)
	}
	file, err := os.OpenFile(archive, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(file, hash), io.LimitReader(response.Body, request.Release.Size+1))
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(copyErr, syncErr, closeErr); err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if size != request.Release.Size || hex.EncodeToString(hash.Sum(nil)) != request.Release.SHA256 {
		return errs.New(errs.KindValidationFailed, "GitHub Runner artifact failed its captured size or SHA256 check")
	}
	if err := extractRunnerPackage(archive, filepath.Join(root, "package"), plan); err != nil {
		return err
	}
	return saveSoftwareJournal(root, softwareJournal{Request: request, Phase: "prepared"})
}

func extractRunnerPackage(archive, destination string, plan runnerallocation.RuntimePlan) error {
	if err := os.Mkdir(destination, 0o700); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	file, err := os.Open(archive)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer file.Close()
	zipped, err := gzip.NewReader(file)
	if err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	defer zipped.Close()
	reader := tar.NewReader(zipped)
	seen := map[string]bool{}
	var total int64
	for count := 0; ; count++ {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return errs.Wrap(errs.KindValidationFailed, err)
		}
		name := strings.TrimPrefix(header.Name, "./")
		clean := filepath.Clean(name)
		if clean == "." {
			continue
		}
		if count > 30000 || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") || seen[clean] ||
			header.Size < 0 ||
			header.Size > 512<<20 {
			return errs.New(
				errs.KindValidationFailed,
				"GitHub Runner archive has invalid paths or exceeds its extraction limit",
			)
		}
		seen[clean] = true
		top := strings.Split(clean, string(filepath.Separator))[0]
		if !validPackageName(top) {
			continue
		}
		target := filepath.Join(destination, clean)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		// No archive entry may redirect extraction through an earlier symlink.
		for parent := filepath.Dir(target); parent != destination && parent != "/"; parent = filepath.Dir(parent) {
			info, err := os.Lstat(parent)
			if err != nil || info.Mode()&os.ModeSymlink != 0 {
				return errs.New(errs.KindValidationFailed, "Runner archive traverses a symlink")
			}
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return errs.Wrap(errs.KindInternal, err)
			}
		case tar.TypeReg:
			total += header.Size
			if total > 1<<30 {
				return errs.New(errs.KindValidationFailed, "Runner archive exceeds one GiB unpacked")
			}
			output, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, os.FileMode(header.Mode)&0o777)
			if err != nil {
				return errs.Wrap(errs.KindInternal, err)
			}
			n, a := io.Copy(output, io.LimitReader(reader, header.Size+1))
			b := output.Close()
			if n != header.Size || a != nil || b != nil {
				return errs.New(errs.KindValidationFailed, "Runner archive file is incomplete")
			}
		case tar.TypeSymlink:
			link := filepath.Clean(filepath.Join(filepath.Dir(clean), header.Linkname))
			if filepath.IsAbs(header.Linkname) || link == ".." || strings.HasPrefix(link, "../") {
				return errs.New(errs.KindValidationFailed, "Runner archive contains an escaping symlink")
			}
			if err := os.Symlink(header.Linkname, target); err != nil {
				return errs.Wrap(errs.KindInternal, err)
			}
		default:
			return errs.New(errs.KindValidationFailed, "Runner archive contains unsupported special files")
		}
	}
	info, err := os.Stat(filepath.Join(destination, "bin/Runner.Listener"))
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return errs.New(errs.KindValidationFailed, "Runner archive has no executable listener")
	}
	walkErr := filepath.WalkDir(destination, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		return os.Lchown(path, int(plan.Identity.UID), int(plan.Identity.GID))
	})
	if walkErr != nil {
		return errs.Wrap(errs.KindInternal, walkErr)
	}
	return nil
}
