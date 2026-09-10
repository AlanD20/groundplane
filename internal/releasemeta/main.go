// Command releasemeta records source compatibility and binary identity without
// executing the candidate. Deployment adds the registry-resolved Agent image.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/jcs"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "releasemeta:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("releasemeta", flag.ContinueOnError)
	binary := flags.String("controller", "", "built Controller executable")
	version := flags.String("version", "", "exact Controller build version")
	output := flags.String("output", "", "build metadata output file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *binary == "" || *version == "" || *output == "" || flags.NArg() != 0 {
		return errors.New("controller, version and output are required")
	}
	file, err := os.Open(*binary)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	const maximum = 256 << 20
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum {
		return errors.New("controller build must be a bounded regular file")
	}
	digest := sha256.New()
	count, err := io.Copy(digest, io.LimitReader(file, maximum+1))
	if err != nil {
		return err
	}
	if count != info.Size() {
		return errors.New("controller build size changed during hashing")
	}
	metadata, err := upgrade.DescribeBuild(upgrade.Digest(fmt.Sprintf("sha256:%x", digest.Sum(nil))), *version)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return err
	}
	staging, err := os.CreateTemp(filepath.Dir(*output), ".controller-build-*")
	if err != nil {
		return err
	}
	defer func() {
		_ = staging.Close()           // Cleanup after a write error or successful explicit close.
		_ = os.Remove(staging.Name()) // Only this invocation's unpublished leaf; renamed on success.
	}()
	if _, err := staging.Write(canonical); err != nil {
		return err
	}
	if err := staging.Close(); err != nil {
		return err
	}
	return os.Rename(staging.Name(), *output)
}
