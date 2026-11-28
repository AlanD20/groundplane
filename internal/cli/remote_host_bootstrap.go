package cli

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func newRemoteHostPrepareCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use: "prepare-enrollment <new-directory>", Short: "Generate the Agent's private key and CSR on this machine",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			keyPath, csrPath, err := prepareRemoteHostKey(args[0])
			if err != nil {
				return err
			}
			paths := map[string]string{"private_key": keyPath, "csr": csrPath}
			return fromContext(cmd).Out.RenderOne([]string{"PRIVATE KEY", "CSR"}, []string{keyPath, csrPath}, paths)
		},
	}
	return withExecutionClass(cmd, executionLocal)
}

func prepareRemoteHostKey(directory string) (string, string, error) {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", "", errs.Wrap(errs.KindInternal, err)
	}
	defer clear(key)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", "", errs.Wrap(errs.KindInternal, err)
	}
	defer clear(keyDER)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	defer clear(keyPEM)
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		return "", "", errs.Wrap(errs.KindInternal, err)
	}
	csrPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})
	if err := os.Mkdir(directory, 0700); err != nil {
		return "", "", errs.Wrap(errs.KindValidationFailed, err)
	}
	keyPath, csrPath := filepath.Join(directory, "agent-key.pem"), filepath.Join(directory, "agent.csr.pem")
	if err := writeEnrollmentFile(keyPath, keyPEM, 0600); err != nil {
		_ = os.Remove(directory)
		return "", "", err
	}
	if err := writeEnrollmentFile(csrPath, csrPEM, 0600); err != nil {
		_ = os.Remove(keyPath)
		_ = os.Remove(directory)
		return "", "", err
	}
	return keyPath, csrPath, nil
}

func writeEnrollmentFile(path string, value []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	_, writeErr := file.Write(value)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr == nil {
		writeErr = closeErr
	}
	if writeErr != nil {
		_ = os.Remove(path)
		return errs.Wrap(errs.KindInternal, writeErr)
	}
	return nil
}
