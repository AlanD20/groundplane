// Package remotetls loads the explicitly provisioned remote channel credentials.
package remotetls

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"os"
	"syscall"

	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

const maximumPEMBytes = 1 << 20

func KeyPair(certificatePath, keyPath string) (tls.Certificate, error) {
	certificate, err := readPEM(certificatePath, false)
	if err != nil {
		return tls.Certificate{}, err
	}
	key, err := readPEM(keyPath, true)
	if err != nil {
		return tls.Certificate{}, err
	}
	defer clear(key)
	pair, err := tls.X509KeyPair(certificate, key)
	if err != nil {
		return tls.Certificate{}, errs.New(
			errs.KindValidationFailed,
			"Remote TLS certificate and private key do not form a valid pair",
		)
	}
	return pair, nil
}

func Roots(path string) (*x509.CertPool, error) {
	value, err := readPEM(path, false)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(value) {
		return nil, errs.New(errs.KindValidationFailed, "Remote Controller TLS trust file has no valid certificates")
	}
	return roots, nil
}

func Authority(path string) (*x509.Certificate, error) {
	value, err := readPEM(path, false)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(value)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, errs.New(errs.KindValidationFailed, "Remote Agent enrollment CA certificate is invalid")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil || !certificate.BasicConstraintsValid || !certificate.IsCA ||
		certificate.KeyUsage&x509.KeyUsageCertSign == 0 {
		return nil, errs.New(errs.KindValidationFailed, "Remote Agent enrollment CA certificate is invalid")
	}
	return certificate, nil
}

func readPEM(path string, private bool) ([]byte, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, errs.Wrap(errs.KindValidationFailed, err)
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return nil, errs.Wrap(errs.KindValidationFailed, err)
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximumPEMBytes {
		return nil, errs.New(errs.KindValidationFailed, "Remote TLS credentials must use bounded regular files")
	}
	if private {
		owner, ok := info.Sys().(*syscall.Stat_t)
		if !ok || owner.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0077 != 0 {
			return nil, errs.New(
				errs.KindValidationFailed,
				"Remote TLS private keys must be owned by the daemon and readable only by their owner",
			)
		}
	}
	value, err := io.ReadAll(io.LimitReader(file, maximumPEMBytes+1))
	if err != nil || len(value) == 0 || len(value) > maximumPEMBytes {
		clear(value)
		return nil, errs.New(errs.KindValidationFailed, "Remote TLS credential could not be read within its bound")
	}
	return value, nil
}
