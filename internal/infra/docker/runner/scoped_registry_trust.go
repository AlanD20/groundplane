package runner

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"slices"

	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

const (
	scopedRegistryBundleName = "registry-trust.pem"
	hostPublicTrustBundle    = "/etc/ssl/certs/ca-certificates.crt"
	maxRegistryCertificate   = 32 << 10
	maxPublicTrustBundle     = 4 << 20
)

// InstallScopedRegistryTrust publishes the host registry's public certificate
// alongside the installed public roots. The caller supplies the authenticated
// local listener certificate, never a certificate obtained on first connection.
func (control *HostControl) InstallScopedRegistryTrust(
	ctx context.Context,
	plan corerunner.Plan,
	certificatePEM []byte,
) error {
	if err := validateCall(ctx, plan); err != nil {
		return err
	}
	if control.local == nil {
		return errs.New(errs.KindStateConflict, "Registry trust installation requires the local Runner host")
	}
	if err := verifyScopedRegistryLeaf(plan.Egress.ScopedRegistry.Addr(), certificatePEM); err != nil {
		return err
	}
	if err := observeSlotClaim(plan); err != nil {
		return err
	}
	systemBundle, err := readScopedTrustFile(hostPublicTrustBundle, maxPublicTrustBundle, nil)
	if err != nil || !x509.NewCertPool().AppendCertsFromPEM(systemBundle) {
		return errs.New(errs.KindStateConflict, "Host public CA bundle is unavailable")
	}
	bundle := append(systemBundle, '\n')
	bundle = append(bundle, bytes.TrimSpace(certificatePEM)...)
	bundle = append(bundle, '\n')
	directory := scopedCredentialSource(plan)
	if err := ensureOwnedDirectory(directory, 0, plan.Identity.GID, 0o710); err != nil {
		return err
	}
	if err := publishScopedFile(directory, plan, scopedRegistryBundleName, bundle); err != nil {
		return err
	}
	return requireScopedRegistryTrust(plan)
}

func requireScopedRegistryTrust(plan corerunner.Plan) error {
	if !plan.Egress.ScopedRegistry.IsValid() {
		return nil
	}
	if err := observeSlotClaim(plan); err != nil {
		return err
	}
	directory := scopedCredentialSource(plan)
	if err := observeOwnedDirectory(directory, 0, plan.Identity.GID, 0o710); err != nil {
		return err
	}
	bundle, err := readScopedTrustFile(
		scopedRegistryHostBundle(plan),
		maxPublicTrustBundle+maxRegistryCertificate+2,
		&plan.Identity.GID,
	)
	if err != nil {
		return errs.New(errs.KindStateConflict, "Scoped Runner registry trust must be installed before activation")
	}
	last := bytes.LastIndex(bundle, []byte("-----BEGIN CERTIFICATE-----"))
	if last <= 0 || !x509.NewCertPool().AppendCertsFromPEM(bundle[:last]) {
		return errs.New(errs.KindStateConflict, "Scoped Runner registry CA bundle is invalid")
	}
	return verifyScopedRegistryLeaf(plan.Egress.ScopedRegistry.Addr(), bundle[last:])
}

func scopedRegistryHostBundle(plan corerunner.Plan) string {
	return filepath.Join(scopedCredentialSource(plan), scopedRegistryBundleName)
}

func readScopedTrustFile(path string, limit int64, gid *uint32) ([]byte, error) {
	flags := os.O_RDONLY | unix.O_NONBLOCK
	if gid != nil {
		flags |= unix.O_NOFOLLOW
	}
	file, err := os.OpenFile(path, flags, 0)
	if err != nil {
		return nil, errs.Wrap(errs.KindStateConflict, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > limit || gid != nil && !ownedScopedFile(info, *gid) {
		return nil, errs.New(errs.KindStateConflict, "Runner registry trust identity changed")
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, errs.New(errs.KindStateConflict, "Runner registry trust is unavailable")
	}
	return data, nil
}

func verifyScopedRegistryLeaf(endpoint netip.Addr, certificatePEM []byte) error {
	invalid := errs.New(
		errs.KindStateConflict,
		"Scoped Runner registry certificate does not match its private endpoint",
	)
	if !endpoint.Is4() || len(certificatePEM) == 0 || len(certificatePEM) > maxRegistryCertificate {
		return invalid
	}
	encoded := bytes.TrimSpace(certificatePEM)
	begin := []byte("-----BEGIN CERTIFICATE-----")
	block, tail := pem.Decode(encoded)
	if !bytes.HasPrefix(encoded, begin) || bytes.Count(encoded, begin) != 1 ||
		block == nil || block.Type != "CERTIFICATE" || len(bytes.TrimSpace(tail)) != 0 || len(block.Headers) != 0 {
		return invalid
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil || leaf.IsCA || leaf.KeyUsage&x509.KeyUsageCertSign != 0 || len(leaf.IPAddresses) != 1 ||
		len(leaf.DNSNames) != 0 || len(leaf.EmailAddresses) != 0 || len(leaf.URIs) != 0 ||
		!slices.Equal(leaf.ExtKeyUsage, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}) ||
		!bytes.Equal(leaf.RawSubject, leaf.RawIssuer) ||
		leaf.CheckSignature(leaf.SignatureAlgorithm, leaf.RawTBSCertificate, leaf.Signature) != nil {
		return invalid
	}
	address, ok := netip.AddrFromSlice(leaf.IPAddresses[0])
	if !ok || address.Unmap() != endpoint {
		return invalid
	}
	roots := x509.NewCertPool()
	roots.AddCert(leaf)
	if _, err := leaf.Verify(x509.VerifyOptions{DNSName: endpoint.String(), Roots: roots}); err != nil {
		return invalid
	}
	return nil
}
