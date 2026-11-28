package remotehost

import (
	"crypto/x509"
	"net/url"
	"strconv"
	"strings"
)

func IdentityURI(identity Identity) *url.URL {
	return &url.URL{Scheme: "urn", Opaque: "groundplane:host:" + identity.HostID + ":agent:" + identity.AgentID +
		":generation:" + strconv.FormatUint(identity.Generation, 10)}
}

func CertificateIdentity(certificate *x509.Certificate) (Identity, bool) {
	if certificate == nil || certificate.IsCA || certificate.KeyUsage != x509.KeyUsageDigitalSignature ||
		len(certificate.ExtKeyUsage) != 1 || certificate.ExtKeyUsage[0] != x509.ExtKeyUsageClientAuth ||
		len(certificate.URIs) != 1 || certificate.URIs[0] == nil {
		return Identity{}, false
	}
	uri := certificate.URIs[0]
	parts := strings.Split(uri.Opaque, ":")
	if len(parts) != 7 || uri.Scheme != "urn" || parts[0] != "groundplane" || parts[1] != "host" ||
		parts[3] != "agent" || parts[5] != "generation" {
		return Identity{}, false
	}
	generation, err := strconv.ParseUint(parts[6], 10, 64)
	identity := Identity{HostID: parts[2], AgentID: parts[4], Generation: generation}
	return identity, err == nil && identity.Valid() && *uri == *IdentityURI(identity)
}
