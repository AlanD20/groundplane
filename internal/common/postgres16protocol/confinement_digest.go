package postgres16protocol

import (
	"crypto/sha256"
)

func appendOptionalDigest(body []byte, present bool, digest Digest) []byte {
	if present {
		return append(body, digest[:]...)
	}
	return append(body, make([]byte, len(Digest{}))...)
}

func appendStreamEvidence(body []byte, evidence ConfinementStreamEvidence) []byte {
	body = appendUint64(body, evidence.Bytes)
	body = append(body, evidence.SHA256[:]...)
	return appendBool(body, evidence.EOF)
}

func appendUint32(body []byte, value uint32) []byte {
	return append(
		body,
		byte(value>>24),
		byte(value>>16),
		byte(value>>8),
		byte(value),
	)
}

func appendUint64(body []byte, value uint64) []byte {
	return append(
		body,
		byte(value>>56),
		byte(value>>48),
		byte(value>>40),
		byte(value>>32),
		byte(value>>24),
		byte(value>>16),
		byte(value>>8),
		byte(value),
	)
}

func appendBool(body []byte, value bool) []byte {
	if value {
		return append(body, 1)
	}
	return append(body, 0)
}

func domainSeparatedDigest(domain string, body []byte) Digest {
	preimage := make([]byte, 0, len(domain)+1+len(body))
	preimage = append(preimage, domain...)
	preimage = append(preimage, 0)
	preimage = append(preimage, body...)
	return Digest(sha256.Sum256(preimage))
}
