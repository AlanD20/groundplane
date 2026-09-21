package runner

import (
	"bytes"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type redactedStderr struct {
	limit     int
	secrets   [][]byte
	maxSecret int
	pending   []byte
	output    []byte
	truncated bool
}

func newRedactedStderr(secrets []string) (*redactedStderr, error) {
	if len(secrets) > maxRedactionItems {
		return nil, errs.New(errs.KindValidationFailed, "runner: too many stderr redactions")
	}
	capture := &redactedStderr{
		limit:  maxBinaryStderrBytes,
		output: make([]byte, 0, maxBinaryStderrBytes),
	}
	total := 0
	for _, secret := range secrets {
		if len(secret) > maxRedactionItemBytes {
			return nil, errs.New(errs.KindValidationFailed, "runner: stderr redaction is too large")
		}
		total += len(secret)
		if total > maxRedactionTotalBytes {
			return nil, errs.New(errs.KindValidationFailed, "runner: stderr redactions are too large")
		}
		if secret == "" {
			continue
		}
		capture.secrets = append(capture.secrets, []byte(secret))
		if len(secret) > capture.maxSecret {
			capture.maxSecret = len(secret)
		}
	}
	return capture, nil
}

func (r *redactedStderr) Write(p []byte) (int, error) {
	written := len(p)
	for len(p) > 0 && !r.truncated {
		chunkSize := min(len(p), redactionWriteBytes)
		r.pending = append(r.pending, p[:chunkSize]...)
		p = p[chunkSize:]
		r.flushSafePrefix()
	}
	if r.truncated {
		r.pending = nil
	}
	return written, nil
}

func (r *redactedStderr) flushSafePrefix() {
	flushLen := len(r.pending)
	if r.maxSecret > 0 {
		flushLen -= r.maxSecret
		if flushLen < 0 {
			return
		}
		for {
			adjusted := false
			for _, secret := range r.secrets {
				for start := 0; start+len(secret) <= len(r.pending); {
					index := bytes.Index(r.pending[start:], secret)
					if index < 0 {
						break
					}
					index += start
					if index < flushLen && index+len(secret) > flushLen {
						flushLen = index + len(secret)
						adjusted = true
					}
					start = index + 1
				}
			}
			if !adjusted {
				break
			}
		}
	}
	r.flush(flushLen)
}

func (r *redactedStderr) flush(length int) {
	if length <= 0 {
		return
	}
	r.appendRedacted(r.pending[:length])
	r.pending = append(r.pending[:0], r.pending[length:]...)
}

func (r *redactedStderr) appendRedacted(chunk []byte) {
	for len(chunk) > 0 && !r.truncated {
		matchAt := len(chunk)
		matchLength := 0
		for _, secret := range r.secrets {
			if index := bytes.Index(chunk, secret); index >= 0 &&
				(index < matchAt || index == matchAt && len(secret) > matchLength) {
				matchAt = index
				matchLength = len(secret)
			}
		}
		if matchLength == 0 {
			r.appendOutput(chunk)
			return
		}
		r.appendOutput(chunk[:matchAt])
		r.appendOutput([]byte(stderrRedactedMark))
		chunk = chunk[matchAt+matchLength:]
	}
}

func (r *redactedStderr) appendOutput(p []byte) {
	if r.truncated || len(p) == 0 {
		return
	}
	remaining := r.limit - len(r.output)
	if len(p) > remaining {
		r.output = append(r.output, p[:remaining]...)
		r.truncated = true
		return
	}
	r.output = append(r.output, p...)
}

func (r *redactedStderr) Bytes() []byte {
	if !r.truncated {
		r.flush(len(r.pending))
	}
	if !r.truncated {
		return append([]byte(nil), r.output...)
	}
	mark := []byte(stderrTruncatedMark)
	if len(mark) >= r.limit {
		return append([]byte(nil), mark[:r.limit]...)
	}
	keep := min(r.limit-len(mark), len(r.output))
	result := append([]byte(nil), r.output[:keep]...)
	return append(result, mark...)
}
