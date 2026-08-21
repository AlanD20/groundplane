package entrymaterialization

import (
	"context"
	"encoding/binary"
	"io"
	"math"
)

const (
	frameMagic   = "GPEM"
	frameVersion = byte(1)

	preludeBytes       = len(frameMagic) + 1
	recordHeaderBytes  = 5
	chunkSequenceBytes = 4
	headerFieldBytes   = 5

	// MaxChunkBytes includes the four-byte sequence followed by content.
	MaxChunkBytes            = 64 * 1024
	maxChunkContentBytes     = MaxChunkBytes - chunkSequenceBytes
	maxSequencedContentBytes = uint64(maxChunkContentBytes) * (uint64(math.MaxUint32) + 1)

	recordHeader  = byte(1)
	recordContent = byte(2)
	recordEnd     = byte(3)

	// Twelve field envelopes plus every fixed-width value and the longest
	// canonical stable ids, excluding only Destination.
	maxHeaderNonDestinationBytes = (12 * headerFieldBytes) + 31 + 31 + 30 + 8 + 30 + 1 + 4 + 4 + 4 + 8 + 32
)

const (
	fieldTaskID = byte(iota + 1)
	fieldStepID
	fieldEnvironmentID
	fieldGeneration
	fieldDestination
	fieldServiceID
	fieldOutputKind
	fieldUID
	fieldGID
	fieldMode
	fieldLength
	fieldDigest
	fieldLimit
)

// Limits is explicit caller policy for one transient frame. Neither value has
// an implicit default.
type Limits struct {
	MaxContentBytes     uint64
	MaxDestinationBytes uint32
}

func (limits Limits) validate() error {
	if limits.MaxContentBytes == 0 || limits.MaxDestinationBytes == 0 {
		return protocolError("invalid frame limits")
	}
	return nil
}

// Consume receives a validated immutable header and a bounded streaming
// reader. It must read content through io.EOF; success before verified EOF is
// rejected. The reader is valid only during the callback.
type Consume func(context.Context, Header, io.Reader) error

// Encode takes ownership of source, streams one complete frame, and closes the
// source on every path. Source.Close must clear its owned plaintext and unblock
// an in-flight Read.
func Encode(
	ctx context.Context,
	destination io.Writer,
	header Header,
	source io.ReadCloser,
	limits Limits,
) error {
	return encodeWithHasher(ctx, destination, header, source, limits, NewHasher())
}

func encodeWithHasher(
	ctx context.Context,
	destination io.Writer,
	header Header,
	source io.ReadCloser,
	limits Limits,
	hasher Hasher,
) (result error) {
	if hasher != nil {
		defer hasher.Destroy()
	}
	if source == nil {
		return protocolError("missing encoder dependency")
	}
	guardContext := ctx
	if guardContext == nil {
		guardContext = context.Background()
	}
	guard := newSourceGuard(guardContext, source)
	defer func() { result = guard.finish(result) }()
	if ctx == nil || destination == nil || hasher == nil {
		return protocolError("missing encoder dependency")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := limits.validate(); err != nil {
		return err
	}
	if err := header.validate(); err != nil {
		return err
	}
	if header.length > limits.MaxContentBytes || header.length > maxSequencedContentBytes ||
		uint64(len(header.destination)) > uint64(limits.MaxDestinationBytes) {
		return protocolError("header exceeds accepted limits")
	}
	if uint64(len(header.destination))+maxHeaderNonDestinationBytes > math.MaxUint32 {
		return protocolError("header exceeds frame representation")
	}

	encodedHeader := encodeHeader(header)
	defer clear(encodedHeader)
	if err := writePrelude(ctx, destination); err != nil {
		return err
	}
	if err := writeRecord(ctx, destination, recordHeader, encodedHeader); err != nil {
		return err
	}

	buffer := make([]byte, maxChunkContentBytes)
	defer clear(buffer)
	remaining := header.length
	var sequence uint32
	for remaining > 0 {
		chunkLength := min(uint64(len(buffer)), remaining)
		chunk := buffer[:int(chunkLength)]
		if err := readExact(ctx, source, chunk); err != nil {
			return err
		}
		if err := hashChunk(hasher, chunk); err != nil {
			return err
		}
		if err := writeChunk(ctx, destination, sequence, chunk); err != nil {
			return err
		}
		clear(chunk)
		remaining -= chunkLength
		sequence++
	}
	if err := requireEOF(ctx, source, "content exceeds declared length"); err != nil {
		return err
	}
	if !hasher.Verify(header.digest) {
		return protocolError("payload digest mismatch")
	}
	return writeRecord(ctx, destination, recordEnd, nil)
}

// Decode takes ownership of source and streams content through consume. It
// returns a Header only after length, digest, terminal record, and EOF verify.
func Decode(
	ctx context.Context,
	source io.ReadCloser,
	limits Limits,
	consume Consume,
) (Header, error) {
	return decodeWithHasher(ctx, source, limits, consume, NewHasher())
}

func decodeWithHasher(
	ctx context.Context,
	source io.ReadCloser,
	limits Limits,
	consume Consume,
	hasher Hasher,
) (header Header, result error) {
	if hasher != nil {
		defer hasher.Destroy()
	}
	if source == nil {
		return Header{}, protocolError("missing decoder dependency")
	}
	guardContext := ctx
	if guardContext == nil {
		guardContext = context.Background()
	}
	guard := newSourceGuard(guardContext, source)
	defer func() {
		result = guard.finish(result)
		if result != nil {
			header = Header{}
		}
	}()
	if ctx == nil || consume == nil || hasher == nil {
		return Header{}, protocolError("missing decoder dependency")
	}
	if err := ctx.Err(); err != nil {
		return Header{}, err
	}

	if err := limits.validate(); err != nil {
		return Header{}, err
	}
	var prelude [preludeBytes]byte
	if err := readExact(ctx, source, prelude[:]); err != nil {
		return Header{}, err
	}
	if string(prelude[:len(frameMagic)]) != frameMagic || prelude[len(frameMagic)] != frameVersion {
		return Header{}, protocolError("invalid frame prelude")
	}

	recordType, recordLength, err := readRecordHeader(ctx, source)
	if err != nil {
		return Header{}, err
	}
	if recordType != recordHeader {
		if recordType == recordContent || recordType == recordEnd {
			return Header{}, protocolError("header is out of order")
		}
		return Header{}, protocolError("unknown frame record")
	}
	maxHeaderBytes := uint64(limits.MaxDestinationBytes) + maxHeaderNonDestinationBytes
	if recordLength == 0 || uint64(recordLength) > maxHeaderBytes {
		return Header{}, protocolError("invalid header length")
	}
	headerBytes := make([]byte, int(recordLength))
	defer clear(headerBytes)
	if err := readExact(ctx, source, headerBytes); err != nil {
		return Header{}, err
	}
	header, err = decodeHeader(headerBytes)
	if err != nil {
		return Header{}, err
	}
	if header.length > limits.MaxContentBytes || header.length > maxSequencedContentBytes ||
		uint64(len(header.destination)) > uint64(limits.MaxDestinationBytes) {
		return Header{}, protocolError("header exceeds accepted limits")
	}

	content := &contentReader{
		ctx:    ctx,
		source: source,
		header: header,
		digest: hasher,
	}
	defer content.destroy()
	if err := consume(ctx, header, content); err != nil {
		return Header{}, err
	}
	if content.failure != nil {
		return Header{}, content.failure
	}
	if !content.complete {
		return Header{}, protocolError("consumer did not verify complete content")
	}
	return header, nil
}

type contentReader struct {
	ctx              context.Context
	source           io.Reader
	header           Header
	digest           Hasher
	chunk            []byte
	offset           int
	total            uint64
	expectedSequence uint32
	complete         bool
	failure          error
}

func (reader *contentReader) Read(target []byte) (int, error) {
	if len(target) == 0 {
		return 0, nil
	}
	if reader.failure != nil {
		return 0, reader.failure
	}
	if reader.complete {
		return 0, io.EOF
	}
	if err := reader.ctx.Err(); err != nil {
		reader.failure = err
		return 0, err
	}
	if reader.offset == len(reader.chunk) {
		clear(reader.chunk)
		reader.chunk = nil
		reader.offset = 0
		if err := reader.loadChunk(); err != nil {
			reader.failure = err
			return 0, err
		}
		if reader.complete {
			return 0, io.EOF
		}
	}
	start := reader.offset
	count := copy(target, reader.chunk[start:])
	clear(reader.chunk[start : start+count])
	reader.offset += count
	if reader.offset == len(reader.chunk) {
		clear(reader.chunk)
		reader.chunk = nil
		reader.offset = 0
	}
	return count, nil
}

func (reader *contentReader) loadChunk() error {
	recordType, recordLength, err := readRecordHeader(reader.ctx, reader.source)
	if err != nil {
		return err
	}
	switch recordType {
	case recordHeader:
		return protocolError("duplicate header")
	case recordContent:
		if recordLength <= chunkSequenceBytes || recordLength > MaxChunkBytes {
			return protocolError("invalid content chunk length")
		}
		var sequence [chunkSequenceBytes]byte
		if err := readExact(reader.ctx, reader.source, sequence[:]); err != nil {
			return err
		}
		if binary.BigEndian.Uint32(sequence[:]) != reader.expectedSequence {
			return protocolError("content chunk is out of order")
		}
		chunkLength := uint64(recordLength - chunkSequenceBytes)
		if reader.total > reader.header.length || chunkLength > reader.header.length-reader.total {
			return protocolError("content exceeds declared length")
		}
		reader.chunk = make([]byte, int(chunkLength))
		if err := readExact(reader.ctx, reader.source, reader.chunk); err != nil {
			clear(reader.chunk)
			reader.chunk = nil
			return err
		}
		if err := hashChunk(reader.digest, reader.chunk); err != nil {
			return err
		}
		reader.total += chunkLength
		reader.expectedSequence++
		return nil
	case recordEnd:
		if recordLength != 0 || reader.total != reader.header.length {
			return protocolError("content length mismatch")
		}
		if !reader.digest.Verify(reader.header.digest) {
			return protocolError("content digest mismatch")
		}
		if err := requireEOF(reader.ctx, reader.source, "extra bytes after frame"); err != nil {
			return err
		}
		reader.complete = true
		return nil
	default:
		return protocolError("unknown frame record")
	}
}

func (reader *contentReader) destroy() {
	clear(reader.chunk)
	reader.chunk = nil
	reader.digest.Destroy()
}

func encodeHeader(header Header) []byte {
	encoded := make([]byte, 0, maxHeaderNonDestinationBytes+len(header.destination))
	encoded = appendField(encoded, fieldTaskID, []byte(header.taskID))
	encoded = appendField(encoded, fieldStepID, []byte(header.stepID))
	encoded = appendField(encoded, fieldEnvironmentID, []byte(header.environmentID))
	encoded = appendUint64Field(encoded, fieldGeneration, header.generation)
	encoded = appendField(encoded, fieldDestination, []byte(header.destination))
	encoded = appendField(encoded, fieldServiceID, []byte(header.serviceID))
	encoded = appendField(encoded, fieldOutputKind, []byte{byte(header.outputKind)})
	encoded = appendUint32Field(encoded, fieldUID, header.uid)
	encoded = appendUint32Field(encoded, fieldGID, header.gid)
	encoded = appendUint32Field(encoded, fieldMode, uint32(header.mode))
	encoded = appendUint64Field(encoded, fieldLength, header.length)
	encoded = appendField(encoded, fieldDigest, header.digest[:])
	return encoded
}

func decodeHeader(encoded []byte) (Header, error) {
	var spec HeaderSpec
	var seen [fieldLimit]bool
	for offset := 0; offset < len(encoded); {
		if len(encoded)-offset < headerFieldBytes {
			return Header{}, protocolError("truncated header field")
		}
		tag := encoded[offset]
		length := uint64(binary.BigEndian.Uint32(encoded[offset+1 : offset+headerFieldBytes]))
		offset += headerFieldBytes
		if tag == 0 || tag >= fieldLimit {
			return Header{}, protocolError("unknown header field")
		}
		if seen[tag] {
			return Header{}, protocolError("duplicate header field")
		}
		if length > uint64(len(encoded)-offset) {
			return Header{}, protocolError("truncated header field")
		}
		seen[tag] = true
		value := encoded[offset : offset+int(length)]
		offset += int(length)

		switch tag {
		case fieldTaskID:
			spec.TaskID = string(value)
		case fieldStepID:
			spec.StepID = string(value)
		case fieldEnvironmentID:
			spec.EnvironmentID = string(value)
		case fieldGeneration:
			if len(value) != 8 {
				return Header{}, protocolError("invalid generation field")
			}
			spec.Generation = binary.BigEndian.Uint64(value)
		case fieldDestination:
			spec.Destination = string(value)
		case fieldServiceID:
			spec.ServiceID = string(value)
		case fieldOutputKind:
			if len(value) != 1 {
				return Header{}, protocolError("invalid output kind field")
			}
			spec.OutputKind = OutputKind(value[0])
		case fieldUID:
			if len(value) != 4 {
				return Header{}, protocolError("invalid uid field")
			}
			spec.UID = binary.BigEndian.Uint32(value)
		case fieldGID:
			if len(value) != 4 {
				return Header{}, protocolError("invalid gid field")
			}
			spec.GID = binary.BigEndian.Uint32(value)
		case fieldMode:
			if len(value) != 4 {
				return Header{}, protocolError("invalid mode field")
			}
			spec.Mode = Mode(binary.BigEndian.Uint32(value))
		case fieldLength:
			if len(value) != 8 {
				return Header{}, protocolError("invalid length field")
			}
			spec.Length = binary.BigEndian.Uint64(value)
		case fieldDigest:
			if len(value) != len(spec.Digest) {
				return Header{}, protocolError("invalid digest field")
			}
			copy(spec.Digest[:], value)
		}
	}
	for tag := byte(1); tag < fieldLimit; tag++ {
		if !seen[tag] {
			return Header{}, protocolError("missing header field")
		}
	}
	return NewHeader(spec)
}

func appendField(target []byte, tag byte, value []byte) []byte {
	target = append(target, tag)
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	target = append(target, length[:]...)
	return append(target, value...)
}

func appendUint32Field(target []byte, tag byte, value uint32) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	return appendField(target, tag, encoded[:])
}

func appendUint64Field(target []byte, tag byte, value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return appendField(target, tag, encoded[:])
}

func writePrelude(ctx context.Context, destination io.Writer) error {
	var prelude [preludeBytes]byte
	copy(prelude[:], frameMagic)
	prelude[len(frameMagic)] = frameVersion
	_, err := writeAll(ctx, destination, prelude[:])
	return err
}

func writeRecord(ctx context.Context, destination io.Writer, recordType byte, payload []byte) error {
	var header [recordHeaderBytes]byte
	header[0] = recordType
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	if _, err := writeAll(ctx, destination, header[:]); err != nil {
		return err
	}
	if _, err := writeAll(ctx, destination, payload); err != nil {
		return err
	}
	return nil
}

func writeChunk(ctx context.Context, destination io.Writer, sequence uint32, content []byte) error {
	var header [recordHeaderBytes + chunkSequenceBytes]byte
	header[0] = recordContent
	binary.BigEndian.PutUint32(header[1:recordHeaderBytes], uint32(len(content)+chunkSequenceBytes))
	binary.BigEndian.PutUint32(header[recordHeaderBytes:], sequence)
	if _, err := writeAll(ctx, destination, header[:]); err != nil {
		return err
	}
	if _, err := writeAll(ctx, destination, content); err != nil {
		return err
	}
	return nil
}

func readRecordHeader(ctx context.Context, source io.Reader) (byte, uint32, error) {
	var header [recordHeaderBytes]byte
	if err := readExact(ctx, source, header[:]); err != nil {
		return 0, 0, err
	}
	return header[0], binary.BigEndian.Uint32(header[1:]), nil
}

func readExact(ctx context.Context, source io.Reader, target []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := io.ReadFull(source, target); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		return protocolError("truncated frame")
	}
	return ctx.Err()
}

func requireEOF(ctx context.Context, source io.Reader, extraMessage string) error {
	var extra [1]byte
	defer clear(extra[:])
	for emptyReads := 0; ; emptyReads++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		count, err := source.Read(extra[:])
		if count != 0 {
			return protocolError(extraMessage)
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return contextErr
			}
			return protocolError("frame source failed")
		}
		if emptyReads == 99 {
			return protocolError("frame source made no progress")
		}
	}
}

func hashChunk(digest Hasher, content []byte) error {
	written, err := digest.Write(content)
	if err != nil || written != len(content) {
		return protocolError("content digest failed")
	}
	return nil
}
