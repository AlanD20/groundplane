package idempotency

import (
	"encoding/binary"
	"sort"
)

type canonicalEncoder struct {
	buffer []byte
}

func (encoder *canonicalEncoder) clear() {
	clear(encoder.buffer)
	encoder.buffer = nil
}

func (encoder *canonicalEncoder) writeRawBytes(value []byte) {
	encoder.ensure(len(value))
	encoder.buffer = append(encoder.buffer, value...)
}

func (encoder *canonicalEncoder) writeRawString(value string) {
	encoder.ensure(len(value))
	encoder.buffer = append(encoder.buffer, value...)
}

func (encoder *canonicalEncoder) writeLength(value uint64) {
	encoder.ensure(8)
	start := len(encoder.buffer)
	encoder.buffer = encoder.buffer[:start+8]
	binary.BigEndian.PutUint64(encoder.buffer[start:], value)
}

func (encoder *canonicalEncoder) writeScalar(tag valueKind, value []byte) {
	encoder.ensure(1)
	encoder.buffer = append(encoder.buffer, byte(tag))
	encoder.writeLength(uint64(len(value)))
	encoder.writeRawBytes(value)
}

func (encoder *canonicalEncoder) writeNull() { encoder.writeScalar(valueNull, nil) }

func (encoder *canonicalEncoder) writeString(value string) {
	encoder.ensure(1)
	encoder.buffer = append(encoder.buffer, byte(valueString))
	encoder.writeLength(uint64(len(value)))
	encoder.writeRawString(value)
}

func (encoder *canonicalEncoder) writeListHeader(count int) {
	encoder.ensure(1)
	encoder.buffer = append(encoder.buffer, byte(valueList))
	encoder.writeLength(uint64(count))
}

func (encoder *canonicalEncoder) writeObjectHeader(count int) {
	encoder.ensure(1)
	encoder.buffer = append(encoder.buffer, byte(valueObject))
	encoder.writeLength(uint64(count))
}

func (encoder *canonicalEncoder) writeValue(value Value) {
	switch value.kind {
	case valueNull:
		encoder.writeNull()
	case valueFalse, valueTrue:
		encoder.writeScalar(value.kind, nil)
	case valueInteger:
		encoder.ensure(1)
		encoder.buffer = append(encoder.buffer, byte(valueInteger))
		encoder.writeLength(uint64(len(value.integer)))
		encoder.writeRawString(value.integer)
	case valueString:
		encoder.writeString(value.text)
	case valueList:
		encoder.writeListHeader(len(value.items))
		for _, item := range value.items {
			encoder.writeValue(item)
		}
	case valueObject:
		fields := append([]Field(nil), value.fields...)
		sort.Slice(fields, func(left, right int) bool { return fields[left].Name < fields[right].Name })
		encoder.writeObjectHeader(len(fields))
		for _, field := range fields {
			encoder.writeString(field.Name)
			encoder.writeValue(field.Value)
		}
	case valueSHA256:
		encoder.writeScalar(valueSHA256, value.digest[:])
	}
}

func (encoder *canonicalEncoder) ensure(additional int) {
	if additional <= cap(encoder.buffer)-len(encoder.buffer) {
		return
	}
	required := len(encoder.buffer) + additional
	capacity := cap(encoder.buffer) * 2
	if capacity < required {
		capacity = required
	}
	if capacity < 64 {
		capacity = 64
	}
	replacement := make([]byte, len(encoder.buffer), capacity)
	copy(replacement, encoder.buffer)
	clear(encoder.buffer)
	encoder.buffer = replacement
}
