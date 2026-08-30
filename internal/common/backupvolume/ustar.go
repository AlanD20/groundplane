package backupvolume

import (
	"bytes"
	"strconv"
)

const (
	headerNameOffset     = 0
	headerNameWidth      = 100
	headerModeOffset     = 100
	headerUIDOffset      = 108
	headerGIDOffset      = 116
	headerSizeOffset     = 124
	headerMTimeOffset    = 136
	headerChecksumOffset = 148
	headerTypeOffset     = 156
	headerLinkOffset     = 157
	headerMagicOffset    = 257
	headerVersionOffset  = 263
	headerUNameOffset    = 265
	headerGNameOffset    = 297
	headerDevMajorOffset = 329
	headerDevMinorOffset = 337
	headerPrefixOffset   = 345
	headerPaddingOffset  = 500
)

type header struct {
	name     []byte
	prefix   []byte
	mode     uint64
	uid      uint64
	gid      uint64
	size     uint64
	typeflag byte
}

func marshalHeader(value header) ([TarBlockBytes]byte, error) {
	var result [TarBlockBytes]byte
	if len(value.name) < 1 || len(value.name) > headerNameWidth || len(value.prefix) > 155 {
		return result, invalid("volume USTAR name or prefix is invalid")
	}
	copy(result[headerNameOffset:headerNameOffset+headerNameWidth], value.name)
	copy(result[headerPrefixOffset:headerPrefixOffset+155], value.prefix)
	if !encodeOctal(result[headerModeOffset:headerModeOffset+8], value.mode) ||
		!encodeOctal(result[headerUIDOffset:headerUIDOffset+8], value.uid) ||
		!encodeOctal(result[headerGIDOffset:headerGIDOffset+8], value.gid) ||
		!encodeOctal(result[headerSizeOffset:headerSizeOffset+12], value.size) ||
		!encodeOctal(result[headerMTimeOffset:headerMTimeOffset+12], 0) ||
		!encodeOctal(result[headerDevMajorOffset:headerDevMajorOffset+8], 0) ||
		!encodeOctal(result[headerDevMinorOffset:headerDevMinorOffset+8], 0) {
		return [TarBlockBytes]byte{}, invalid("volume USTAR numeric value does not fit")
	}
	for index := 0; index < 8; index++ {
		result[headerChecksumOffset+index] = ' '
	}
	result[headerTypeOffset] = value.typeflag
	copy(result[headerMagicOffset:headerMagicOffset+6], []byte{'u', 's', 't', 'a', 'r', 0})
	copy(result[headerVersionOffset:headerVersionOffset+2], []byte("00"))
	var checksum uint64
	for _, current := range result {
		checksum += uint64(current)
	}
	encoded := strconv.FormatUint(checksum, 8)
	if len(encoded) > 6 {
		return [TarBlockBytes]byte{}, invalid("volume USTAR checksum does not fit")
	}
	for index := 0; index < 6-len(encoded); index++ {
		result[headerChecksumOffset+index] = '0'
	}
	copy(result[headerChecksumOffset+6-len(encoded):headerChecksumOffset+6], encoded)
	result[headerChecksumOffset+6] = 0
	result[headerChecksumOffset+7] = ' '
	return result, nil
}

func parseHeader(block [TarBlockBytes]byte) (header, error) {
	if !bytes.Equal(block[headerMagicOffset:headerMagicOffset+6], []byte{'u', 's', 't', 'a', 'r', 0}) ||
		!bytes.Equal(block[headerVersionOffset:headerVersionOffset+2], []byte("00")) {
		return header{}, invalid("volume USTAR magic or version is invalid")
	}
	if !allZero(block[headerLinkOffset:headerLinkOffset+100]) ||
		!allZero(block[headerUNameOffset:headerUNameOffset+32]) ||
		!allZero(block[headerGNameOffset:headerGNameOffset+32]) ||
		!allZero(block[headerPaddingOffset:TarBlockBytes]) {
		return header{}, invalid("volume USTAR unused field is nonzero")
	}
	name, err := parseString(block[headerNameOffset : headerNameOffset+headerNameWidth])
	if err != nil || len(name) == 0 {
		return header{}, invalid("volume USTAR name field is invalid")
	}
	prefix, err := parseString(block[headerPrefixOffset : headerPrefixOffset+155])
	if err != nil {
		return header{}, err
	}
	mode, err := parseOctal(block[headerModeOffset : headerModeOffset+8])
	if err != nil {
		return header{}, err
	}
	uid, err := parseOctal(block[headerUIDOffset : headerUIDOffset+8])
	if err != nil {
		return header{}, err
	}
	gid, err := parseOctal(block[headerGIDOffset : headerGIDOffset+8])
	if err != nil {
		return header{}, err
	}
	size, err := parseOctal(block[headerSizeOffset : headerSizeOffset+12])
	if err != nil {
		return header{}, err
	}
	mtime, err := parseOctal(block[headerMTimeOffset : headerMTimeOffset+12])
	if err != nil || mtime != 0 {
		return header{}, invalid("volume USTAR mtime is not zero")
	}
	major, err := parseOctal(block[headerDevMajorOffset : headerDevMajorOffset+8])
	if err != nil || major != 0 {
		return header{}, invalid("volume USTAR device major is not zero")
	}
	minor, err := parseOctal(block[headerDevMinorOffset : headerDevMinorOffset+8])
	if err != nil || minor != 0 {
		return header{}, invalid("volume USTAR device minor is not zero")
	}
	if block[headerChecksumOffset+6] != 0 || block[headerChecksumOffset+7] != ' ' {
		return header{}, invalid("volume USTAR checksum encoding is invalid")
	}
	checksum, err := parseOctalDigits(block[headerChecksumOffset : headerChecksumOffset+6])
	if err != nil {
		return header{}, err
	}
	var calculated uint64
	for index, current := range block {
		if index >= headerChecksumOffset && index < headerChecksumOffset+8 {
			calculated += uint64(' ')
		} else {
			calculated += uint64(current)
		}
	}
	if checksum != calculated {
		return header{}, invalid("volume USTAR checksum does not match")
	}
	return header{
		name: append([]byte(nil), name...), prefix: append([]byte(nil), prefix...),
		mode: mode, uid: uid, gid: gid, size: size, typeflag: block[headerTypeOffset],
	}, nil
}

func splitUSTARPath(path []byte) (name []byte, prefix []byte, ok bool) {
	if len(path) <= headerNameWidth {
		return path, nil, true
	}
	for index := len(path) - 1; index > 0; index-- {
		if path[index] == '/' && index <= 155 && len(path)-index-1 >= 1 && len(path)-index-1 <= headerNameWidth {
			return path[index+1:], path[:index], true
		}
	}
	return nil, nil, false
}

func physicalPath(value header) []byte {
	if len(value.prefix) == 0 {
		return append([]byte(nil), value.name...)
	}
	result := make([]byte, 0, len(value.prefix)+1+len(value.name))
	result = append(result, value.prefix...)
	result = append(result, '/')
	return append(result, value.name...)
}

func encodeOctal(destination []byte, value uint64) bool {
	digits := strconv.FormatUint(value, 8)
	if len(digits) > len(destination)-1 {
		return false
	}
	for index := range destination[:len(destination)-1-len(digits)] {
		destination[index] = '0'
	}
	copy(destination[len(destination)-1-len(digits):len(destination)-1], digits)
	destination[len(destination)-1] = 0
	return true
}

func parseOctal(value []byte) (uint64, error) {
	if len(value) < 2 || value[len(value)-1] != 0 {
		return 0, invalid("volume USTAR numeric field is not NUL terminated")
	}
	return parseOctalDigits(value[:len(value)-1])
}

func parseOctalDigits(value []byte) (uint64, error) {
	var result uint64
	for _, current := range value {
		if current < '0' || current > '7' {
			return 0, invalid("volume USTAR numeric field is not canonical octal")
		}
		result = result*8 + uint64(current-'0')
	}
	return result, nil
}

func parseString(value []byte) ([]byte, error) {
	if index := bytes.IndexByte(value, 0); index >= 0 {
		if !allZero(value[index:]) {
			return nil, invalid("volume USTAR string padding is nonzero")
		}
		return value[:index], nil
	}
	return value, nil
}

func allZero(value []byte) bool {
	for _, current := range value {
		if current != 0 {
			return false
		}
	}
	return true
}
