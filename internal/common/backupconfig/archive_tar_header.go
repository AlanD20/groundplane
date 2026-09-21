package backupconfig

func canonicalHeader(name string, mode uint32, size uint64) ([TarBlockBytes]byte, error) {
	var header [TarBlockBytes]byte
	if len(name) == 0 || len(name) > headerNameLength || size > maximumUSTARSize {
		return header, archiveError("USTAR member name or size is outside the canonical profile")
	}
	for index := 0; index < len(name); index++ {
		if name[index] == 0 || name[index] > 0x7f {
			return header, archiveError("USTAR member name is not canonical ASCII")
		}
	}
	copy(header[headerNameOffset:headerNameOffset+headerNameLength], name)
	if !encodeOctal(header[headerModeOffset:headerModeOffset+headerNumericLength], uint64(mode)) ||
		!encodeOctal(header[headerUIDOffset:headerUIDOffset+headerNumericLength], 0) ||
		!encodeOctal(header[headerGIDOffset:headerGIDOffset+headerNumericLength], 0) ||
		!encodeOctal(header[headerSizeOffset:headerSizeOffset+headerSizeLength], size) ||
		!encodeOctal(header[headerMTimeOffset:headerMTimeOffset+headerSizeLength], 0) ||
		!encodeOctal(header[headerDevMajorOffset:headerDevMajorOffset+headerNumericLength], 0) ||
		!encodeOctal(header[headerDevMinorOffset:headerDevMinorOffset+headerNumericLength], 0) {
		return header, archiveError("USTAR numeric field exceeds the canonical octal profile")
	}
	for index := 0; index < headerChecksumLength; index++ {
		header[headerChecksumOffset+index] = ' '
	}
	header[headerTypeOffset] = '0'
	copy(header[headerMagicOffset:headerMagicOffset+6], []byte{'u', 's', 't', 'a', 'r', 0})
	copy(header[headerVersionOffset:headerVersionOffset+2], "00")
	var checksum uint64
	for _, value := range header {
		checksum += uint64(value)
	}
	if checksum >= 1<<18 {
		return header, archiveError("USTAR checksum exceeds six octal digits")
	}
	for index := 5; index >= 0; index-- {
		header[headerChecksumOffset+index] = byte('0' + checksum&7)
		checksum >>= 3
	}
	header[headerChecksumOffset+6] = 0
	header[headerChecksumOffset+7] = ' '
	return header, nil
}

func parseCanonicalHeader(header [TarBlockBytes]byte, name string, mode uint32) (uint64, error) {
	sizeField := header[headerSizeOffset : headerSizeOffset+headerSizeLength]
	if sizeField[len(sizeField)-1] != 0 {
		return 0, archiveError("USTAR size terminator is invalid")
	}
	var size uint64
	for _, character := range sizeField[:len(sizeField)-1] {
		if character < '0' || character > '7' {
			return 0, archiveError("USTAR size is not canonical unsigned octal")
		}
		size = size<<3 | uint64(character-'0')
	}
	expected, err := canonicalHeader(name, mode, size)
	if err != nil || header != expected {
		return 0, archiveError("USTAR header bytes are not canonical")
	}
	return size, nil
}

func encodeOctal(destination []byte, value uint64) bool {
	if len(destination) < 2 {
		return false
	}
	for index := len(destination) - 2; index >= 0; index-- {
		destination[index] = byte('0' + value&7)
		value >>= 3
	}
	if value != 0 {
		return false
	}
	destination[len(destination)-1] = 0
	return true
}
