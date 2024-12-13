package datahub

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetCompressTypeFromValue(t *testing.T) {
	ret := getCompressTypeFromValue(0)
	assert.Equal(t, NOCOMPRESS, ret)

	ret = getCompressTypeFromValue(1)
	assert.Equal(t, DEFLATE, ret)

	ret = getCompressTypeFromValue(2)
	assert.Equal(t, LZ4, ret)

	ret = getCompressTypeFromValue(3)
	assert.Equal(t, ZLIB, ret)

	ret = getCompressTypeFromValue(4)
	assert.Equal(t, NOCOMPRESS, ret)

	ret = getCompressTypeFromValue(-1)
	assert.Equal(t, NOCOMPRESS, ret)
}

func TestInvalidLz4(t *testing.T) {
	compressor := lz4Compressor{}

	data := []byte("hello") // len=5
	cData, err := compressor.Compress(data)
	assert.Nil(t, err)
	assert.Equal(t, 6, len(cData))
}
