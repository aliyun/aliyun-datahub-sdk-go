package datahub

import (
	"bytes"
	"compress/zlib"
	"io"

	"github.com/pierrec/lz4"

	"github.com/klauspost/compress/zstd"
)

// compress type
type CompressorType string

const (
	NOCOMPRESS CompressorType = ""
	LZ4        CompressorType = "lz4"
	DEFLATE    CompressorType = "deflate"
	ZLIB       CompressorType = "zlib"
	ZSTD       CompressorType = "zstd"
)

// validate that the type is valid
func validateCompressorType(ct CompressorType) bool {
	switch ct {
	case NOCOMPRESS, LZ4, DEFLATE, ZLIB, ZSTD:
		return true
	}
	return false
}

func getCompressTypeFromValue(value int) CompressorType {
	switch value {
	case 0:
		return NOCOMPRESS
	case 1:
		return DEFLATE
	case 2:
		return LZ4
	case 3:
		return ZLIB
	case 4:
		return ZSTD
	default:
		return NOCOMPRESS
	}
}

func (ct *CompressorType) String() string {
	return string(*ct)
}

func (ct *CompressorType) toValue() int {
	switch *ct {
	case NOCOMPRESS:
		return 0
	case DEFLATE:
		return 1
	case LZ4:
		return 2
	case ZLIB:
		return 3
	case ZSTD:
		return 4
	default:
		return 0
	}
}

// Compressor is a interface for the compress
type compressor interface {
	Compress(data []byte) ([]byte, error)
	DeCompress(data []byte, rawSize int64) ([]byte, error)
}

type lz4Compressor struct {
}

func (lc *lz4Compressor) Compress(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}

	buf := make([]byte, lz4.CompressBlockBound(len(data)))
	ht := make([]int, 64<<10)
	n, err := lz4.CompressBlock(data, buf, ht)
	if err != nil {
		return nil, err
	}

	if n == 0 {
		return data, nil
	}

	return buf[:n], nil
}

func (lc *lz4Compressor) DeCompress(data []byte, rawSize int64) ([]byte, error) {
	// Allocated a very large buffer for decompression.
	buf := make([]byte, rawSize)
	_, err := lz4.UncompressBlock(data, buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

type deflateCompressor struct {
}

func (dc *deflateCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (dc *deflateCompressor) DeCompress(data []byte, rawSize int64) ([]byte, error) {
	b := bytes.NewReader(data)
	var buf bytes.Buffer
	r, _ := zlib.NewReader(b)
	if _, err := io.Copy(&buf, r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type zlibCompressor struct {
}

func (zc *zlibCompressor) Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (zc *zlibCompressor) DeCompress(data []byte, rawSize int64) ([]byte, error) {
	b := bytes.NewReader(data)
	var buf bytes.Buffer
	r, _ := zlib.NewReader(b)
	if _, err := io.Copy(&buf, r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type zstdCompressor struct {
}

func (zc *zstdCompressor) Compress(data []byte) ([]byte, error) {

	writer, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	defer writer.Close()

	return writer.EncodeAll(data, make([]byte, 0, len(data))), nil
}

func (zc *zstdCompressor) DeCompress(data []byte, rawSize int64) ([]byte, error) {
	reader, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	dData := make([]byte, 0, rawSize)
	dData, err = reader.DecodeAll(data, dData)
	if err != nil {
		return nil, err
	}

	return dData, nil
}

var compressorMap map[CompressorType]compressor = map[CompressorType]compressor{
	LZ4:     &lz4Compressor{},
	DEFLATE: &deflateCompressor{},
	ZLIB:    &zlibCompressor{},
	ZSTD:    &zstdCompressor{},
}

func newCompressor(c CompressorType) compressor {
	switch CompressorType(c) {
	case LZ4:
		return &lz4Compressor{}
	case DEFLATE:
		return &deflateCompressor{}
	case ZLIB:
		return &zlibCompressor{}
	case ZSTD:
		return &zstdCompressor{}
	default:
		return nil
	}
}

func getCompressor(c CompressorType) compressor {
	if c == NOCOMPRESS {
		return nil
	}
	ret, ok := compressorMap[c]
	if !ok {
		com := newCompressor(c)
		compressorMap[c] = com
	}
	return ret
}
