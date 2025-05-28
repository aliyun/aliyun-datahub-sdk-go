package datahub

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	batchRecordHeaderSize = 36
	avroDataType          = 2
)

var (
	batchMagicBytes = []byte{'D', 'H', 'U', 'B'}
	batchMagicNum   = int32(binary.LittleEndian.Uint32(batchMagicBytes))
)

func calculateCrc32(buf []byte) uint32 {
	table := crc32.MakeTable(crc32.Castagnoli)
	return crc32.Checksum(buf, table)
}

type respMeta struct {
	cursor     string
	nextCursor string
	sequence   int64
	systemTime int64
	serial     int64
}

type batchHeader struct {
	magic         int32
	version       int32
	length        int32
	rawSize       int32
	crc32         uint32
	attribute     int16
	dataType      int16
	schemaVersion int32
	dataOffset    int32
	recordCount   int32
}

func setCompressType(attrbuite int16, cType CompressorType) int16 {
	return int16((uint16(attrbuite) & uint16(0xfffc)) | uint16(cType.toValue()))
}

func getCompressType(attribute int16) CompressorType {
	val := attribute & 0x0003
	return getCompressTypeFromValue(int(val))
}

func (serializer *batchSerializer) serializeBatchHeader(bHeader *batchHeader) []byte {
	buf := make([]byte, batchRecordHeaderSize)
	copy(buf, batchMagicBytes)
	binary.LittleEndian.PutUint32(buf[4:], uint32(bHeader.version))
	binary.LittleEndian.PutUint32(buf[8:], uint32(bHeader.length))
	binary.LittleEndian.PutUint32(buf[12:], uint32(bHeader.rawSize))
	binary.LittleEndian.PutUint32(buf[16:], uint32(bHeader.crc32))
	binary.LittleEndian.PutUint16(buf[20:], uint16(bHeader.attribute))
	binary.LittleEndian.PutUint16(buf[22:], uint16(bHeader.dataType))
	binary.LittleEndian.PutUint32(buf[24:], uint32(bHeader.schemaVersion))
	binary.LittleEndian.PutUint32(buf[28:], uint32(bHeader.dataOffset))
	binary.LittleEndian.PutUint32(buf[32:], uint32(bHeader.recordCount))
	return buf
}

type batchSerializer struct {
	project    string
	topic      string
	cType      CompressorType
	serializer dataSerializer
	cache      *topicSchemaCache
}

func newBatchSerializer(project, topic string, schemaCache *topicSchemaCache, cType CompressorType) *batchSerializer {
	return &batchSerializer{
		project:    project,
		topic:      topic,
		cType:      cType,
		serializer: newDataSerializer(schemaCache),
		cache:      schemaCache,
	}
}

func (bs *batchSerializer) serialize(records []IRecord) ([]byte, error) {
	schemaVersionId, err := bs.getSchemaVersion(records[0])
	if err != nil {
		return nil, err
	}

	rawBuf, err := bs.serializer.serialize(records)
	if err != nil {
		return nil, err
	}

	var attrbuite int16 = 0
	buf, err := bs.compress(rawBuf, &attrbuite)
	if err != nil {
		return nil, err
	}

	header := batchHeader{
		magic:         batchMagicNum,
		version:       1,
		length:        int32(len(buf)) + batchRecordHeaderSize,
		rawSize:       int32(len(rawBuf)),
		crc32:         calculateCrc32(buf),
		attribute:     attrbuite,
		dataType:      avroDataType,
		schemaVersion: schemaVersionId,
		dataOffset:    batchRecordHeaderSize,
		recordCount:   int32(len(records)),
	}

	headerBuf := bs.serializeBatchHeader(&header)

	res := bytes.NewBuffer(headerBuf)
	_, err = res.Write(buf)
	if err != nil {
		return nil, err
	}

	return res.Bytes(), nil
}

func (bs *batchSerializer) getSchemaVersion(record IRecord) (int32, error) {
	var dhSchema *RecordSchema = nil
	tupleRecord, ok := record.(*TupleRecord)
	if ok {
		dhSchema = tupleRecord.RecordSchema
	}

	versionId := bs.cache.getVersionIdBySchema(dhSchema)
	if versionId == invalidSchemaVersionId {
		schemaStr := "nil"
		if dhSchema != nil {
			schemaStr = dhSchema.String()
		}
		return 0, fmt.Errorf("%s/%s schema not found, schema:%s", bs.project, bs.topic, schemaStr)
	}
	return int32(versionId), nil
}

func (bs *batchSerializer) compress(data []byte, attrbuite *int16) ([]byte, error) {
	*attrbuite = setCompressType(*attrbuite, bs.cType)
	compressor := getCompressor(bs.cType)
	if compressor != nil {
		cData, err := compressor.Compress(data)
		if err != nil {
			return nil, err
		}

		return cData, nil
	}

	return data, nil
}

type batchDeserializer struct {
	shardId      string
	deserializer dataDeserializer
}

func newBatchDeserializer(project, topic, shardId string, schemaCache *topicSchemaCache) *batchDeserializer {
	return &batchDeserializer{
		shardId:      shardId,
		deserializer: newDataDeserializer(schemaCache),
	}
}

func (bd *batchDeserializer) deserialize(data []byte, meta *respMeta) ([]IRecord, error) {
	header, err := parseBatchHeader(data)
	if err != nil {
		return nil, err
	}

	rawBuf, err := bd.decompress(data[batchRecordHeaderSize:], header)
	if err != nil {
		return nil, err
	}

	records, err := bd.deserializer.deserialize(rawBuf, header)
	if err != nil {
		return nil, err
	}

	for idx, record := range records {
		record.setMetaInfo(meta.sequence, meta.systemTime, meta.serial, idx, bd.shardId, meta.cursor, meta.nextCursor)
	}

	return records, nil
}

func (deserializer *batchDeserializer) decompress(data []byte, header *batchHeader) ([]byte, error) {
	cType := getCompressType(header.attribute)
	compressor := getCompressor(cType)
	if compressor == nil {
		return data, nil
	}

	buf, err := compressor.DeCompress(data, int64(header.rawSize))
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func parseBatchHeader(data []byte) (*batchHeader, error) {
	if len(data) < batchRecordHeaderSize {
		return nil, fmt.Errorf("read batch header fail, current length[%d] not enough", len(data))
	}

	header := &batchHeader{}
	header.magic = int32(binary.LittleEndian.Uint32(data[0:]))
	header.version = int32(binary.LittleEndian.Uint32(data[4:]))
	header.length = int32(binary.LittleEndian.Uint32(data[8:]))
	header.rawSize = int32(binary.LittleEndian.Uint32(data[12:]))
	header.crc32 = binary.LittleEndian.Uint32(data[16:])
	header.attribute = int16(binary.LittleEndian.Uint16(data[20:]))
	header.dataType = int16(binary.LittleEndian.Uint16(data[22:]))
	header.schemaVersion = int32(binary.LittleEndian.Uint32(data[24:]))
	header.dataOffset = int32(binary.LittleEndian.Uint32(data[28:]))
	header.recordCount = int32(binary.LittleEndian.Uint32(data[32:]))

	if header.magic != batchMagicNum {
		return nil, fmt.Errorf("check magic number fail")
	}

	if header.length != int32(len(data)) {
		return nil, fmt.Errorf("check payload length fail, expect:%d, real:%d", header.length, len(data))
	}

	if header.crc32 != 0 {
		calCrc := calculateCrc32(data[batchRecordHeaderSize:])
		if calCrc != header.crc32 {
			return nil, fmt.Errorf("check crc fail. expect:%d, real:%d", header.crc32, calCrc)
		}
	}

	if header.dataType != avroDataType {
		return nil, fmt.Errorf("only support avro data type, real:%d", header.dataType)
	}

	return header, nil
}
