package datahub

import (
	"hash/crc32"
	"hash/fnv"
)

func calculateCrc32(buf []byte) uint32 {
	table := crc32.MakeTable(crc32.Castagnoli)
	return crc32.Checksum(buf, table)
}

func calculateHashCode(input string) (uint32, error) {
	fnv32 := fnv.New32a()
	_, err := fnv32.Write([]byte(input))
	if err != nil {
		return 0, err
	}
	return fnv32.Sum32(), nil
}
