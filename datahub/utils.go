package datahub

import (
	"crypto/md5"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"io"
	"net"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
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

func calculateMD5(input string) (string, error) {
	hasher := md5.New()
	_, err := io.WriteString(hasher, input)
	if err != nil {
		return "", err
	}

	hashBytes := hasher.Sum(nil)
	return fmt.Sprintf("%x", hashBytes), nil
}

func getHostIP() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}

	ips, err := net.LookupIP(hostname)
	if err != nil {
		return "", err
	}

	for _, ip := range ips {
		if ip.To4() != nil && !ip.IsLoopback() {
			return ip.String(), nil
		}
	}

	return "", fmt.Errorf("cannot get host ip")
}

func withRecover(key string, fn func()) {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("%s panic, err:%v", key, err)
		}
		time.Sleep(time.Second)
	}()

	fn()
}
