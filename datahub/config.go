package datahub

import (
	"fmt"
	"math"
	"net/http"
	"os"
	"runtime"
	"time"
)

type Protocol int

const (
	unknownProtocol Protocol = iota
	Protobuf
	Batch
)

type Config struct {
	UserAgent      string
	CompressorType CompressorType
	Protocol       Protocol
	HttpClient     *http.Client
}

func NewDefaultConfig() *Config {
	return &Config{
		UserAgent:      DefaultUserAgent(),
		CompressorType: ZSTD,
		Protocol:       Batch,
		HttpClient:     DefaultHttpClient(),
	}
}

// DefaultHttpClient returns a default HTTP client with sensible values.
func DefaultHttpClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext:           TraceDialContext(10 * time.Second),
			Proxy:                 http.ProxyFromEnvironment,
			MaxIdleConns:          math.MaxInt32,
			MaxIdleConnsPerHost:   math.MaxInt32,
			MaxConnsPerHost:       math.MaxInt32,
			IdleConnTimeout:       30 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			ResponseHeaderTimeout: 100 * time.Second,
		},
	}
}

// DefaultUserAgent returns a default user agent
func DefaultUserAgent() string {
	hostIp, _ := getHostIP()
	return fmt.Sprintf("godatahub/%s %s@%s#%s#%d", DATAHUB_SDK_VERSION, runtime.Version(), runtime.GOOS, hostIp, os.Getpid())
}

func defaultClientAgent() string {
	hostIp, _ := getHostIP()
	return fmt.Sprintf("goclient/%s %s@%s#%s#%d", DATAHUB_SDK_VERSION, runtime.Version(), runtime.GOOS, hostIp, os.Getpid())
}
