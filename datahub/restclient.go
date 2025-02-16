package datahub

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	httpHeaderAcceptEncoding     = "Accept-Encoding"
	httpHeaderAuthorization      = "Authorization"
	httpHeadercacheControl       = "Cache-Control"
	httpHeaderChunked            = "chunked"
	httpHeaderClientVersion      = "x-datahub-client-version"
	httpHeaderContentDisposition = "Content-Disposition"
	httpHeaderContentEncoding    = "Content-Encoding"
	httpHeaderContentLength      = "Content-Length"
	httpHeaderContentMD5         = "Content-MD5"
	httpHeaderContentType        = "Content-Type"
	httpHeaderDate               = "Date"
	httpHeaderETAG               = "ETag"
	httpHeaderEXPIRES            = "Expires"
	httpHeaderHost               = "Host"
	httpHeaderlastModified       = "Last-Modified"
	httpHeaderLocation           = "Location"
	httpHeaderRange              = "Range"
	httpHeaderRawSize            = "x-datahub-content-raw-size"
	httpHeaderRequestAction      = "x-datahub-request-action"
	httpHeaderRequestId          = "x-datahub-request-id"
	httpHeaderSecurityToken      = "x-datahub-security-token"
	httpHeaderDwarfToken         = "x-datahub-dwarf-token"
	httpHeaderDwarfSign          = "x-datahub-dwarf-sign"
	httpHeaderTransferEncoding   = "Transfer-Encoding"
	httpHeaderUserAgent          = "User-Agent"
	httpHeaderConnectorMode      = "mode"
)

const (
	httpFilterQuery       = "filter"
	httpJsonContent       = "application/json"
	httpProtoContent      = "application/x-protobuf"
	httpProtoBatchContent = "application/x-binary"
	httpPublistContent    = "pub"
	httpSubscribeContent  = "sub"
)

const (
	datahubHeadersPrefix = "x-datahub-"
)

func init() {
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.TextFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File examples
	log.SetOutput(os.Stdout)

	// Only log the level severity or above.
	dev := strings.ToLower(os.Getenv("GODATAHUB_DEV"))
	switch dev {
	case "true":
		log.SetLevel(log.DebugLevel)
	default:
		log.SetLevel(log.WarnLevel)
	}
}

// DialContextFn was defined to make code more readable.
type DialContextFn func(ctx context.Context, network, address string) (net.Conn, error)

// TraceDialContext implements our own dialer in order to trace conn info.
func TraceDialContext(ctimeout time.Duration) DialContextFn {
	dialer := &net.Dialer{
		Timeout:   ctimeout,
		KeepAlive: ctimeout,
	}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
}

// RestClient rest客户端
type RestClient struct {
	// Endpoint datahub服务的endpint
	Endpoint string
	// Useragent user agent
	Useragent string
	// HttpClient http client
	HttpClient *http.Client
	// Account
	Account        Account
	CompressorType CompressorType
}

// NewRestClient create a new rest client
func NewRestClient(endpoint string, useragent string, httpClient *http.Client, account Account, cType CompressorType) *RestClient {
	endpoint = strings.TrimSuffix(endpoint, "/")
	return &RestClient{
		Endpoint:       endpoint,
		Useragent:      useragent,
		HttpClient:     httpClient,
		Account:        account,
		CompressorType: cType,
	}
}

type RequestParameter struct {
	Header map[string]string
	Query  map[string]string
}

// Get send HTTP Get method request
func (client *RestClient) Get(resource string, para *RequestParameter) ([]byte, *CommonResponseResult, error) {
	return client.request(http.MethodGet, resource, &EmptyRequest{}, para)
}

// Post send HTTP Post method request
func (client *RestClient) Post(resource string, model RequestModel, para *RequestParameter) ([]byte, *CommonResponseResult, error) {
	return client.request(http.MethodPost, resource, model, para)
}

// Put send HTTP Put method request
func (client *RestClient) Put(resource string, model RequestModel, para *RequestParameter) (interface{}, *CommonResponseResult, error) {
	return client.request(http.MethodPut, resource, model, para)
}

// Delete send HTTP Delete method request
func (client *RestClient) Delete(resource string, para *RequestParameter) (interface{}, *CommonResponseResult, error) {
	return client.request(http.MethodDelete, resource, &EmptyRequest{}, para)
}

func (client *RestClient) request(method, resource string, requestModel RequestModel, para *RequestParameter) ([]byte, *CommonResponseResult, error) {
	url := fmt.Sprintf("%s%s", client.Endpoint, resource)

	header := map[string]string{
		httpHeaderClientVersion: DATAHUB_CLIENT_VERSION,
		httpHeaderDate:          time.Now().UTC().Format(http.TimeFormat),
		httpHeaderUserAgent:     client.Useragent,
	}

	//serialization
	reqBody, err := requestModel.requestBodyEncode()
	if err != nil {
		return nil, nil, err
	}

	rawSize := len(reqBody)
	client.compressIfNeed(header, &reqBody)
	reqSize := len(reqBody)

	if client.Account.GetSecurityToken() != "" {
		header[httpHeaderSecurityToken] = client.Account.GetSecurityToken()
	}

	if credential, ok := client.Account.(*DwarfCredential); ok &&
		len(credential.DwarfToken) > 0 && len(credential.DwarfSign) > 0 {
		header[httpHeaderDwarfToken] = credential.DwarfToken
		header[httpHeaderDwarfSign] = credential.DwarfSign
	}

	req, err := http.NewRequest(method, url, bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, nil, err
	}

	if para != nil {
		for k, v := range para.Header {
			header[k] = v
		}

		query := req.URL.Query()
		for k, v := range para.Query {
			query.Add(k, v)
		}
		req.URL.RawQuery = query.Encode()
	}

	for k, v := range header {
		req.Header.Add(k, v)
	}

	client.buildSignature(&req.Header, method, req.URL.RequestURI())

	resp, err := client.HttpClient.Do(req)
	if err != nil {
		if strings.Contains(err.Error(), "EOF") {
			return nil, nil, NewServiceTemporaryUnavailableError(err.Error())
		}
		return nil, nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)

	if err != nil {
		return nil, nil, err
	}

	//decompress
	if err := client.decompress(&respBody, &resp.Header); err != nil {
		return nil, nil, err
	}

	//detect error
	respResult, err := newCommonResponseResult(resp.StatusCode, &resp.Header, respBody)
	if log.IsLevelEnabled(log.DebugLevel) {
		log.Debugf("request id: %s\nrequest url: %s\nrequest headers: %v\nrequest body: %s\nresponse headers: %v\nresponse body: %s",
			respResult.RequestId, url, req.Header, string(reqBody), resp.Header, string(respBody))
	}

	if err != nil {
		return nil, nil, err
	}

	respResult.RawSize = rawSize
	respResult.ReqSize = reqSize
	return respBody, respResult, nil
}

func (client *RestClient) buildSignature(header *http.Header, method, url string) {
	builder := make([]string, 0, 5)
	builder = append(builder, method)
	builder = append(builder, header.Get(httpHeaderContentType))
	builder = append(builder, header.Get(httpHeaderDate))

	headersToSign := make(map[string][]string)
	for k, v := range *header {
		lower := strings.ToLower(k)
		if strings.HasPrefix(lower, datahubHeadersPrefix) {
			headersToSign[lower] = v
		}
	}

	keys := make([]string, len(headersToSign))
	for k := range headersToSign {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		for _, v := range headersToSign[k] {
			builder = append(builder, fmt.Sprintf("%s:%s", k, v))
		}
	}

	builder = append(builder, url)
	canonString := strings.Join(builder, "\n")

	if log.IsLevelEnabled(log.DebugLevel) {
		log.Debugf("canonString: %s, accesskey: %s", canonString, client.Account.GetAccountKey())
	}

	hash := hmac.New(sha1.New, []byte(client.Account.GetAccountKey()))
	hash.Write([]byte(canonString))
	crypto := hash.Sum(nil)
	signature := base64.StdEncoding.EncodeToString(crypto)
	authorization := fmt.Sprintf("DATAHUB %s:%s", client.Account.GetAccountId(), signature)

	header.Add(httpHeaderAuthorization, authorization)
}

func (client *RestClient) compressIfNeed(header map[string]string, reqBody *[]byte) {
	if client.CompressorType == NOCOMPRESS {
		return
	}
	compressor := getCompressor(client.CompressorType)
	if compressor != nil {
		header[httpHeaderAcceptEncoding] = client.CompressorType.String()
		compressedReqBody, err := compressor.Compress(*reqBody)
		if err != nil {
			log.Warningf("compress failed, give up compression, error:%v", err)
		} else if len(compressedReqBody) > len(*reqBody) {
			if log.IsLevelEnabled(log.DebugLevel) {
				log.Debugf("compress invalid, give up compression, rawSize:%d, compressSize:%d",
					len(*reqBody), len(compressedReqBody))
			}
		} else {
			header[httpHeaderContentEncoding] = client.CompressorType.String()
			//header[httpHeaderAcceptEncoding] = client.CompressorType.String()
			header[httpHeaderRawSize] = strconv.Itoa(len(*reqBody))
			*reqBody = compressedReqBody
		}
	}
	header[httpHeaderContentLength] = strconv.Itoa(len(*reqBody))
}

func (client *RestClient) decompress(respBody *[]byte, header *http.Header) error {
	encoding := header.Get(httpHeaderContentEncoding)
	if encoding == "" {
		return nil
	}
	compressor := getCompressor(CompressorType(encoding))
	if compressor == nil {
		return fmt.Errorf("not support the compress mode %s ", encoding)
	}
	rawSize := header.Get(httpHeaderRawSize)
	//str convert to int64
	size, err := strconv.ParseInt(rawSize, 10, 64)
	if err != nil {
		return err
	}

	buf, err := compressor.DeCompress(*respBody, size)
	if err != nil {
		return err
	}
	*respBody = buf
	return nil
}
