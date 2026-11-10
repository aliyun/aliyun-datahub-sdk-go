package datahub

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClientInitRequestTimeout(t *testing.T) {
	for _, kind := range []string{"producer", "async", "consumer"} {
		t.Run(kind, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-r.Context().Done():
				case <-time.After(time.Second):
					w.WriteHeader(http.StatusInternalServerError)
				}
			}))
			defer server.Close()
			base := BaseConfig{Endpoint: server.URL, Project: "test", Topic: "test",
				Account: NewAliyunAccount("ak", "sk"), RequestTimeout: 30 * time.Millisecond}
			var init func() error
			switch kind {
			case "producer":
				cfg := NewProducerConfig()
				cfg.BaseConfig = base
				init = NewProducer(cfg).Init
			case "async":
				cfg := NewProducerConfig()
				cfg.BaseConfig = base
				producer := NewAsyncProducer(cfg).(*asyncProducerImpl)
				defer producer.buffer.close()
				init = producer.Init
			case "consumer":
				cfg := NewConsumerConfig()
				cfg.BaseConfig = base
				init = NewConsumer(cfg).Init
			}
			err := init()
			require.Error(t, err)
			require.Contains(t, err.Error(), "Client.Timeout")
		})
	}
}

func TestBaseConfigRequestTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	cfg := BaseConfig{RequestTimeout: 30 * time.Millisecond, ConnectTimeout: time.Second}
	client := cfg.newClientConfig().HttpClient
	resp, err := client.Get(server.URL)
	require.NoError(t, err)
	defer resp.Body.Close()
	var b [1]byte
	_, err = resp.Body.Read(b[:])
	require.Error(t, err)
	require.Contains(t, err.Error(), "Client.Timeout")
}

func TestBaseConfigTimeoutDefaults(t *testing.T) {
	for _, cfg := range []BaseConfig{{}, NewProducerConfig().BaseConfig, NewConsumerConfig().BaseConfig} {
		client := cfg.newClientConfig().HttpClient
		require.Zero(t, client.Timeout)
		transport := client.Transport.(*http.Transport)
		require.Equal(t, 100*time.Second, transport.ResponseHeaderTimeout)
		require.Equal(t, 10*time.Second, transport.TLSHandshakeTimeout)
	}
}

func TestBaseConfigConnectTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	for _, timeout := range []time.Duration{0, time.Second, time.Nanosecond} {
		cfg := BaseConfig{ConnectTimeout: timeout}
		transport := cfg.newClientConfig().HttpClient.Transport.(*http.Transport)
		conn, err := transport.DialContext(context.Background(), "tcp", listener.Addr().String())
		if conn != nil {
			conn.Close()
		}
		if timeout == time.Nanosecond {
			require.Error(t, err)
			var netErr net.Error
			require.ErrorAs(t, err, &netErr)
			require.True(t, netErr.Timeout())
		} else {
			require.NoError(t, err)
		}
	}
}
