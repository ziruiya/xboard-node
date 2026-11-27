package panel

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

type batch2bTransport func(*http.Request) (*http.Response, error)

func (f batch2bTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestBatch2BRelayTrafficRequestUsesShutdownContext(t *testing.T) {
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &Client{
		baseURL: "http://127.0.0.1",
		httpClient: &http.Client{Transport: batch2bTransport(func(req *http.Request) (*http.Response, error) {
			close(started)
			<-req.Context().Done()
			return nil, req.Context().Err()
		})},
	}
	errs := make(chan error, 1)
	go func() {
		_, err := client.PostRelayTrafficContext(ctx, []RelayTrafficBatch{{RuleCode: "rule", BootID: "test", Seq: 1}})
		errs <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not reach transport")
	}
	cancel()
	select {
	case err := <-errs:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("request returned %v, want cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP request did not stop on cancellation")
	}
}
