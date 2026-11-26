package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cedar2025/xboard-node/internal/controlplane"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
)

type batch5Certificate struct {
	certificateManager
	pending   atomic.Uint64
	confirmed chan uint64
	tls       kernel.TLSCert
}

func (c *batch5Certificate) TLSCert() kernel.TLSCert { return c.tls }
func (c *batch5Certificate) PendingRenewal() uint64  { return c.pending.Load() }
func (c *batch5Certificate) ConfirmRenewal(version uint64) {
	c.pending.CompareAndSwap(version, 0)
	if c.confirmed != nil {
		c.confirmed <- version
	}
}

type batch5Source struct {
	controlplane.Source
	polled chan struct{}
}

func (s batch5Source) SupportsPolling() bool { return s.polled != nil }
func (s batch5Source) Poll(context.Context) (controlplane.Snapshot, error) {
	close(s.polled)
	return controlplane.Snapshot{}, errors.New("test poll unavailable")
}

type batch5Sink struct{ controlplane.Sink }

func (batch5Sink) SupportsReporting() bool { return false }

type batch5PushClient struct{ controlplane.PushClient }

func (batch5PushClient) IsConnected() bool { return true }

type batch5TLSKernel struct {
	*fakeKernel
	attempts chan kernel.TLSCert
	result   chan error
}

func (k *batch5TLSKernel) Start(_ *model.NodeSpec, _ []model.UserSpec, tls kernel.TLSCert) error {
	k.attempts <- tls
	return <-k.result
}

func TestBatch5FailedPollPreservesRenewal(t *testing.T) {
	s := newTestService(&fakeKernel{})
	cert := &batch5Certificate{}
	cert.pending.Store(1)
	s.cert = cert
	polled := make(chan struct{})
	s.source = batch5Source{polled: polled}
	s.pullViaAPIAsync(context.Background())
	select {
	case <-polled:
	case <-time.After(2 * time.Second):
		t.Fatal("poll was never attempted")
	}
	if cert.PendingRenewal() != 1 {
		t.Fatal("poll consumed the pending renewal")
	}
}

func TestBatch5WebSocketRenewalRetriesUntilPEMApplied(t *testing.T) {
	s := newTestService(&fakeKernel{})
	k := &batch5TLSKernel{
		fakeKernel: &fakeKernel{running: true},
		attempts:   make(chan kernel.TLSCert, 4),
		result:     make(chan error, 4),
	}
	s.kernel = k
	s.source, s.sink, s.wsClient = batch5Source{}, batch5Sink{}, batch5PushClient{}
	s.lastConfig = &model.NodeSpec{Protocol: "vless", ServerPort: 443, TLS: 1}
	s.lastUsers = []model.UserSpec{{ID: 1}}
	cert := &batch5Certificate{tls: kernel.TLSCert{CertPEM: []byte("new cert"), KeyPEM: []byte("new key")}, confirmed: make(chan uint64, 4)}
	cert.pending.Store(42)
	s.cert = cert

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	go func() { done <- s.runEvents(ctx, ticker, ticker, ticker, ticker, ticker) }()
	defer func() {
		cancel()
		close(k.result)
		<-done
	}()
	for _, startErr := range []error{errors.New("test restart failed"), nil} {
		select {
		case tls := <-k.attempts:
			if cert.PendingRenewal() != 42 || string(tls.CertPEM) != "new cert" || string(tls.KeyPEM) != "new key" {
				t.Fatalf("kernel start got wrong pending certificate: %+v", tls)
			}
			k.result <- startErr
		case <-time.After(3500 * time.Millisecond):
			t.Fatal("WS event loop did not attempt certificate restart")
		}
	}
	select {
	case version := <-cert.confirmed:
		if version != 42 || cert.PendingRenewal() != 0 {
			t.Fatalf("confirmed renewal %d, pending %d", version, cert.PendingRenewal())
		}
	case <-time.After(time.Second):
		t.Fatal("successful PEM application did not confirm renewal")
	}
}
