package relay

import (
	"context"
	"net"
	"sync"

	"github.com/go-gost/core/handler"
	"github.com/go-gost/core/service"
	xconfig "github.com/go-gost/x/config"
	serviceparser "github.com/go-gost/x/config/parsing/service"
	"github.com/go-gost/x/handler/forward/local"
	"github.com/go-gost/x/registry"
)

// runtimeFlows owns the cancellation and completion of one rule generation's
// forwarding handlers. Gost's Service.Close only stops its listener.
type runtimeFlows struct {
	mu      sync.Mutex
	active  map[net.Conn]context.CancelFunc
	done    sync.WaitGroup
	stopped bool
}

func (f *runtimeFlows) idle() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.active) == 0
}

func (f *runtimeFlows) terminate() {
	f.mu.Lock()
	f.stopped = true
	for conn, cancel := range f.active {
		// Gost's forwarding pipe closes both endpoints and joins both
		// copy loops on cancellation. Close ingress immediately as well.
		cancel()
		_ = conn.Close()
	}
	f.mu.Unlock()
	f.done.Wait()
}

type trackedHandler struct {
	handler.Handler
	handler.Forwarder
	flows *runtimeFlows
}

func (h *trackedHandler) Handle(ctx context.Context, conn net.Conn, opts ...handler.HandleOption) error {
	f := h.flows
	f.mu.Lock()
	if f.stopped {
		f.mu.Unlock()
		return conn.Close()
	}
	ctx, cancel := context.WithCancel(ctx)
	if f.active == nil {
		f.active = make(map[net.Conn]context.CancelFunc)
	}
	f.active[conn] = cancel
	f.done.Add(1)
	f.mu.Unlock()

	defer func() {
		cancel()
		f.mu.Lock()
		delete(f.active, conn)
		f.mu.Unlock()
		f.done.Done()
	}()
	return h.Handler.Handle(ctx, conn, opts...)
}

func parseTrackedService(cfg *xconfig.ServiceConfig, flows *runtimeFlows) (service.Service, error) {
	// A factory scoped to this parse preserves Gost's parser, listener
	// limiters and stats wrappers. The constructed handler owns the tracker.
	name := cfg.Name + "-tracked"
	factory := func(opts ...handler.Option) handler.Handler {
		h := local.NewHandler(opts...)
		return &trackedHandler{Handler: h, Forwarder: h.(handler.Forwarder), flows: flows}
	}
	if err := registry.HandlerRegistry().Register(name, factory); err != nil {
		return nil, err
	}
	defer registry.HandlerRegistry().Unregister(name)
	cfg.Handler.Type = name
	return serviceparser.ParseService(cfg)
}
