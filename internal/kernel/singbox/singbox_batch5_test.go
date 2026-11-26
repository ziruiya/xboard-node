package singbox

import (
	"errors"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-tun"
	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
)

func batch5RunningSingBox(t *testing.T) (*SingBox, model.NodeSpec) {
	t.Helper()
	var spec model.NodeSpec
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	switch {
	case err == nil:
		spec = model.NodeSpec{Protocol: "vless", ServerPort: ln.Addr().(*net.TCPAddr).Port}
		ln.Close()
	case errors.Is(err, os.ErrPermission):
		// A box with no inbound exercises the same outbound manager and
		// router where opening a local socket is unavailable.
	default:
		t.Fatal(err)
	}
	s := New(config.KernelConfig{LogLevel: "warn"})
	s.platform = batch5Platform{}
	if err := s.Start(&spec, nil, kernel.TLSCert{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Stop)
	return s, spec
}

type batch5Platform struct{ adapter.PlatformInterface }

func (batch5Platform) Initialize(adapter.NetworkManager) error     { return nil }
func (batch5Platform) UsePlatformAutoDetectInterfaceControl() bool { return false }
func (batch5Platform) CreateDefaultInterfaceMonitor(logger.Logger) tun.DefaultInterfaceMonitor {
	return batch5InterfaceMonitor{}
}

type batch5InterfaceMonitor struct{ tun.DefaultInterfaceMonitor }

func (batch5InterfaceMonitor) Start() error { return nil }
func (batch5InterfaceMonitor) Close() error { return nil }
func (batch5InterfaceMonitor) RegisterCallback(tun.DefaultInterfaceUpdateCallback) *list.Element[tun.DefaultInterfaceUpdateCallback] {
	return nil
}

func TestBatch5ReloadChangesRunningOutboundManager(t *testing.T) {
	s, spec := batch5RunningSingBox(t)
	original := s.box
	spec.CustomOutbounds = []model.OutboundConfig{{Tag: "new-outbound", Protocol: "direct"}}
	if err := s.Reload(&spec, nil, kernel.TLSCert{}); err != nil {
		t.Fatal(err)
	}
	manager := service.FromContext[adapter.OutboundManager](s.ctx)
	outbound, ok := manager.Outbound("new-outbound")
	if !ok || outbound.Type() != "direct" || s.box == original {
		t.Fatal("reload returned success without replacing the real outbound manager")
	}
}

func TestBatch5ReloadSurfacesRouterUpdateError(t *testing.T) {
	s, spec := batch5RunningSingBox(t)
	spec.CustomRoutes = []map[string]any{{"ip_cidr": []string{"not-a-cidr"}, "outbound": "direct"}}
	if err := s.Reload(&spec, nil, kernel.TLSCert{}); err == nil || !strings.Contains(err.Error(), "reload sing-box routing") {
		t.Fatalf("expected an error from router.UpdateRules, got %v", err)
	}
}
