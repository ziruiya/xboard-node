package singbox

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing/common/bufio"
	singM "github.com/sagernet/sing/common/metadata"
)

func TestBatch1ARevokedUUIDClosesEstablishedTCPAndUDP(t *testing.T) {
	tracker := NewConnTracker(0)
	tracker.SetUserMap(map[string]int{"revoked": 1, "retained": 2})
	ctx := context.Background()

	tcp, tcpPeer := net.Pipe()
	udp, udpPeer := net.Pipe()
	keep, keepPeer := net.Pipe()
	for _, conn := range []net.Conn{tcp, tcpPeer, udp, udpPeer, keep, keepPeer} {
		t.Cleanup(func() { conn.Close() })
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	}
	revoked := testInboundContext("revoked", "1.1.1.1")
	tcpFlow := tracker.RoutedConnection(ctx, tcp, revoked, nil, nil)
	udpFlow := tracker.RoutedPacketConnection(ctx, bufio.NewUnbindPacketConnWithAddr(udp, singM.ParseSocksaddr("9.9.9.9:53")), revoked, nil, nil)
	keepFlow := tracker.RoutedConnection(ctx, keep, testInboundContext("retained", "2.2.2.2"), nil, nil)

	assertForward := func(write func() error, peer net.Conn) {
		t.Helper()
		done := make(chan error, 1)
		go func() { done <- write() }()
		var b [1]byte
		if _, err := io.ReadFull(peer, b[:]); err != nil || b[0] != 42 {
			t.Fatalf("expected established data flow, read %v, error %v", b, err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	assertForward(func() error { _, err := tcpFlow.Write([]byte{42}); return err }, tcpPeer)
	assertForward(func() error {
		_, err := bufio.NewNetPacketConn(udpFlow).WriteTo([]byte{42}, &net.UDPAddr{IP: net.IPv4(9, 9, 9, 9), Port: 53})
		return err
	}, udpPeer)

	tracker.SetUserMap(map[string]int{"retained": 2})
	for _, peer := range []net.Conn{tcpPeer, udpPeer} {
		var b [1]byte
		if n, err := peer.Read(b[:]); n != 0 || !errors.Is(err, io.EOF) {
			t.Fatalf("revoked transport still alive: read %d, err %v", n, err)
		}
	}
	assertForward(func() error { _, err := keepFlow.Write([]byte{42}); return err }, keepPeer)
	if got := tracker.CloseByUUID("retained"); got != 1 {
		t.Fatalf("CloseByUUID closed %d connections, want 1", got)
	}
	var b [1]byte
	if n, err := keepPeer.Read(b[:]); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("CloseByUUID did not terminate the retained connection: %d, %v", n, err)
	}
}
