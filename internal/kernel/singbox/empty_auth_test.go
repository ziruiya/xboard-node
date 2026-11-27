package singbox

import (
	"bufio"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	singM "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/protocol/socks"
	"github.com/sagernet/sing/protocol/socks/socks5"

	"github.com/cedar2025/xboard-node/internal/config"
	"github.com/cedar2025/xboard-node/internal/kernel"
	"github.com/cedar2025/xboard-node/internal/model"
)

func TestEmptyAuthInbound(t *testing.T) {
	for _, protocol := range []string{"socks", "http"} {
		t.Run(protocol, func(t *testing.T) {
			echo, err := net.Listen("tcp4", "127.0.0.1:0")
			if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
				t.Skipf("loopback socket unavailable: %v", err)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer echo.Close()
			go func() {
				for {
					conn, err := echo.Accept()
					if err != nil {
						return
					}
					go func() {
						defer conn.Close()
						_, _ = io.Copy(conn, conn)
					}()
				}
			}()

			reserved, err := net.Listen("tcp6", "[::]:0")
			if err != nil {
				t.Fatal(err)
			}
			port := reserved.Addr().(*net.TCPAddr).Port
			reserved.Close()
			address := fmt.Sprintf("127.0.0.1:%d", port)
			spec := model.NodeSpec{
				Protocol:   protocol,
				ServerPort: port,
				CustomRoutes: []map[string]any{
					// Allow only the local echo target through the default private-IP block.
					{"ip_cidr": []string{"127.0.0.1/32"}, "port": []int{echo.Addr().(*net.TCPAddr).Port}, "outbound": "direct"},
				},
			}
			users := []model.UserSpec{{ID: 1, UUID: "local-test-user"}}
			username := users[0].UUID
			s := New(config.KernelConfig{LogLevel: "warn"})
			s.platform = batch5Platform{}
			if err := s.Start(&spec, nil, kernel.TLSCert{}); err != nil {
				t.Fatal(err)
			}
			defer s.Stop()
			instance := s.box

			connect := func(name, password string) (net.Conn, error) {
				return connectAuthTestProxy(protocol, address, echo.Addr().String(), name, password)
			}
			assertClosed := func(t *testing.T, state string) {
				t.Helper()
				for _, credentials := range [][2]string{{"", ""}, {username, "wrong"}, {username, username}} {
					conn, err := connect(credentials[0], credentials[1])
					if conn != nil {
						conn.Close()
					}
					if !errors.Is(err, syscall.ECONNREFUSED) {
						t.Fatalf("%s: expected closed listener for credentials %q, got %v", state, credentials, err)
					}
				}
			}
			assertAuthenticated := func(t *testing.T) net.Conn {
				t.Helper()
				for _, credentials := range [][2]string{{"", ""}, {username, "wrong"}} {
					conn, err := connect(credentials[0], credentials[1])
					if conn != nil {
						conn.Close()
					}
					want := "HTTP CONNECT status 407"
					if protocol == "socks" {
						want = "incorrect user name or password"
						if credentials[0] == "" {
							want = "unsupported auth method: 255"
						}
					}
					if err == nil || !strings.Contains(err.Error(), want) {
						t.Fatalf("expected authentication rejection for %q, got %v", credentials, err)
					}
				}
				conn, err := connect(username, username)
				if err != nil {
					t.Fatal(err)
				}
				if err := authTestEcho(conn, "before clear"); err != nil {
					conn.Close()
					t.Fatal(err)
				}
				return conn
			}

			assertClosed(t, "empty startup")
			if _, err := s.AddUsers(users); err != nil {
				t.Fatal(err)
			}
			for _, clear := range []struct {
				name string
				run  func() error
			}{
				{"UpdateUsers", func() error { _, _, err := s.UpdateUsers(nil); return err }},
				{"RemoveUsers", func() error { _, err := s.RemoveUsers(users); return err }},
				{"Reload", func() error { return s.Reload(&spec, nil, kernel.TLSCert{}) }},
			} {
				t.Run(clear.name, func(t *testing.T) {
					existing := assertAuthenticated(t)
					defer existing.Close()
					if err := clear.run(); err != nil {
						t.Fatal(err)
					}
					assertClosed(t, "after clear")
					var b [1]byte
					_ = existing.SetReadDeadline(time.Now().Add(time.Second))
					n, err := existing.Read(b[:])
					var timeout net.Error
					if n != 0 || err == nil || (errors.As(err, &timeout) && timeout.Timeout()) {
						t.Fatalf("established authenticated tunnel survived revocation: read %d, error %v", n, err)
					}
				})
				if t.Failed() {
					return
				}
				if _, _, err := s.UpdateUsers(users); err != nil {
					t.Fatal(err)
				}
				if s.box != instance {
					t.Fatal("restoring users replaced the sing-box instance")
				}
				assertAuthenticated(t).Close()
			}
		})
	}
}

func connectAuthTestProxy(protocol, address, target, username, password string) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if protocol == "socks" {
		var response socks5.Response
		response, err = socks.ClientHandshake5(conn, socks5.CommandConnect, singM.ParseSocksaddr(target), username, password)
		if err == nil && response.ReplyCode != socks5.ReplyCodeSuccess {
			err = fmt.Errorf("SOCKS CONNECT reply %d", response.ReplyCode)
		}
	} else {
		request := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n", target, target)
		if username != "" {
			request += "Proxy-Authorization: Basic " + base64.URLEncoding.EncodeToString([]byte(username+":"+password)) + "\r\n"
		}
		if _, err = io.WriteString(conn, request+"\r\n"); err == nil {
			var response *http.Response
			response, err = http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodConnect})
			if err == nil && response.StatusCode != http.StatusOK {
				err = fmt.Errorf("HTTP CONNECT status %d", response.StatusCode)
			}
		}
	}
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func authTestEcho(conn net.Conn, payload string) error {
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.WriteString(conn, payload); err != nil {
		return err
	}
	reply := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, reply); err != nil {
		return err
	}
	if string(reply) != payload {
		return fmt.Errorf("echo got %q, want %q", reply, payload)
	}
	return nil
}
