package cert

// SB-ER-010 (agent half) regression tests: a bare IP written as the
// cert "domain" by the panel's dedicated-machine provisioner must
// take the ACME IP path (shortlived profile, HTTP-01, no DNS solver)
// instead of the doomed DNS-01 path.

import (
	"testing"

	"github.com/caddyserver/certmagic"

	"github.com/cedar2025/xboard-node/internal/config"
)

func TestIsIPLiteral(t *testing.T) {
	for _, s := range []string{"203.0.113.10", "2001:db8::1", " 198.51.100.7 "} {
		if !isIPLiteral(s) {
			t.Errorf("isIPLiteral(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"example.com", "", "999.1.1.1", "node.example.org"} {
		if isIPLiteral(s) {
			t.Errorf("isIPLiteral(%q) = true, want false", s)
		}
	}
}

func TestBuildACMEIssuerIPDomainUsesShortlivedProfile(t *testing.T) {
	// The exact panel shape for a dedicated machine: dns mode, bare
	// IP as domain, possibly without any DNS credentials at all.
	m := NewManager(config.CertConfig{
		CertMode: "dns", Domain: "203.0.113.10", Email: "ops@example.com",
	})
	iss := m.buildACMEIssuer(nil)
	if iss.Profile != "shortlived" {
		t.Fatalf("Profile = %q, want \"shortlived\" (LE IP certificates require it)", iss.Profile)
	}
	if iss.DNS01Solver != nil {
		t.Fatal("DNS01Solver must be nil for IP issuance — no DNS-01 exists for IP identifiers")
	}
	if iss.DisableHTTPChallenge {
		t.Fatal("HTTP-01 must stay enabled for IP issuance")
	}
	if !iss.DisableTLSALPNChallenge {
		t.Fatal("TLS-ALPN must stay disabled (not offered for IP certs)")
	}
	if iss.AltHTTPPort != 80 {
		t.Fatalf("AltHTTPPort = %d, want default 80", iss.AltHTTPPort)
	}
}

func TestBuildACMEIssuerDNSDomainKeepsDNS01(t *testing.T) {
	m := NewManager(config.CertConfig{
		CertMode: "dns", Domain: "node.example.com", Email: "ops@example.com",
		DNSProvider: "cloudflare",
	})
	solver := &certmagic.DNS01Solver{}
	iss := m.buildACMEIssuer(solver)
	if iss.DNS01Solver != solver {
		t.Fatal("real domains in dns mode must keep the DNS-01 solver")
	}
	if !iss.DisableHTTPChallenge || !iss.DisableTLSALPNChallenge {
		t.Fatal("DNS-01 plan must disable HTTP and TLS-ALPN challenges")
	}
	if iss.Profile != "" {
		t.Fatalf("Profile = %q, want empty for ordinary domain issuance", iss.Profile)
	}
}

func TestBuildACMEIssuerHTTPMode(t *testing.T) {
	m := NewManager(config.CertConfig{
		CertMode: "http", Domain: "node.example.com", Email: "ops@example.com", HTTPPort: 8080,
	})
	iss := m.buildACMEIssuer(nil)
	if iss.Profile != "" || iss.DNS01Solver != nil {
		t.Fatalf("http mode issuer = %+v, want plain HTTP-01 plan", iss)
	}
	if iss.AltHTTPPort != 8080 {
		t.Fatalf("AltHTTPPort = %d, want configured 8080", iss.AltHTTPPort)
	}
}
