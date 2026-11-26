package relay

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"time"

	"github.com/cedar2025/xboard-node/internal/panel"
)

func resolveRelayTargets(ctx context.Context, desired map[string]panel.RelayRule, interfaceAddrs func() ([]net.Addr, error)) (map[string]string, error) {
	targets := make(map[string]string, len(desired))
	if len(desired) == 0 {
		return targets, nil
	}
	addrs, err := interfaceAddrs()
	if err != nil {
		return nil, fmt.Errorf("enumerate relay interface addresses: %w", err)
	}
	local := make(map[netip.Addr]bool, len(addrs))
	for _, addr := range addrs {
		if prefix, err := netip.ParsePrefix(addr.String()); err == nil {
			local[prefix.Addr().Unmap()] = true
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cache := make(map[string][]netip.Addr)
	for code, spec := range desired {
		ips := cache[spec.TargetHost]
		if len(ips) == 0 {
			if ip, err := netip.ParseAddr(spec.TargetHost); err == nil {
				ips = []netip.Addr{ip}
			} else {
				ips, err = net.DefaultResolver.LookupNetIP(ctx, "ip", spec.TargetHost)
				if err != nil {
					return nil, fmt.Errorf("rule %s resolve target %q: %w", code, spec.TargetHost, err)
				}
				if len(ips) == 0 {
					return nil, fmt.Errorf("rule %s target %q resolved to no addresses", code, spec.TargetHost)
				}
			}
			cache[spec.TargetHost] = ips
		}
		for _, ip := range ips {
			if err := checkRelayTarget(spec, ip, local, desired); err != nil {
				return nil, err
			}
		}
		targets[code] = ips[0].Unmap().String()
	}
	return targets, nil
}

func checkRelayTarget(spec panel.RelayRule, ip netip.Addr, local map[netip.Addr]bool, desired map[string]panel.RelayRule) error {
	ip = ip.Unmap().WithZone("")
	if !local[ip] {
		return nil
	}
	transports := specTransports(spec)
	for _, listener := range desired {
		if spec.TargetPort == 0 || spec.TargetPort != listener.ListenPort {
			continue
		}
		bind, err := netip.ParseAddr(listener.ListenIP)
		wildcard := listener.ListenIP == "" || (err == nil && bind.Unmap().IsUnspecified())
		if !wildcard && ip != bind.Unmap().WithZone("") {
			continue
		}
		for transport := range specTransports(listener) {
			if transports[transport] {
				return fmt.Errorf("rule %s target %s:%d loops to local %s listener of rule %s",
					spec.RuleCode, ip, spec.TargetPort, transport, listener.RuleCode)
			}
		}
	}
	return nil
}
