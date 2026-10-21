// Package netguard prevents server-side request forgery (SSRF).
//
// A webhook service makes HTTP requests to URLs chosen by its users. Without
// protection, anyone who can create an endpoint can make our servers call
// http://169.254.169.254/ (cloud credentials), http://localhost:5432/,
// http://10.0.0.7/admin and so on, and read the responses in the attempt log.
//
// Checking the URL's hostname when the endpoint is created is NOT enough:
// DNS can return a public IP at creation time and a private one later
// ("DNS rebinding"). So the check runs inside the dialer, on the exact IP
// address we are about to connect to, after DNS resolution, for every
// connection.
package netguard

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"syscall"
)

// ErrBlocked is returned (wrapped) when a connection target is not allowed.
var ErrBlocked = errors.New("destination address is not allowed")

// blocked lists ranges that must never be reachable from webhook delivery.
var blocked = mustPrefixes(
	"0.0.0.0/8",       // "this" network
	"10.0.0.0/8",      // private
	"100.64.0.0/10",   // carrier-grade NAT
	"127.0.0.0/8",     // loopback
	"169.254.0.0/16",  // link-local, incl. cloud metadata 169.254.169.254
	"172.16.0.0/12",   // private
	"192.0.0.0/24",    // IETF protocol assignments
	"192.0.2.0/24",    // TEST-NET-1
	"192.88.99.0/24",  // 6to4 relay anycast
	"192.168.0.0/16",  // private
	"198.18.0.0/15",   // benchmarking
	"198.51.100.0/24", // TEST-NET-2
	"203.0.113.0/24",  // TEST-NET-3
	"224.0.0.0/4",     // multicast
	"240.0.0.0/4",     // reserved, incl. broadcast
	"::/128",          // unspecified
	"::1/128",         // loopback
	"64:ff9b::/96",    // NAT64 (can map to private IPv4)
	"64:ff9b:1::/48",  // local-use NAT64
	"100::/64",        // discard
	"2001::/23",       // IETF protocol assignments
	"2001:db8::/32",   // documentation
	"2002::/16",       // 6to4 (embeds IPv4)
	"fc00::/7",        // unique local
	"fe80::/10",       // link-local
	"ff00::/8",        // multicast
)

func mustPrefixes(cidrs ...string) []netip.Prefix {
	out := make([]netip.Prefix, len(cidrs))
	for i, c := range cidrs {
		out[i] = netip.MustParsePrefix(c)
	}
	return out
}

// Allowed reports whether ip may be contacted.
func Allowed(ip netip.Addr) bool {
	if !ip.IsValid() {
		return false
	}
	// ::ffff:10.0.0.1 is 10.0.0.1 in disguise.
	ip = ip.Unmap()
	for _, p := range blocked {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}

// Control is a net.Dialer Control function. It runs after DNS resolution,
// just before connect(2), with the literal IP:port being dialled.
func Control(network, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrBlocked, address)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil || !Allowed(ip) {
		return fmt.Errorf("%w: %s (%s)", ErrBlocked, host, network)
	}
	return nil
}

// CheckLiteral rejects a hostname that is itself a forbidden IP literal.
// It is a fast, user-friendly check at endpoint creation; Control is the
// real enforcement.
func CheckLiteral(host string) error {
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return nil // a DNS name: checked at connect time
	}
	if !Allowed(ip) {
		return fmt.Errorf("%w: %s", ErrBlocked, host)
	}
	return nil
}
