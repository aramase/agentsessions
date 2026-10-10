package main

import (
	"strings"
	"testing"
)

// A typo or wildcard on the admin listener must never turn an unauthenticated API into a network service.
func TestRegistryAddressIsLiteralLoopback(t *testing.T) {
	for _, tc := range []struct {
		addr  string
		valid bool
	}{
		{"127.0.0.1:9000", true}, {"127.4.5.6:0", true}, {"[::1]:9000", true}, {"[::ffff:127.0.0.1]:0", true},
		{"0.0.0.0:9000", false}, {"[::]:9000", false}, {":9000", false}, {"localhost:9000", false},
		{"192.168.1.1:9000", false}, {"8.8.8.8:9000", false}, {"[2001:db8::1]:9000", false},
		{"127.0.0.1", false}, {"127.0.0.1:", false}, {"127.0.0.1:abc", false}, {"127.0.0.1:65536", false},
	} {
		t.Run(tc.addr, func(t *testing.T) {
			_, err := checkRegistryAddress(tc.addr)
			if (err == nil) != tc.valid {
				t.Fatalf("checkRegistryAddress(%q) = %v, valid=%v", tc.addr, err, tc.valid)
			}
			if !tc.valid && !strings.Contains(err.Error(), "literal loopback") {
				t.Fatalf("unhelpful address error: %v", err)
			}
		})
	}
}
