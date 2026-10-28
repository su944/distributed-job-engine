package jobs

import (
	"fmt"
	"testing"
)

// A persisted hash is an API compatibility contract: changing the algorithm would
// turn previously accepted client retries into conflicts after a deployment.
func TestPersistedHashCompatibility(t *testing.T) {
	const want = "0d984a4e7fbf9b3b408f09c42a69463c62e9e7484298537cf0f5b89bae5931d1"
	for _, body := range []string{
		`{"type":"invoice.send","payload":{"invoice_id":"inv_001","amount":1}}`,
		`{"payload":{"amount":10e-1,"invoice_id":"inv_\u0030\u00301"},"max_attempts":5,"type":"invoice.send"}`,
		` { "max_attempts": 5, "type": "invoice.send", "payload": {"amount": 1.000, "invoice_id": "inv_001"} } `,
	} {
		s, err := ParseSubmission("key", []byte(body), allowed)
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprintf("%x", s.Hash); got != want {
			t.Fatalf("persisted hash changed: %s", got)
		}
	}
}

func TestLogicalRequestBoundaries(t *testing.T) {
	types := map[string]bool{"invoice.send": true, "email.send": true}
	for _, tc := range []struct {
		name, left, right string
		equal             bool
	}{
		{"negative_zero", `{"n":-0.000e10}`, `{"n":0}`, true},
		{"small_decimal", `{"n":0.00100}`, `{"n":1e-3}`, true},
		{"large_decimal", `{"n":123456789012345678901234567890.00}`, `{"n":12345678901234567890123456789e1}`, true},
		{"unicode", `{"\u0061":"\ud83d\ude80"}`, `{"a":"🚀"}`, true},
		{"nested_arrays", `{"a":[{"x":1.0,"y":2}]}`, `{"a":[{"y":2e0,"x":1}]}`, true},
		{"array_order", `{"a":[1,2]}`, `{"a":[2,1]}`, false},
		{"null_and_missing", `{"a":null}`, `{}`, false},
		{"empty_and_null", `{"a":[]}`, `{"a":null}`, false},
		{"number_and_string", `{"a":1}`, `{"a":"1"}`, false},
		{"adjacent_integers", `{"n":9007199254740992}`, `{"n":9007199254740993}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, err := ParseSubmission("first-key", []byte(`{"type":"invoice.send","payload":`+tc.left+`}`), types)
			if err != nil {
				t.Fatal(err)
			}
			b, err := ParseSubmission("second-key", []byte(`{"type":"invoice.send","payload":`+tc.right+`}`), types)
			if err != nil {
				t.Fatal(err)
			}
			if (a.Hash == b.Hash) != tc.equal {
				t.Fatalf("equal=%t, want %t", a.Hash == b.Hash, tc.equal)
			}
		})
	}
	a, err := ParseSubmission("key", []byte(`{"type":"invoice.send","payload":{}}`), types)
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{`{"type":"email.send","payload":{}}`, `{"type":"invoice.send","payload":{},"max_attempts":6}`} {
		b, err := ParseSubmission("key", []byte(body), types)
		if err != nil {
			t.Fatal(err)
		}
		if a.Hash == b.Hash {
			t.Fatal("type/policy omitted from hash")
		}
	}
}
