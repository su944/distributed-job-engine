package jobs

import (
	"strings"
	"testing"
)

var allowed = map[string]bool{"invoice.send": true}

func TestCanonicalSubmission(t *testing.T) {
	a, err := ParseSubmission("same", []byte(`{"type":"invoice.send","payload":{"b":[1.00,-0,100e-2],"a":"x"}}`), allowed)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ParseSubmission("same", []byte(`{"max_attempts":5,"payload":{"a":"\u0078","b":[1,0,1]},"type":"invoice.send"}`), allowed)
	if err != nil {
		t.Fatal(err)
	}
	if a.Hash != b.Hash || string(a.Payload) != string(b.Payload) {
		t.Fatal("equivalent requests differ")
	}
	for _, body := range []string{
		`{"type":"invoice.send","payload":{"b":[1,0,1],"a":"x"},"max_attempts":6}`,
		`{"type":"invoice.send","payload":{"b":[1,0,2],"a":"x"}}`,
	} {
		c, err := ParseSubmission("same", []byte(body), allowed)
		if err != nil {
			t.Fatal(err)
		}
		if c.Hash == a.Hash {
			t.Fatal("different requests hash equally")
		}
	}
	large1, _ := ParseSubmission("same", []byte(`{"type":"invoice.send","payload":{"n":9007199254740992}}`), allowed)
	large2, _ := ParseSubmission("same", []byte(`{"type":"invoice.send","payload":{"n":9007199254740993}}`), allowed)
	if large1.Hash == large2.Hash {
		t.Fatal("integer precision lost")
	}
}

func TestInvalidSubmissions(t *testing.T) {
	valid := `{"type":"invoice.send","payload":{}}`
	for name, body := range map[string]string{
		"null": "null", "empty": "", "array": "[]", "trailing": valid + ` {}`,
		"unknown":          `{"type":"unknown","payload":{}}`,
		"duplicate":        `{"type":"invoice.send","type":"invoice.send","payload":{}}`,
		"nested duplicate": `{"type":"invoice.send","payload":{"x":1,"x":2}}`,
		"NUL value":        `{"type":"invoice.send","payload":{"x":"\u0000"}}`,
		"NUL key":          `{"type":"invoice.send","payload":{"\u0000":"x"}}`,
		"missing payload":  `{"type":"invoice.send"}`,
		"null payload":     `{"type":"invoice.send","payload":null}`,
		"unknown field":    `{"type":"invoice.send","payload":{},"extra":1}`,
		"zero budget":      `{"type":"invoice.send","payload":{},"max_attempts":0}`,
		"excess budget":    `{"type":"invoice.send","payload":{},"max_attempts":21}`,
		"null budget":      `{"type":"invoice.send","payload":{},"max_attempts":null}`,
		"fraction budget":  `{"type":"invoice.send","payload":{},"max_attempts":1.5}`,
		"huge exponent":    `{"type":"invoice.send","payload":{"n":1e999999999}}`,
		"deep":             `{"type":"invoice.send","payload":{"x":` + strings.Repeat("[", 70) + `0` + strings.Repeat("]", 70) + `}}`,
		"invalid UTF8":     "{\"type\":\"invoice.send\",\"payload\":{\"x\":\"\xff\"}}",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseSubmission("key", []byte(body), allowed); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
	for _, key := range []string{"", "a b", "a\n", strings.Repeat("a", 129), "é"} {
		if _, err := ParseSubmission(key, []byte(valid), allowed); err == nil {
			t.Errorf("accepted key %q", key)
		}
	}
}

func TestUUID(t *testing.T) {
	a, err := UUID()
	if err != nil {
		t.Fatal(err)
	}
	b, err := UUID()
	if err != nil {
		t.Fatal(err)
	}
	if !ValidID(a) || !ValidID(b) || a == b || a[14] != '4' {
		t.Fatalf("invalid UUIDs %q %q", a, b)
	}
}
