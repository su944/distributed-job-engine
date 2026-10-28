package jobs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

func parseJSON(body []byte) (any, error) {
	if !utf8.Valid(body) {
		return nil, errors.New("JSON must be UTF-8")
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	v, err := readValue(d, 0)
	if err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("request must contain exactly one JSON value")
	}
	return v, nil
}

func readValue(d *json.Decoder, depth int) (any, error) {
	if depth > 64 {
		return nil, errors.New("JSON nesting exceeds 64")
	}
	t, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch t {
	case json.Delim('{'):
		m := make(map[string]any)
		for d.More() {
			t, err := d.Token()
			if err != nil {
				return nil, err
			}
			key, ok := t.(string)
			if !ok {
				return nil, errors.New("invalid object key")
			}
			if strings.ContainsRune(key, '\x00') {
				return nil, errors.New("JSON keys must not contain NUL")
			}
			if _, found := m[key]; found {
				return nil, errors.New("duplicate object key")
			}
			v, err := readValue(d, depth+1)
			if err != nil {
				return nil, err
			}
			m[key] = v
		}
		if _, err := d.Token(); err != nil {
			return nil, err
		}
		return m, nil
	case json.Delim('['):
		a := make([]any, 0)
		for d.More() {
			v, err := readValue(d, depth+1)
			if err != nil {
				return nil, err
			}
			a = append(a, v)
		}
		if _, err := d.Token(); err != nil {
			return nil, err
		}
		return a, nil
	default:
		if _, ok := t.(json.Delim); ok {
			return nil, errors.New("unexpected delimiter")
		}
		if s, ok := t.(string); ok && strings.ContainsRune(s, '\x00') {
			return nil, errors.New("JSON strings must not contain NUL")
		}
		return t, nil
	}
}

// Decimal normalization avoids float64 precision loss: 1, 1.0 and 10e-1 hash
// identically, while adjacent large integers remain distinct. Bounds also stay
// comfortably inside PostgreSQL jsonb numeric limits.
func normalizeNumber(n json.Number) (json.Number, error) {
	s := n.String()
	if len(s) > 1024 {
		return "", errors.New("JSON number too long")
	}
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign = "-"
		s = s[1:]
	}
	exp := 0
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		e, err := strconv.Atoi(s[i+1:])
		if err != nil || e < -1000 || e > 1000 {
			return "", errors.New("JSON exponent out of range")
		}
		exp = e
		s = s[:i]
	}
	if i := strings.IndexByte(s, '.'); i >= 0 {
		exp -= len(s) - i - 1
		s = s[:i] + s[i+1:]
	}
	s = strings.TrimLeft(s, "0")
	if s == "" {
		return "0", nil
	}
	trimmed := strings.TrimRight(s, "0")
	exp += len(s) - len(trimmed)
	return json.Number(sign + trimmed + "e" + strconv.Itoa(exp)), nil
}

func normalizeNumbers(v any) error {
	switch x := v.(type) {
	case map[string]any:
		for k, v := range x {
			if n, ok := v.(json.Number); ok {
				normalized, err := normalizeNumber(n)
				if err != nil {
					return err
				}
				x[k] = normalized
			} else if err := normalizeNumbers(v); err != nil {
				return err
			}
		}
	case []any:
		for i, v := range x {
			if n, ok := v.(json.Number); ok {
				normalized, err := normalizeNumber(n)
				if err != nil {
					return err
				}
				x[i] = normalized
			} else if err := normalizeNumbers(v); err != nil {
				return err
			}
		}
	}
	return nil
}
