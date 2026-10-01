package secret

import (
	"encoding/json"
	"os"
	"testing"
)

func TestEncryptDecryptRoundTrip(t *testing.T) {
	b, err := Encrypt("postgres://u:p@host/db", "k1")
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decrypt(b, "k1")
	if err != nil || got != "postgres://u:p@host/db" {
		t.Fatalf("got %q, %v", got, err)
	}
	if _, err := Decrypt(b, "wrong"); err == nil {
		t.Fatal("expected failure with wrong key")
	}
}

func TestValueUnmarshalForms(t *testing.T) {
	b, _ := Encrypt("secret!", "k")
	raw, _ := json.Marshal(b)
	stringified, _ := json.Marshal(string(raw))
	wrapped := []byte(`{"url":` + string(raw) + `}`)

	cases := map[string][]byte{
		"plain":       []byte(`"hello"`),
		"object":      raw,
		"stringified": stringified,
		"url-wrapped": wrapped,
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			var v Value
			if err := json.Unmarshal(in, &v); err != nil {
				t.Fatal(err)
			}
			if err := v.Resolve("k"); err != nil {
				t.Fatal(err)
			}
			want := "secret!"
			if name == "plain" {
				want = "hello"
			}
			if v.String() != want {
				t.Fatalf("got %q want %q", v.String(), want)
			}
		})
	}
}

func TestEncryptedWithoutKey(t *testing.T) {
	b, _ := Encrypt("x", "k")
	raw, _ := json.Marshal(b)
	var v Value
	_ = json.Unmarshal(raw, &v)
	if err := v.Resolve(""); err == nil {
		t.Fatal("expected error without key")
	}
}

// TestLegacyNodeFixture decrypts a blob produced by the legacy Node code path
// (aes-256-gcm, key = sha256(cryptoTemp)). Generate with testdata/gen_fixture.js.
func TestLegacyNodeFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/node_fixture.json")
	if err != nil {
		t.Skip("no node fixture:", err)
	}
	var fx struct {
		Key       string `json:"key"`
		Plaintext string `json:"plaintext"`
		Blob      Blob   `json:"blob"`
	}
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatal(err)
	}
	got, err := Decrypt(fx.Blob, fx.Key)
	if err != nil || got != fx.Plaintext {
		t.Fatalf("got %q, %v", got, err)
	}
}
