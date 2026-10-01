// Package secret handles config values stored either as plain strings or as
// AES-256-GCM blobs compatible with the legacy Node services
// ({"encryptedData","iv","authTag"} hex, key = sha256(encryptionKey)).
// Kept in sync with cipher-replicator-go/internal/secret.
package secret

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type Blob struct {
	EncryptedData string `json:"encryptedData"`
	IV            string `json:"iv"`
	AuthTag       string `json:"authTag"`
}

// Value is a config field that is either plaintext or an encrypted Blob.
type Value struct {
	plain     string
	blob      *Blob
	resolved  bool
	plaintext string
}

func Plain(s string) Value { return Value{plain: s, resolved: true, plaintext: s} }

func (v *Value) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if bytes.Equal(b, []byte("null")) {
		*v = Value{}
		return nil
	}
	if len(b) > 0 && b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		if blob, ok := parseBlob([]byte(s)); ok {
			*v = Value{blob: blob}
			return nil
		}
		*v = Value{plain: s}
		return nil
	}
	blob, ok := parseBlob(b)
	if !ok {
		return errors.New("secret must be a string or an object with encryptedData, iv and authTag")
	}
	*v = Value{blob: blob}
	return nil
}

func (v Value) MarshalJSON() ([]byte, error) {
	if v.blob != nil {
		return json.Marshal(v.blob)
	}
	return json.Marshal(v.plain)
}

func parseBlob(b []byte) (*Blob, bool) {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || b[0] != '{' {
		return nil, false
	}
	var wrapper struct {
		URL json.RawMessage `json:"url"`
		Blob
	}
	if err := json.Unmarshal(b, &wrapper); err != nil {
		return nil, false
	}
	if len(wrapper.URL) > 0 && bytes.TrimSpace(wrapper.URL)[0] == '{' {
		return parseBlob(wrapper.URL)
	}
	blob := wrapper.Blob
	if inner, ok := parseBlob([]byte(blob.EncryptedData)); ok {
		if inner.IV == "" {
			inner.IV = blob.IV
		}
		if inner.AuthTag == "" {
			inner.AuthTag = blob.AuthTag
		}
		blob = *inner
	}
	if blob.EncryptedData == "" || blob.IV == "" || blob.AuthTag == "" {
		return nil, false
	}
	return &blob, true
}

func (v Value) IsSet() bool { return v.blob != nil || v.plain != "" }

func (v *Value) Resolve(key string) error {
	if v.blob == nil {
		v.plaintext, v.resolved = v.plain, true
		return nil
	}
	if key == "" {
		return errors.New("value is encrypted but no encryption key is configured")
	}
	pt, err := Decrypt(*v.blob, key)
	if err != nil {
		return err
	}
	v.plaintext, v.resolved = pt, true
	return nil
}

func (v Value) String() string {
	if !v.resolved && v.blob != nil {
		panic("secret.Value used before Resolve")
	}
	if !v.resolved {
		return v.plain
	}
	return v.plaintext
}

func gcmFor(key string, nonceSize int) (cipher.AEAD, error) {
	sum := sha256.Sum256([]byte(key))
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCMWithNonceSize(block, nonceSize)
}

func Decrypt(b Blob, key string) (string, error) {
	iv, err := hex.DecodeString(b.IV)
	if err != nil || len(iv) == 0 {
		return "", fmt.Errorf("invalid iv: %w", err)
	}
	tag, err := hex.DecodeString(b.AuthTag)
	if err != nil {
		return "", fmt.Errorf("invalid authTag: %w", err)
	}
	if len(tag) != 16 {
		return "", fmt.Errorf("unsupported authTag length %d (want 16)", len(tag))
	}
	ct, err := hex.DecodeString(strings.TrimSpace(b.EncryptedData))
	if err != nil {
		return "", fmt.Errorf("invalid encryptedData: %w", err)
	}
	aead, err := gcmFor(key, len(iv))
	if err != nil {
		return "", err
	}
	pt, err := aead.Open(nil, iv, append(ct, tag...), nil)
	if err != nil {
		return "", errors.New("decryption failed (wrong key or corrupted value)")
	}
	return string(pt), nil
}

func Encrypt(plaintext, key string) (Blob, error) {
	if key == "" {
		return Blob{}, errors.New("encryption key is empty")
	}
	iv := make([]byte, 12)
	if _, err := rand.Read(iv); err != nil {
		return Blob{}, err
	}
	aead, err := gcmFor(key, len(iv))
	if err != nil {
		return Blob{}, err
	}
	sealed := aead.Seal(nil, iv, []byte(plaintext), nil)
	n := len(sealed) - aead.Overhead()
	return Blob{
		EncryptedData: hex.EncodeToString(sealed[:n]),
		IV:            hex.EncodeToString(iv),
		AuthTag:       hex.EncodeToString(sealed[n:]),
	}, nil
}
