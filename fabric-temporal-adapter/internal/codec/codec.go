// Package codec optionally encrypts Temporal payloads (workflow inputs and
// results, activity inputs and results) with AES-256-GCM, so connection
// profiles, transient data and enrollment secrets are not stored in plain
// text in Temporal history. Workers and every client must use the same key.
package codec

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

const (
	metadataEncoding = "binary/encrypted"
	metadataKeyID    = "encryption-key-id"
)

type encryptionCodec struct {
	aead  cipher.AEAD
	keyID string
}

// DataConverter wraps the default converter with encryption; key "" returns the default.
func DataConverter(key string) (converter.DataConverter, error) {
	if key == "" {
		return converter.GetDefaultDataConverter(), nil
	}
	sum := sha256.Sum256([]byte(key))
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	id := sha256.Sum256(sum[:])
	c := &encryptionCodec{aead: aead, keyID: fmt.Sprintf("%x", id[:4])}
	return converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), c), nil
}

func (c *encryptionCodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		plain, err := proto.Marshal(p)
		if err != nil {
			return nil, err
		}
		nonce := make([]byte, c.aead.NonceSize())
		if _, err := rand.Read(nonce); err != nil {
			return nil, err
		}
		out[i] = &commonpb.Payload{
			Metadata: map[string][]byte{
				converter.MetadataEncoding: []byte(metadataEncoding),
				metadataKeyID:              []byte(c.keyID),
			},
			Data: c.aead.Seal(nonce, nonce, plain, nil),
		}
	}
	return out, nil
}

func (c *encryptionCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		if string(p.Metadata[converter.MetadataEncoding]) != metadataEncoding {
			out[i] = p // not encrypted (e.g. started by a client without the codec)
			continue
		}
		if kid := string(p.Metadata[metadataKeyID]); kid != c.keyID {
			return nil, fmt.Errorf("payload encrypted with key %q, worker has %q", kid, c.keyID)
		}
		ns := c.aead.NonceSize()
		if len(p.Data) < ns {
			return nil, fmt.Errorf("encrypted payload too short")
		}
		plain, err := c.aead.Open(nil, p.Data[:ns], p.Data[ns:], nil)
		if err != nil {
			return nil, fmt.Errorf("decrypt payload: %w", err)
		}
		var dp commonpb.Payload
		if err := proto.Unmarshal(plain, &dp); err != nil {
			return nil, err
		}
		out[i] = &dp
	}
	return out, nil
}
