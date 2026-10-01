// Produces node_fixture.json using the same algorithm as the legacy
// cipher_replicator/lib/helpers/crypto.js (aes-256-gcm, key = sha256(cryptoTemp), 16-byte IV).
// Usage: node gen_fixture.js > node_fixture.json
const crypto = require('crypto');
const key = 'fixture-key-not-a-real-secret';
const plaintext = 'postgres://user:pass@localhost:5432/db';
const iv = crypto.randomBytes(16);
const cipher = crypto.createCipheriv('aes-256-gcm', crypto.createHash('sha256').update(key).digest(), iv);
let enc = cipher.update(plaintext, 'utf8', 'hex');
enc += cipher.final('hex');
process.stdout.write(JSON.stringify({
  key,
  plaintext,
  blob: { encryptedData: enc, iv: iv.toString('hex'), authTag: cipher.getAuthTag().toString('hex') },
}, null, 2));
