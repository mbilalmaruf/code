'use strict';

const crypto = require('crypto');
const _ = require('lodash');
const { v1: uuid } = require('uuid');
const config = require('../lib/config');

module.exports = {
  encrypt,
  decrypt,
  encryptEx,
  decryptEx
};

const ALGORITHM = 'aes-256-gcm';

function encrypt(crypt = '') {
  crypt = _.isObject(crypt) ? JSON.stringify(crypt) : crypt;
  const cipher = crypto.createCipher('aes-256-ctr', config.get('cryptoTemp'));
  return cipher.update(crypt, 'utf8', 'hex');
}

function decrypt(str = '') {
  return decryptEx(str);
}

function encryptEx(crypt = '') {
  const signingKey = uuid() + uuid() + uuid() + uuid();
  crypt = _.isObject(crypt) ? JSON.stringify(crypt) : crypt;

  const cipher = crypto.createCipher('aes-256-ctr', signingKey);
  const finalCrypt = {
    key: signingKey,
    value: cipher.update(crypt, 'utf8', 'hex')
  };

  return encrypt(finalCrypt);

}

function decryptEx(str = '') {

  // const decipher = crypto.createDecipher('aes-256-ctr', config.get('cryptoTemp'));
  // const crypt = decipher.update(str, 'hex', 'utf8');
  try {
    const { encryptedData, iv, authTag } = typeof str === "string" ? JSON.parse(str) :str;
    // const decryptStr = _decrypt(encryptedData, iv, authTag);
    let encryptedStr = encryptedData
    // console.debug({ decrypt: decryptStr });
    if (typeof encryptedStr === "string" && encryptedStr.trim().startsWith("{")) {
      encryptedStr = JSON.parse(encryptedStr);
    }
    const extractedIv = iv || encryptedStr.iv;
    const extractedAuthTag = authTag || encryptedStr.authTag;
    encryptedStr = typeof encryptedStr === "object" ? encryptedStr.encryptedData : encryptedStr;

    const key = crypto.createHash("sha256").update(config.get('cryptoTemp')).digest();
    const decipher = crypto.createDecipheriv(ALGORITHM, key, Buffer.from(extractedIv, "hex"));
    decipher.setAuthTag(Buffer.from(extractedAuthTag, "hex"));

    let decrypted = decipher.update(encryptedData, "hex", "utf8");
    decrypted += decipher.final("utf8");

    return decrypted;
  } catch (err) {
    console.debug("Error decoding token", err);
    return null;
  }
}