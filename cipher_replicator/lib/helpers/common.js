'use strict';

const passwordPolicyRepo = require('../repositories/passwordPolicy');
const jwt = require('./jwt');
const config = require('../../config');

function generatePassword() {
  return passwordPolicyRepo.findOne()
    .then((policy) => {
      const diff = policy.minimumPasswordLength - (policy.minimumDigitCount + policy.minimumAlphabetCount);
      const numLength = Math.ceil(diff / 2) + policy.minimumDigitCount;
      return {
        password: randomUpperCase(policy.minimumUpperCase) + randomLowerCase(policy.minimumLowerCase) + randomNum(numLength),
        hashType: 'md5',
        id: policy._id
      };
    });

  function randomUpperCase(length) {
    let text = '';
    const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
    for (let i = 0; i < length; i += 1) {
      text += possible.charAt(Math.floor(Math.random() * possible.length));
    }
    return text;
  }

  function randomLowerCase(length) {
    let text = '';
    const possible = 'abcdefghijklmnopqrstuvwxyz';
    for (let i = 0; i < length; i += 1) {
      text += possible.charAt(Math.floor(Math.random() * possible.length));
    }
    return text;
  }
  function randomNum(length) {
    let text = '';
    const possible = '0123456789';
    for (let i = 0; i < length; i += 1) {
      text += possible.charAt(Math.floor(Math.random() * possible.length));
    }
    return text;
  }
}

function generateResetUri(user) {
  const token = jwt.create(user);
  return {
    uri: `${config.get('webAddress')}changePassword/SDG?t=${token}`,
    passwordToken: token
  };
}

function success(scopeData) {
  return new Promise((resolve, reject) => {
    resolve(scopeData);
  });
}

function reject(scopeData) {
  return new Promise((resolve, reject) => {
    reject(scopeData);
  });
}

function validPassword(password) {
  return passwordPolicyRepo.findOne()
    .then((policy) => {
      const msg = {
        error: policy.errorMessage,
        valid: false,
        unAcceptedKeywords: policy.unAcceptedKeywords || []
      };
      if (!new RegExp(`[a-zA-Z0-9]{${policy.minimumPasswordLength},${policy.maximumPasswordLength}}`, 'g').test(password)) {
        return msg;
      }
      if (!new RegExp(`[0-9]{${policy.minimumDigitCount},${policy.maximumDigitCount}}`, 'g').test(password)) {
        return msg;
      }
      if (!new RegExp(`[A-Z]{${policy.minimumUpperCase},${policy.maximumAlphabetCount}}`, 'g').test(password)) {
        return msg;
      }
      if (!new RegExp(`[a-z]{${policy.minimumLowerCase},${policy.maximumAlphabetCount}}`, 'g').test(password)) {
        return msg;
      }
      msg.valid = true;
      return msg;
    });
}

module.exports = {
  generatePassword: generatePassword,
  generateResetUri: generateResetUri,
  validPassoword: validPassword,
  chainBreaker: { success, reject }
};
