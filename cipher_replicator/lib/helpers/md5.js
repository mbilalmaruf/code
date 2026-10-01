'use strict';

const md5 = require('md5');

function encrypt(string = '') {
  return md5(string);
}

module.exports = {
  encrypt: encrypt
};
