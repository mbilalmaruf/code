'use strict';

const crypto = require('./crypto');
const dates = require('../helpers/dates');
const uuid = require('uuid/v1');

module.exports = {
  create: create
};

function create(user) {
  const jwt = {
    _id: user._id,
    userID: user.userID,
    orgType: user.orgType,
    hypUser: user.hypUser,
    quorrumUser: user.quorrumUser,
    orgCode: user.orgCode || '',
    createdAt: dates.newDate()
  };
  return crypto.encryptEx(jwt);
}
