'use strict';

const _ = require('lodash');
const config = global.config;

function get(path) {
  return _.get(config, path, '');
}

module.exports.get = get;
module.exports._instance = config;
