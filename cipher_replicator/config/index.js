'use strict';

const _ = require('lodash');
const config = global.config;

function get(path) {
  // console.log(global.config)
  return _.get(config, path, '');
}

module.exports.get = get;
module.exports._instance = config;
