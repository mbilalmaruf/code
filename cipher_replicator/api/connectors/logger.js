'use strict';

const logger = require('../../lib/helpers/logger');
const config = require('../../../lib/constants/logger');

module.exports = (() => {
  return logger(config);
})();
