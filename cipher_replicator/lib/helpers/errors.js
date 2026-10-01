'use strict';

const util = require('util');

function RestError(opts, caller) {
  this.message = opts.message;
  this.isServer = opts.isServer;
  this.httpStatus = opts.code;
  this.fields = opts.fields ? [].concat(opts.fields).filter(Boolean) : undefined;
  this.values = opts.values ? [].concat(opts.values).filter(Boolean) : undefined;
  Error.captureStackTrace(this, caller || RestError);
}
util.inherits(RestError, Error);

module.exports = {
  notFound: function notFound(message = 'The resource you are looking for does not exist.', status = 404) {
    const opts = { code: status, message: message };
    throw new RestError(opts, notFound);
  }
};
