'use strict';
const factory = require('../client/index');

//let mqConnection = crypto.decrypt(config.get('amqp.url'));

function _start(mqConnection,queue) {
  return factory.createClient('amqp', mqConnection,queue);
}

module.exports = {
  start: _start
};
