'use strict';

const bunyan = require('../customLogger');
const RotatingFileStream = require('bunyan-rotating-file-stream');

const _ = require('lodash');
const path = require('path');

let _logger;

const Logger = (config) => {
  const rootDir = path.join(__dirname, '../../');
  return {
    app: bunyan.createLogger({
      name: config.name,
      streams: [{
        type: 'raw',
        stream: new RotatingFileStream({
          path: path.join(rootDir, config.appPath),
          period: '1d',
          totalFiles: 0,
          rotateExisting: true,
          threshold: '25m',
          totalSize: 0,
          gzip: false
        })
      }, {
        stream: process.stderr,
        level: 'error'
      }],
      level: 'trace'
    }),
    activity: bunyan.createLogger({
      name: config.name,
      streams: [{
        type: 'raw',
        stream: new RotatingFileStream({
          path: path.join(rootDir, config.activityPath),
          period: '1d',
          totalFiles: 0,
          rotateExisting: true,
          threshold: '25m',
          totalSize: 0,
          gzip: false
        })
      }, {
        stream: process.stderr,
        level: 'error'
      }],
      level: 'trace'
    }),
    request: bunyan.createLogger({
      name: config.name,
      streams: [{
        type: 'raw',
        stream: new RotatingFileStream({
          path: path.join(rootDir, config.requestPath),
          period: '1d',
          totalFiles: 0,
          rotateExisting: true,
          threshold: '25m',
          totalSize: 0,
          gzip: false
        })
      }, {
        stream: process.stderr,
        level: 'error'
      }],
      level: 'trace'
    }),
    soap: bunyan.createLogger({
      name: config.name,
      streams: [{
        type: 'raw',
        stream: new RotatingFileStream({
          path: path.join(rootDir, config.soapPath),
          period: '1d',
          totalFiles: 0,
          rotateExisting: true,
          threshold: '25m',
          totalSize: 0,
          gzip: false
        })
      }, {
        stream: process.stderr,
        level: 'error'
      }],
      level: 'trace'
    })
  };
};

module.exports = (config) => {
  _logger = _.isObject(_logger) ? _logger : Logger(config);
  return _logger;
};
