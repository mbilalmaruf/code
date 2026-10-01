'use strict';

var util = require('util');

var helper = require('./helper.js');

var NodeCouchDb = require('node-couchdb');

var logger = helper.getLogger('Query');

var _ = require('lodash');

var _require = require('deep-parse-json'),
    deepParseJson = _require.deepParseJson;

var crypto = require('./crypto');

var snooze = function snooze(ms) {
  return new Promise(function (resolve) {
    return setTimeout(resolve, ms);
  });
};

var queryChaincode = function queryChaincode(channelName, chaincodeName, args, fcn, username, network) {
  return regeneratorRuntime.async(function queryChaincode$(_context) {
    while (1) {
      switch (_context.prev = _context.next) {
        case 0:
          if (!(global.config.replicatorSource != "EVENTHUB_GETDATABYKEY")) {
            _context.next = 6;
            break;
          }

          _context.next = 3;
          return regeneratorRuntime.awrap(queryCouch(channelName, chaincodeName, args, fcn, username, network));

        case 3:
          return _context.abrupt("return", _context.sent);

        case 6:
          _context.next = 8;
          return regeneratorRuntime.awrap(queryChaincodeHyp(channelName, chaincodeName, args, fcn, username, network));

        case 8:
          return _context.abrupt("return", _context.sent);

        case 9:
        case "end":
          return _context.stop();
      }
    }
  });
};

var couchInit = function couchInit(callback) {
  var configConnection;
  return regeneratorRuntime.async(function couchInit$(_context2) {
    while (1) {
      switch (_context2.prev = _context2.next) {
        case 0:
          if (!(global.config.replicatorSource != "EVENTHUB_GETDATABYKEY")) {
            _context2.next = 8;
            break;
          }

          global.collectionList = {};
          configConnection = global.config.blockChainConfiguration;
          console.log(JSON.stringify(configConnection.couch));
          global.couch = new NodeCouchDb({
            host: configConnection.couch.ip,
            // IP address
            protocol: configConnection.couch.protocall,
            //protocall
            port: configConnection.couch.port,
            //port
            timeout: configConnection.couch.timeout || 30000,
            //timeout
            auth: {
              user: configConnection.couch.username,
              //username
              pass: configConnection.couch.password //password

            }
          });
          return _context2.abrupt("return", global.couch.listDatabases().then(function (list) {
            if (!list.error) {
              list.forEach(function (element) {
                var key = element.replace(/[^a-zA-Z0-9]/g, ''); // && element.indexOf('$$p') > -1

                if (element.indexOf('$$h') == -1 && element.indexOf(configConnection.smartContract.replace(/([A-Z])/g, "%24$1").replace(/%24/g, '$').toLowerCase()) > -1 && element.indexOf(configConnection.channel.replace(/([A-Z])/g, "%24$1").replace(/%24/g, '$').toLowerCase()) > -1) {
                  console.log(element, '--> ', element.replace(/[^a-zA-Z0-9]/g, ''));

                  _.set(global.collectionList, key, element);
                }
              }); // process.exit(0);

              if (callback) callback(global.collectionList);
              return global.collectionList;
            } else {
              console.log(list);
              logger.error({
                fs: 'QueryTransaction',
                func: 'listDatabases'
              }, list.error);
              process.exit(0);
            }
          }));

        case 8:
          console.log("Couch as a datasource not selected!!!");

        case 9:
        case "end":
          return _context2.stop();
      }
    }
  });
};

var queryCouch = function queryCouch(channelName, chaincodeName, args, fcn, username, network) {
  var databaseName, dbNameResolved, generalConfig;
  return regeneratorRuntime.async(function queryCouch$(_context3) {
    while (1) {
      switch (_context3.prev = _context3.next) {
        case 0:
          databaseName = "";

          if (args[1] == "") {
            databaseName = "".concat(channelName, "_").concat(chaincodeName.replace(/([A-Z])/g, "%24$1").replace(/%24/g, '$').toLowerCase().replace("$$p", ""));
          } else {
            databaseName = "".concat(channelName, "_").concat(chaincodeName.replace(/([A-Z])/g, "%24$1").replace(/%24/g, '$').toLowerCase(), "$$p").concat(args[1].replace(/([A-Z])/g, "%24$1").replace(/%24/g, '$').toLowerCase());
          }

          dbNameResolved = databaseName;
          generalConfig = global.config;

          if (!(generalConfig.localCouch && !global.couch)) {
            _context3.next = 8;
            break;
          }

          _context3.next = 7;
          return regeneratorRuntime.awrap(couchInit());

        case 7:
          console.log("couch init sessfully");

        case 8:
          console.log("queryChaincode", databaseName);

          if (!dbNameResolved) {
            _context3.next = 13;
            break;
          }

          return _context3.abrupt("return", new Promise(function (resolve, reject) {
            global.couch.get(dbNameResolved, args[0]).then(function (_ref) {
              var data = _ref.data,
                  headers = _ref.headers,
                  status = _ref.status;
              var response = {
                success: true,
                data: data
              };
              console.log("[Returning Data][DB:".concat(dbNameResolved, "][KEY:").concat(args[0], "] data found!"));
              return resolve(response);
            }, function (err) {
              console.log("[Querying][DB:".concat(dbNameResolved, "][KEY:").concat(args[0], "] data not found for key!!"));
              console.log(err); //  todo queue in repllicator queue

              return resolve({
                success: true,
                data: {}
              }); // return reject(err);
            });
          })["catch"](function (err) {
            return console.log(err);
          }));

        case 13:
          console.log("[Querying][DB:".concat(dbNameResolved, "][KEY:").concat(args[0], "] local couch not initialized!!"));
          return _context3.abrupt("return", null);

        case 15:
          ;

        case 16:
        case "end":
          return _context3.stop();
      }
    }
  });
};

var queryChaincodeHyp = function queryChaincodeHyp(channelName, chaincodeName, args, fcn, username, network) {
  var client, channel, message, admin, _message, transient_data, request, response_payloads, i, load, key, response, _response;

  return regeneratorRuntime.async(function queryChaincodeHyp$(_context4) {
    while (1) {
      switch (_context4.prev = _context4.next) {
        case 0:
          _context4.prev = 0;
          _context4.next = 3;
          return regeneratorRuntime.awrap(helper.getClientForOrg(network, username, channelName));

        case 3:
          client = _context4.sent;
          logger.debug({
            fs: 'QueryTransaction',
            func: 'getClientForOrg'
          }, 'Successfully got the fabric client for the organization ' + network);
          channel = client.getChannel(channelName);

          if (channel) {
            _context4.next = 10;
            break;
          }

          message = util.format('Channel %s was not defined in the connection profile', channelName);
          logger.error({
            fs: 'QueryTransaction',
            func: 'getChannel'
          }, message);
          throw new Error(message);

        case 10:
          _context4.next = 12;
          return regeneratorRuntime.awrap(helper.getOrgAdmin(network, client));

        case 12:
          admin = _context4.sent;

          if (admin) {
            _context4.next = 17;
            break;
          }

          _message = util.format('admin error');
          logger.error({
            fs: 'QueryTransaction',
            func: 'getOrgAdmin'
          }, _message);
          throw new Error(_message);

        case 17:
          // send query
          transient_data = {
            'PrivateArgs': Buffer.from(args.join("|")) // string <-> byte[]

          };
          _context4.next = 20;
          return regeneratorRuntime.awrap(helper.newPeers(undefined, network, username, channelName));

        case 20:
          _context4.t0 = _context4.sent;
          _context4.t1 = chaincodeName;
          _context4.t2 = fcn;
          _context4.t3 = [];
          _context4.t4 = transient_data;
          request = {
            targets: _context4.t0,
            chaincodeId: _context4.t1,
            fcn: _context4.t2,
            args: _context4.t3,
            transientMap: _context4.t4
          };
          _context4.next = 28;
          return regeneratorRuntime.awrap(channel.queryByChaincode(request));

        case 28:
          response_payloads = _context4.sent;
          console.log("Error Occured Please Check Chaincode >", JSON.stringify(response_payloads));

          if (!response_payloads) {
            _context4.next = 38;
            break;
          }

          for (i = 0; i < response_payloads.length; i++) {
            logger.info({
              fs: 'QueryTransaction',
              func: 'queryByChaincode'
            }, response_payloads[i].toString('utf8'));

            if (response_payloads[i].status == 500) {
              console.log("Error Occured Please Check Chaincode >", JSON.stringify(response_payloads[i]));
            }
          }

          load = JSON.parse(response_payloads[0].toString('utf8')) || {};

          for (key in load) {
            if (load[key] instanceof String && load[key].indexOf("{") > -1) load[key] = deepParseJson(load[key]) || load[key];
          }

          response = {
            success: true,
            data: load || {}
          };
          return _context4.abrupt("return", response);

        case 38:
          logger.error({
            fs: 'QueryTransaction',
            func: 'queryByChaincode'
          }, 'response_payloads is null');
          _response = {
            success: true,
            data: {}
          };
          return _context4.abrupt("return", _response);

        case 41:
          _context4.next = 47;
          break;

        case 43:
          _context4.prev = 43;
          _context4.t5 = _context4["catch"](0);
          logger.error({
            fs: 'QueryTransaction',
            func: 'queryByChaincode'
          }, 'Failed to query due to error: ' + _context4.t5.stack ? _context4.t5.stack : _context4.t5);
          return _context4.abrupt("return", _context4.t5.toString());

        case 47:
        case "end":
          return _context4.stop();
      }
    }
  }, null, null, [[0, 43]]);
};

exports.queryChaincode = queryChaincode;
exports.couchInit = couchInit;