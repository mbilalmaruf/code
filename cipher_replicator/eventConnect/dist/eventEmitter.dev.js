'use strict';

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(source, true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(source).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

var helper = require('./helper.js');

var crypto = require('./crypto');

var amqp = require('amqplib');

var connection;
var shouldbedelivered = 0;

var isReachable = require('is-reachable'); // const amq = require('../api/connectors/queue');


var amq = require('amqp-connection-manager');

var query = require('./query-transaction');

var typedata = require('./typedata');

var logger = helper.getLogger('invoke-chaincode');

var follow = require('follow');

var entity = require('./partnerEcomm');

var errorCodes = require('./errorCode');

var graph = require('./stateSet');

var queueReplication = require('fastq').promise(worker, 5);

var queueEvents = require('fastq').promise(workerEvent, 5);

var _ = require('lodash');

var meta;
var replicatorqueue;
var myUser;

var _require = require('sequelize'),
    Op = _require.Op;

var allowed = _.get(global, "config.replicatorPermission.allowed", []);

var denied = _.get(global, "config.replicatorPermission.denied", []);

var inactivity_ms = _.get(global, "config.inactivity_ms", undefined);

var snooze = function snooze(ms) {
  return new Promise(function (resolve) {
    return setTimeout(resolve, ms);
  });
};

var count = 0;

function worker(_ref) {
  var result, collection, change, collectionMapSequence, newCollectionName, currentSeek, scname, response;
  return regeneratorRuntime.async(function worker$(_context) {
    while (1) {
      switch (_context.prev = _context.next) {
        case 0:
          result = _ref.result, collection = _ref.collection, change = _ref.change, collectionMapSequence = _ref.collectionMapSequence, newCollectionName = _ref.newCollectionName, currentSeek = _ref.currentSeek, scname = _ref.scname;
          _context.next = 3;
          return regeneratorRuntime.awrap(ProcessData(result, collection, change, collectionMapSequence, newCollectionName, currentSeek));

        case 3:
          response = _context.sent;

          if (!(response == 1)) {
            _context.next = 15;
            break;
          }

          _context.prev = 5;
          _context.next = 8;
          return regeneratorRuntime.awrap(meta.upsert({
            name: scname,
            seek: currentSeek,
            pvtCollection: newCollectionName
          }));

        case 8:
          _context.next = 13;
          break;

        case 10:
          _context.prev = 10;
          _context.t0 = _context["catch"](5);
          console.log('Error while upserting:', _context.t0);

        case 13:
          _context.next = 16;
          break;

        case 15:
          console.log("not updating meta due to error");

        case 16:
        case "end":
          return _context.stop();
      }
    }
  }, null, null, [[5, 10]]);
}

function workerEvent(_ref2, cb) {
  var result, username, collection, change, collectionMapSequence, newCollectionName, currentSeek, scname, response, output;
  return regeneratorRuntime.async(function workerEvent$(_context2) {
    while (1) {
      switch (_context2.prev = _context2.next) {
        case 0:
          result = _ref2.result, username = _ref2.username, collection = _ref2.collection, change = _ref2.change, collectionMapSequence = _ref2.collectionMapSequence, newCollectionName = _ref2.newCollectionName, currentSeek = _ref2.currentSeek, scname = _ref2.scname;
          _context2.prev = 1;
          _context2.next = 4;
          return regeneratorRuntime.awrap(qryChain(result, username));

        case 4:
          response = _context2.sent;

          if (!response.data) {
            _context2.next = 22;
            break;
          }

          _context2.next = 8;
          return regeneratorRuntime.awrap(ProcessData(response.data, collection, change, collectionMapSequence, newCollectionName, currentSeek));

        case 8:
          output = _context2.sent;
          console.log("Processor Response>  ".concat(output, " block - ").concat(currentSeek));

          if (!(output == 1 || output == -1)) {
            _context2.next = 21;
            break;
          }

          _context2.prev = 11;
          _context2.next = 14;
          return regeneratorRuntime.awrap(meta.upsert({
            name: scname,
            seek: currentSeek,
            pvtCollection: scname
          }));

        case 14:
          _context2.next = 19;
          break;

        case 16:
          _context2.prev = 16;
          _context2.t0 = _context2["catch"](11);
          console.log('Error while upserting:', _context2.t0);

        case 19:
          _context2.next = 22;
          break;

        case 21:
          throw new Error("not updating meta due to error");

        case 22:
          _context2.next = 28;
          break;

        case 24:
          _context2.prev = 24;
          _context2.t1 = _context2["catch"](1);
          console.log("I Quit!", _context2.t1);
          process.exit(1);

        case 28:
        case "end":
          return _context2.stop();
      }
    }
  }, null, null, [[1, 24], [11, 16]]);
}

function sleep(time) {
  return new Promise(function (resolve) {
    return setTimeout(resolve, time);
  });
}

var getRegisteredUsersOneTime = function getRegisteredUsersOneTime(username, org) {
  if (!myUser) {
    myUser = helper.getRegisteredUsers(username, org);
  }

  logger.debug('the user object is ' + JSON.stringify(myUser));
  return myUser;
};

var listenPeerEvents = function listenPeerEvents(username, org, eventPeer, ccid, eventname, callback, channelName) {
  var queryInterval, currentValue, currentSeek, data, _data, client, channel, eventhubs, eh, collectionMapSequence, collectionSeq, LastProcessed, url, ch, configConnection;

  return regeneratorRuntime.async(function listenPeerEvents$(_context7) {
    while (1) {
      switch (_context7.prev = _context7.next) {
        case 0:
          replicatorqueue = require('../lib/db/postgres').sequelize.models[global.config.replicatorQueueName];
          meta = require('../lib/db/postgres').sequelize.models['meta'];
          queryInterval = undefined;
          queryInterval = setTimeout(QueryQueue, 5000, username, callback);
          currentValue = 0;
          currentSeek = '';
          _context7.prev = 6;
          _context7.next = 9;
          return regeneratorRuntime.awrap(meta.findOne({
            where: {
              name: "".concat(channelName, "_").concat(ccid)
            },
            raw: true
          }));

        case 9:
          _data = _context7.sent;
          currentSeek = _data.seek;
          console.log("seek found!!!", currentSeek);
          currentValue = currentSeek;
          _context7.next = 18;
          break;

        case 15:
          _context7.prev = 15;
          _context7.t0 = _context7["catch"](6);
          console.log("seek not found!!!");

        case 18:
          console.log('username... ' + JSON.stringify(username));
          console.log('Orgination... ' + JSON.stringify(org));
          console.log('channelName... ' + JSON.stringify(channelName));
          console.log('ccid... ' + JSON.stringify(ccid));
          console.log('eventname... ' + JSON.stringify(eventname));
          _context7.next = 25;
          return regeneratorRuntime.awrap(helper.getClientForOrg(org, username, channelName));

        case 25:
          client = _context7.sent;
          channel = client.getChannel(channelName);
          eventhubs = channel.getChannelEventHubsForOrg();
          eh = eventhubs[0];
          collectionMapSequence = {};
          collectionSeq = {};
          console.log("Pub-sub started at blkNo-".concat(currentValue));
          global.config.replicatorSource = global.config.replicatorSource || "EVENTHUB_GETDATABYKEY";
          global.config.replicatorTarget = global.config.replicatorTarget || "EVENT_QUEUE_AND_DB";
          console.log("global.config.replicatorSource ==== ", global.config.replicatorSource);

          if (global.config.replicatorSource == "EVENTHUB_FETCHFROMCOUCH" || global.config.replicatorSource == "EVENTHUB_GETDATABYKEY") {
            eh.registerChaincodeEvent(ccid, eventname, function (event, blockNum, txID, status) {
              currentValue = parseInt(blockNum) || 0;

              if (status && status === 'VALID') {
                LastProcessed = currentValue;
                var evt = JSON.parse(event.payload.toString('utf8'));
                var msg = "[got event][C: ".concat(channelName, "-").concat(currentValue, "] ... ").concat(event.event_name, " - ").concat(event.tx_id, " - SMC: ").concat(event.chaincode_id, " - ").concat(shouldbedelivered);
                console.log(msg);

                if (evt.eventName == 'typeDataSync') {
                  console.log("Performing Typedata Synchronization!!!!");
                  return typedata.sync(evt, username);
                }

                if (evt.eventName == 'stateSetTransitionSync') {
                  console.log("Performing StateSetTransition Synchronization!!!!");
                  return graph.sync(evt, username);
                }

                if (evt.eventName == 'errorCodeSync') {
                  console.log("Performing ErrorCode Synchronization!!!!");
                  return errorCodes.sync(evt, username);
                }

                if (evt.eventName == 'entitySync') {
                  console.log("Performing Entity Synchronization!!!!");
                  return entity.sync(evt, username);
                }

                if (evt.events) {
                  evt.events.forEach(function _callee(element) {
                    var key;
                    return regeneratorRuntime.async(function _callee$(_context3) {
                      while (1) {
                        switch (_context3.prev = _context3.next) {
                          case 0:
                            key = element;

                            try {
                              _.set(element, 'block_num', blockNum);

                              _.set(element, 'txid', txID);

                              _.set(element, 'eventName', evt.eventName);

                              if (evt.additionalData) _.set(element, 'additionalData', evt.additionalData);
                              queueEvents.push({
                                result: element,
                                username: username,
                                scname: "".concat(channelName, "_").concat(event.chaincode_id),
                                currentSeek: currentValue
                              });
                            } catch (err) {
                              replicatorqueue.create({
                                key: key,
                                status: status,
                                block_num: blockNum,
                                txid: txID,
                                eventName: evt.eventName,
                                isprocessed: false
                              })["catch"](function (ex) {
                                console.log(ex);
                              });
                              console.log(err);
                            }

                          case 2:
                          case "end":
                            return _context3.stop();
                        }
                      }
                    });
                  });
                } else {
                  // console.log(`Ignoring no event data found against block commit - ${LastProcessed}!!`);
                  return callback([]);
                }
              }
            }, function (err) {
              console.log('Oh snap!' + err);
              console.log('reconnecting in 10 seconds!!!');
              setTimeout(function () {
                listenPeerEvents(username, org, eventPeer, ccid, eventname, callback, channelName);
              }, 10000);
            }, {
              startBlock: currentValue
            });
          } // Event Hub with query from Smart contract


          url = crypto.decrypt(global.config.amqp.url);

          if (connection) {
            _context7.next = 41;
            break;
          }

          _context7.next = 40;
          return regeneratorRuntime.awrap(amqp.connect(url, {
            clientProperties: {
              connection_name: "cipher_replicator_".concat(process.pid)
            }
          }));

        case 40:
          connection = _context7.sent;

        case 41:
          _context7.next = 43;
          return regeneratorRuntime.awrap(connection.createChannel());

        case 43:
          ch = _context7.sent;
          _context7.next = 46;
          return regeneratorRuntime.awrap(ch.assertExchange('logs', 'fanout', {
            durable: true
          }));

        case 46:
          _context7.next = 48;
          return regeneratorRuntime.awrap(ch.assertQueue(global.config.amqp.queueName, {
            durable: true
          }));

        case 48:
          _context7.next = 50;
          return regeneratorRuntime.awrap(sleep(3000));

        case 50:
          _context7.next = 52;
          return regeneratorRuntime.awrap(connection.close());

        case 52:
          connection = undefined;
          _context7.next = 55;
          return regeneratorRuntime.awrap(sleep(3000));

        case 55:
          if (!(global.config.replicatorSource == "EVENTHUB_GETDATABYKEY")) {
            _context7.next = 60;
            break;
          }

          console.log('Event Hub Connected-local hyp!!');
          eh.connect(true);
          _context7.next = 66;
          break;

        case 60:
          if (!(global.config.replicatorSource == "EVENTHUB_FETCHFROMCOUCH")) {
            _context7.next = 65;
            break;
          }

          _context7.next = 63;
          return regeneratorRuntime.awrap(query.couchInit(function () {
            console.log('Event Hub Connected - local couch!!');
            eh.connect(true);
          }));

        case 63:
          _context7.next = 66;
          break;

        case 65:
          //Case of Replicator Service
          if (global.config.replicatorSource == "COUCH_FOLLOW") {
            configConnection = global.config.blockChainConfiguration;
            query.couchInit(function _callee3(dataCouch) {
              var _loop, collection, _ret;

              return regeneratorRuntime.async(function _callee3$(_context6) {
                while (1) {
                  switch (_context6.prev = _context6.next) {
                    case 0:
                      _loop = function _loop(collection) {
                        var dbConnection, channelName, collectionName, newCollectionName, i, partyName, mapSequence, currSeek, followConfig;
                        return regeneratorRuntime.async(function _loop$(_context5) {
                          while (1) {
                            switch (_context5.prev = _context5.next) {
                              case 0:
                                dbConnection = "".concat(configConnection.couch.protocall, "://").concat(configConnection.couch.username, ":").concat(configConnection.couch.password, "@").concat(configConnection.couch.ip, ":").concat(configConnection.couch.port, "/").concat(dataCouch[collection]);
                                channelName = dataCouch[collection].substring(0, dataCouch[collection].indexOf('_'));
                                collectionName = dataCouch[collection].substring(dataCouch[collection].indexOf('$$p') + 3, dataCouch[collection].length);
                                newCollectionName = '';

                                for (i = 0; i < collectionName.length; i++) {
                                  if (collectionName.charAt(i) == '$') {
                                    newCollectionName += collectionName.charAt(i + 1).toUpperCase();
                                    i++;
                                  } else {
                                    newCollectionName += collectionName.charAt(i);
                                  }
                                }

                                _.set(collectionSeq, channelName + "_" + newCollectionName, 0);

                                if (!(allowed && allowed.length || denied && denied.length)) {
                                  _context5.next = 15;
                                  break;
                                }

                                partyName = newCollectionName && newCollectionName !== "" ? newCollectionName.split("_")[0] : undefined;

                                if (!(allowed && allowed.length)) {
                                  _context5.next = 13;
                                  break;
                                }

                                if (allowed.includes(partyName)) {
                                  _context5.next = 11;
                                  break;
                                }

                                return _context5.abrupt("return", "continue");

                              case 11:
                                _context5.next = 15;
                                break;

                              case 13:
                                if (!(denied && denied.length && denied.includes(partyName))) {
                                  _context5.next = 15;
                                  break;
                                }

                                return _context5.abrupt("return", "continue");

                              case 15:
                                _context5.next = 17;
                                return regeneratorRuntime.awrap(meta.findAll({
                                  where: {
                                    name: global.config.blockChainConfiguration.smartContract,
                                    pvtCollection: channelName + "_" + newCollectionName
                                  },
                                  raw: true
                                }));

                              case 17:
                                data = _context5.sent;
                                mapSequence = {};
                                data.forEach(function (element) {
                                  if (element.pvtCollection != '-') {
                                    _.set(mapSequence, element.pvtCollection, element.seek);
                                  }
                                });
                                currSeek = _.get(mapSequence, channelName + "_" + newCollectionName, 'now'); //88245

                                console.log('Start Seek', currSeek + "...", "for collection ", newCollectionName);
                                console.log('Seek', currSeek);
                                followConfig = {
                                  db: dbConnection,
                                  include_docs: true,
                                  max_retry_seconds: 3,
                                  inactivity_ms: inactivity_ms || 1000 * 3,
                                  since: currSeek,
                                  filter: function filter(doc, req) {
                                    return doc["documentName"] || doc["DocumentName"];
                                  }
                                };
                                currSeek === 'now' || String(currSeek).trim() === '' ? delete followConfig["since"] : {};
                                follow(followConfig, function _callee2(error, change) {
                                  return regeneratorRuntime.async(function _callee2$(_context4) {
                                    while (1) {
                                      switch (_context4.prev = _context4.next) {
                                        case 0:
                                          if (!error) {
                                            currentSeek = change.seq;
                                            console.log("adding change to in-memory queue!!", dataCouch[collection]);
                                            queueReplication.push({
                                              result: change.doc,
                                              collection: newCollectionName,
                                              change: change.seq,
                                              collectionMapSequence: collectionMapSequence,
                                              newCollectionName: channelName + "_" + newCollectionName,
                                              currentSeek: currentSeek,
                                              scname: global.config.blockChainConfiguration.smartContract
                                            });
                                          } else {
                                            console.log("cannot follow !!", error);
                                          }

                                        case 1:
                                        case "end":
                                          return _context4.stop();
                                      }
                                    }
                                  });
                                }); // break;

                              case 26:
                              case "end":
                                return _context5.stop();
                            }
                          }
                        });
                      };

                      _context6.t0 = regeneratorRuntime.keys(dataCouch);

                    case 2:
                      if ((_context6.t1 = _context6.t0()).done) {
                        _context6.next = 11;
                        break;
                      }

                      collection = _context6.t1.value;
                      _context6.next = 6;
                      return regeneratorRuntime.awrap(_loop(collection));

                    case 6:
                      _ret = _context6.sent;

                      if (!(_ret === "continue")) {
                        _context6.next = 9;
                        break;
                      }

                      return _context6.abrupt("continue", 2);

                    case 9:
                      _context6.next = 2;
                      break;

                    case 11:
                    case "end":
                      return _context6.stop();
                  }
                }
              });
            });
            console.log('Event Hub Connected - local couch follw mode!!');
            eh.connect(true);
          } else {
            console.log("Invalid Service Type");
          }

        case 66:
        case "end":
          return _context7.stop();
      }
    }
  }, null, null, [[6, 15]]);
};

var ProcessData = function ProcessData(result, collection, change, collectionMapSequence, newCollectionName, currentSeek, type) {
  var documentKey, documentName, eventData, start, _final, flag;

  return regeneratorRuntime.async(function ProcessData$(_context8) {
    while (1) {
      switch (_context8.prev = _context8.next) {
        case 0:
          result = _.set(result, 'eventName', type);
          result = _.set(result, '__collection', collection);
          documentKey = result.documentKey || result.key || result.Key;
          documentName = result.documentName || result.DocumentName;
          eventData = {
            _metaData: {
              block_num: 1000,
              txnid: documentKey,
              status: 'VALID'
            },
            eventData: _objectSpread({
              InternalEventKey: result.Key,
              InternalEventName: type,
              DocumentKey: documentKey,
              DocumentName: documentName
            }, result),
            extra: {
              tranxData: result
            }
          };
          start = require('../lib/start');
          flag = false;

          _.set(collectionMapSequence, newCollectionName, currentSeek); // logic to send to queue and replicate    


          if (!(global.config.replicatorTarget == "EVENT_QUEUE_AND_DB" || global.config.replicatorTarget == "EVENT_QUEUE")) {
            _context8.next = 13;
            break;
          }

          flag = true;
          _context8.next = 12;
          return regeneratorRuntime.awrap(connectQueueService(eventData));

        case 12:
          _final = _context8.sent;

        case 13:
          if (!(global.config.replicatorTarget == "EVENT_QUEUE_AND_DB" || global.config.replicatorTarget == "DB_ONLY")) {
            _context8.next = 16;
            break;
          }

          flag = true;
          return _context8.abrupt("return", start.eventHandle(eventData));

        case 16:
          if (flag) {
            _context8.next = 18;
            break;
          }

          throw new Error("SERVICE MODE MISMATCH" + global.config.replicatorTarget);

        case 18:
          return _context8.abrupt("return", -1);

        case 19:
        case "end":
          return _context8.stop();
      }
    }
  });
};

var QueryQueue = function QueryQueue(username, callback) {
  var list, _iteratorNormalCompletion, _didIteratorError, _iteratorError, _iterator, _step, qresp, data;

  return regeneratorRuntime.async(function QueryQueue$(_context9) {
    while (1) {
      switch (_context9.prev = _context9.next) {
        case 0:
          replicatorqueue = require('../lib/db/postgres').sequelize.models[global.config.replicatorQueueName];
          _context9.next = 3;
          return regeneratorRuntime.awrap(replicatorqueue.findAll({
            where: {
              isprocessed: false,
              retry: _defineProperty({}, Op.lte, global.config.retryCnt || 3)
            },
            limit: global.config.retryChunkSize || 5,
            order: [['id', 'ASC'], ['block_num', 'ASC']],
            raw: true
          })["catch"](function (ex) {
            console.log(ex);
          }));

        case 3:
          list = _context9.sent;
          _iteratorNormalCompletion = true;
          _didIteratorError = false;
          _iteratorError = undefined;
          _context9.prev = 7;
          _iterator = list[Symbol.iterator]();

        case 9:
          if (_iteratorNormalCompletion = (_step = _iterator.next()).done) {
            _context9.next = 20;
            break;
          }

          qresp = _step.value;

          if (!(qresp && qresp.key)) {
            _context9.next = 17;
            break;
          }

          if (global.config.db == 'mssql') {
            qresp.key = JSON.parse(qresp.key);
          }

          _context9.next = 15;
          return regeneratorRuntime.awrap(qryChain(qresp.key, username));

        case 15:
          data = _context9.sent;
          deleteDBQueue(qresp);

        case 17:
          _iteratorNormalCompletion = true;
          _context9.next = 9;
          break;

        case 20:
          _context9.next = 26;
          break;

        case 22:
          _context9.prev = 22;
          _context9.t0 = _context9["catch"](7);
          _didIteratorError = true;
          _iteratorError = _context9.t0;

        case 26:
          _context9.prev = 26;
          _context9.prev = 27;

          if (!_iteratorNormalCompletion && _iterator["return"] != null) {
            _iterator["return"]();
          }

        case 29:
          _context9.prev = 29;

          if (!_didIteratorError) {
            _context9.next = 32;
            break;
          }

          throw _iteratorError;

        case 32:
          return _context9.finish(29);

        case 33:
          return _context9.finish(26);

        case 34:
          if (!(list && list.length)) {
            _context9.next = 39;
            break;
          }

          _context9.next = 37;
          return regeneratorRuntime.awrap(snooze(2000));

        case 37:
          _context9.next = 41;
          break;

        case 39:
          _context9.next = 41;
          return regeneratorRuntime.awrap(snooze(8000));

        case 41:
          return _context9.abrupt("return", QueryQueue(username));

        case 42:
        case "end":
          return _context9.stop();
      }
    }
  }, null, null, [[7, 22, 26, 34], [27,, 29, 33]]);
}; // hyperledger


var qryChain = function qryChain(element, username) {
  return regeneratorRuntime.async(function qryChain$(_context10) {
    while (1) {
      switch (_context10.prev = _context10.next) {
        case 0:
          return _context10.abrupt("return", callHyperledger(element, username));

        case 1:
        case "end":
          return _context10.stop();
      }
    }
  });
};

function updateDBQueueFailure(qresp) {
  if (qresp.id) {
    replicatorqueue.update({
      isprocessed: false,
      retry: qresp.retry + 1
    }, {
      returning: true,
      where: {
        id: qresp.id
      }
    });
  }
}

function deleteDBQueue(qresp) {
  if (qresp.id) {
    replicatorqueue.destroy({
      where: {
        id: qresp.id
      }
    });
  }
}

function callHyperledger(args, username) {
  var config;
  return regeneratorRuntime.async(function callHyperledger$(_context11) {
    while (1) {
      switch (_context11.prev = _context11.next) {
        case 0:
          config = global.config.blockChainConfiguration;
          return _context11.abrupt("return", query.queryChaincode(config.channel, config.smartContract, [args.Key, args.Collection], 'GetDataByKey', username, config.network));

        case 2:
        case "end":
          return _context11.stop();
      }
    }
  });
}

function connectQueueService(request) {
  var url, channel;
  return regeneratorRuntime.async(function connectQueueService$(_context12) {
    while (1) {
      switch (_context12.prev = _context12.next) {
        case 0:
          url = crypto.decrypt(global.config.amqp.url);

          if (connection) {
            _context12.next = 5;
            break;
          }

          _context12.next = 4;
          return regeneratorRuntime.awrap(amqp.connect(url, {
            clientProperties: {
              connection_name: "cipher_replicator_".concat(process.pid)
            }
          }));

        case 4:
          connection = _context12.sent;

        case 5:
          shouldbedelivered++;
          _context12.next = 8;
          return regeneratorRuntime.awrap(connection.createChannel());

        case 8:
          channel = _context12.sent;
          _context12.next = 11;
          return regeneratorRuntime.awrap(channel.assertExchange('logs', 'fanout', {
            durable: true
          }));

        case 11:
          _context12.next = 13;
          return regeneratorRuntime.awrap(channel.assertQueue(global.config.amqp.queueName, {
            durable: true
          }));

        case 13:
          _context12.next = 15;
          return regeneratorRuntime.awrap(channel.bindQueue(global.config.amqp.queueName, 'logs'));

        case 15:
          _context12.next = 17;
          return regeneratorRuntime.awrap(channel.publish('logs', '', Buffer.from(JSON.stringify(request))));

        case 17:
          console.log("Message Published successfully! - txid ".concat(_.get(request, '_metaData.txnid', "N/A"))); // connection.close();

          return _context12.abrupt("return");

        case 19:
        case "end":
          return _context12.stop();
      }
    }
  });
}

exports.listenPeerEvents = listenPeerEvents;
exports.getRegisteredUsersOneTime = getRegisteredUsersOneTime;