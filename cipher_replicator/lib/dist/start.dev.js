'use strict';

function ownKeys(object, enumerableOnly) { var keys = Object.keys(object); if (Object.getOwnPropertySymbols) { var symbols = Object.getOwnPropertySymbols(object); if (enumerableOnly) symbols = symbols.filter(function (sym) { return Object.getOwnPropertyDescriptor(object, sym).enumerable; }); keys.push.apply(keys, symbols); } return keys; }

function _objectSpread(target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i] != null ? arguments[i] : {}; if (i % 2) { ownKeys(source, true).forEach(function (key) { _defineProperty(target, key, source[key]); }); } else if (Object.getOwnPropertyDescriptors) { Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)); } else { ownKeys(source).forEach(function (key) { Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key)); }); } } return target; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

var errorTable;

var db = require('./db/postgres');

var Sequelize = require('sequelize');

var _ = require('lodash');

var sequelize = db.sequelize;
sequelize.options.logging = false;

var pgInstance = require("./db/rawPostgres");

var schemaConfig = require("./schemaConfig.json");

var flatten = require('flat');

module.exports = {
  server: server,
  eventHandle: eventHandle
};
var listModels = {};

function createMetaServer() {
  var fields, index;
  return regeneratorRuntime.async(function createMetaServer$(_context) {
    while (1) {
      switch (_context.prev = _context.next) {
        case 0:
          fields = {
            name: {
              type: Sequelize.STRING(500)
            },
            value: {
              type: Sequelize.TEXT
            },
            seek: {
              type: Sequelize.TEXT
            },
            pvtCollection: {
              type: Sequelize.STRING(500)
            }
          };
          index = {
            indexes: [{
              unique: true,
              fields: ["name", "pvtCollection"]
            }]
          };
          sequelize.define('meta', fields, index);

        case 3:
        case "end":
          return _context.stop();
      }
    }
  });
}

function createErrorTable() {
  var errorTableFields, j, field;
  return regeneratorRuntime.async(function createErrorTable$(_context2) {
    while (1) {
      switch (_context2.prev = _context2.next) {
        case 0:
          try {
            errorTableFields = [];

            for (j = 0; j < schemaConfig.errorTable.columns.length; j++) {
              field = schemaConfig.errorTable.columns[j];

              if (field['typeData']['type'] === 'STRING') {
                field['typeData']['type'] = Sequelize.STRING(field['typeData']['length']);
              } else {
                field['typeData']['type'] = Sequelize[field['typeData']['type']];
              }

              errorTableFields[field['name']] = field['typeData'];
            }

            sequelize.define(schemaConfig.errorTable.name, errorTableFields, schemaConfig.errorTable.otherOptions);
            errorTable = sequelize.models[schemaConfig.errorTable.name];
          } catch (ex) {
            console.log(ex);
          }

        case 1:
        case "end":
          return _context2.stop();
      }
    }
  });
}

function setUpDBTables() {
  var fieldList, j, field, additionalFields, _j, _field, i, data, fields, _j2, _field2;

  return regeneratorRuntime.async(function setUpDBTables$(_context3) {
    while (1) {
      switch (_context3.prev = _context3.next) {
        case 0:
          try {
            console.log("replicatorQueue Name -> ", global.config.replicatorQueueName);
            fieldList = {};

            for (j = 0; j < schemaConfig[global.config.replicatorQueueName].length; j++) {
              field = schemaConfig[global.config.replicatorQueueName][j];

              if (field['typeData']['type'] === 'STRING') {
                field['typeData']['type'] = Sequelize.STRING(field['typeData']['length']);
              } else {
                field['typeData']['type'] = Sequelize[field['typeData']['type']];
              }

              fieldList[field['name']] = field['typeData'];
            }

            fieldList = _objectSpread({}, fieldList);
            sequelize.define(global.config.replicatorQueueName, fieldList, {
              freezeTableName: true
            });
            additionalFields = {};

            for (_j = 0; _j < schemaConfig.metaDataFields.length; _j++) {
              _field = schemaConfig.metaDataFields[_j];

              if (_field['typeData']['type'] === 'STRING') {
                _field['typeData']['type'] = Sequelize.STRING(_field['typeData']['length']);
              } else {
                _field['typeData']['type'] = Sequelize[_field['typeData']['type']];
              }

              additionalFields[_field['name']] = _field['typeData'];
            }

            for (i = 0; i < global.config.schema.length; i++) {
              data = global.config.schema[i];
              fields = {}; //Normal Columns

              for (_j2 = 0; _j2 < data.columns.length; _j2++) {
                _field2 = data.columns[_j2];

                if (_field2['typeData']['type'] === 'STRING') {
                  _field2['typeData']['type'] = Sequelize.STRING(_field2['typeData']['length']);
                } else {
                  _field2['typeData']['type'] = Sequelize[_field2['typeData']['type']];
                }

                fields[_field2['name']] = _field2['typeData'];
              } //Generic Columns


              fields = _objectSpread({}, fields, {}, additionalFields);

              if (data.hasOwnProperty('otherOptions')) {
                data.otherOptions['freezeTableName'] = true;
              } else {
                data['otherOptions'] = {
                  "freezeTableName": true
                };
              }

              _.set(listModels, data.name, sequelize.define(data.name, fields, data.otherOptions));
            }
          } catch (ex) {
            console.log(ex);
          }

        case 1:
        case "end":
          return _context3.stop();
      }
    }
  });
}

function buildPSQLDynamically(field) {
  try {
    var rest_of_query = '',
        sequelize_field = {};

    if (global.config.db == "pg" && field.typeData.type.toLowerCase() == "json") {
      sequelize_field = {
        type: Sequelize.JSON,
        allowNull: true
      };
      if (rest_of_query == '') rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" json ';
    } else if (field.typeData.type.toLowerCase() == "bigint") {
      sequelize_field = {
        type: Sequelize.BIGINT,
        allowNull: true
      };
      rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" bigint ';
    } else if (field.typeData.type.toLowerCase() == "integer") {
      sequelize_field = {
        type: Sequelize.INTEGER,
        allowNull: true
      };
      if (global.config.db == "pg") rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" integer ';else rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" int ';
    } else if (field.typeData.type.toLowerCase() == "float") {
      sequelize_field = {
        type: Sequelize.FLOAT,
        allowNull: true
      };
      rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" float ';
    } else if (field.typeData.type.toLowerCase() == "boolean") {
      sequelize_field = {
        type: Sequelize.BOOLEAN,
        allowNull: true,
        defaultValue: false
      };
      rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" boolean DEFAULT false ';
    } else if (field.typeData.type.toLowerCase() == "varchar" || field.typeData.type.toLowerCase() == "string") {
      if (field.typeData.hasOwnProperty('length')) {
        var length = parseInt(field.typeData.length);
        sequelize_field = {
          type: Sequelize.STRING(length),
          allowNull: true,
          defaultValue: false
        };
        rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" varchar(' + length + ')  ';
      } else {
        sequelize_field = {
          type: Sequelize.STRING,
          allowNull: true,
          defaultValue: false
        };
        rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" varchar ';
      }
    } else if (field.typeData.type.toLowerCase() == "nvarchar") {
      //valid for mssql
      if (field.typeData.hasOwnProperty('length')) {
        var _length = 255;
        if (parseInt(field.typeData.length) < 255) _length = parseInt(field.typeData.length);
        sequelize_field = {
          type: Sequelize.STRING(_length),
          allowNull: true,
          defaultValue: false
        };
        rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" nvarchar(' + _length + ') ';
      } else {
        sequelize_field = {
          type: Sequelize.STRING,
          allowNull: true,
          defaultValue: false
        };
        rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" nvarchar ';
      }
    } else if (field.typeData.type.toLowerCase() == "nchar") {
      //valid for mssql
      if (field.typeData.hasOwnProperty('length')) {
        var _length2 = 4000;
        if (parseInt(field.typeData.length) < 4000) _length2 = parseInt(field.typeData.length);
        sequelize_field = {
          type: Sequelize.STRING(_length2),
          allowNull: true,
          defaultValue: false
        };
        rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" nchar(' + _length2 + ')  ';
      } else {
        sequelize_field = {
          type: Sequelize.STRING,
          allowNull: true,
          defaultValue: false
        };
        rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" nchar ';
      }
    } else if (field.typeData.type.toLowerCase() == "char") {
      if (field.typeData.hasOwnProperty('length')) {
        var _length3 = 4000;
        if (parseInt(field.typeData.length) < 4000) _length3 = parseInt(field.typeData.length);
        sequelize_field = {
          type: Sequelize.STRING(_length3),
          allowNull: true,
          defaultValue: false
        };
        rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" char(' + _length3 + ')  ';
      } else {
        sequelize_field = {
          type: Sequelize.STRING,
          allowNull: true,
          defaultValue: false
        };
        rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" char ';
      }
    } else if (global.config.db == "pg" && field.typeData.type.toLowerCase() == "array" && field.typeData.hasOwnProperty('typeOfArray')) {
      var typeOfArray = field.typeData.typeOfArray.toLowerCase();

      if (typeOfArray == "integer") {
        sequelize_field = {
          type: Sequelize.ARRAY(Sequelize.INTEGER),
          allowNull: true
        };
        rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" integer[] ';
      } else if (typeOfArray == "varchar" || typeOfArray == "string") {
        sequelize_field = {
          type: Sequelize.ARRAY(Sequelize.STRING),
          allowNull: true,
          defaultValue: false
        };
        rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" varchar[] ';
      } else if (typeOfArray == "text") {
        sequelize_field = {
          type: Sequelize.ARRAY(Sequelize.TEXT),
          allowNull: true
        };
        rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" text[] ';
      } else if (typeOfArray == "char") {
        sequelize_field = {
          type: Sequelize.ARRAY(Sequelize.STRING),
          allowNull: true,
          defaultValue: false
        };
        rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" char[] ';
      } else if (typeOfArray == "bigint") {
        sequelize_field = {
          type: Sequelize.ARRAY(Sequelize.BIGINT),
          allowNull: true
        };
        rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" bigint[] ';
      } else if (typeOfArray == "float") {
        sequelize_field = {
          type: Sequelize.ARRAY(Sequelize.FLOAT),
          allowNull: true
        };
        rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" float[] ';
      }
    } else {
      sequelize_field = {
        type: Sequelize.TEXT,
        allowNull: true
      };
      rest_of_query += ' ADD COLUMN IF NOT EXISTS "' + field.name + '" text ';
    }

    return {
      "rest_of_query": rest_of_query,
      "sequelize_field": sequelize_field
    };
  } catch (ex) {
    console.log(ex);
  }
}

function updateSchemaFromConfig() {
  var _loop, i;

  return regeneratorRuntime.async(function updateSchemaFromConfig$(_context5) {
    while (1) {
      switch (_context5.prev = _context5.next) {
        case 0:
          try {
            _loop = function _loop(i) {
              var data = global.config.schemaConfig[i];
              var fields = {},
                  isNew = false;
              var client = pgInstance.getDb(),
                  query = "SELECT EXISTS (\n          SELECT FROM information_schema.tables \n          WHERE  table_schema = 'public'\n          AND    table_name   = '".concat(data.name, "'\n          )"); // Check if table already existing or note

              client.query(query, function _callee(err, res) {
                var queryCreateTable, alterQuery, rest_of_query, j, field, _j3, _field3, psqlData;

                return regeneratorRuntime.async(function _callee$(_context4) {
                  while (1) {
                    switch (_context4.prev = _context4.next) {
                      case 0:
                        if (!err) {
                          _context4.next = 3;
                          break;
                        }

                        console.error(err);
                        throw new Error('Error while checking if table exists');

                      case 3:
                        if (res.rows[0].exists) {
                          _context4.next = 9;
                          break;
                        }

                        isNew = true;
                        queryCreateTable = "CREATE TABLE \"".concat(data.name, "\"(\n              id SERIAL\n            )");
                        _context4.next = 8;
                        return regeneratorRuntime.awrap(client.query(queryCreateTable));

                      case 8:
                        console.log('Table is created successfully');

                      case 9:
                        // Code snippet to Alter the table to add missing columns if any
                        alterQuery = "ALTER TABLE \"".concat(data.name, "\""), rest_of_query = '';

                        for (j = 0; j < schemaConfig.metaDataFields.length; j++) {
                          field = schemaConfig.metaDataFields[j];

                          if (isNew) {
                            if (field['typeData']['type'] === 'STRING') {
                              if (rest_of_query == '') rest_of_query += " ADD COLUMN IF NOT EXISTS \"".concat(field['name'], "\" text ");else rest_of_query += " , ADD COLUMN IF NOT EXISTS \"".concat(field['name'], "\" text ");
                            } else {
                              if (rest_of_query == '') rest_of_query += " ADD COLUMN IF NOT EXISTS \"".concat(field['name'], "\" ").concat(field['typeData']['type'], " ");else rest_of_query += " , ADD COLUMN IF NOT EXISTS \"".concat(field['name'], "\" ").concat(field['typeData']['type'], " ");
                            }

                            fields[field['name']] = field['typeData'];
                          } else {
                            fields[field['name']] = field['typeData'];
                          }
                        }

                        if (data.columns.length > 0 || isNew) {
                          for (_j3 = 0; _j3 < data.columns.length; _j3++) {
                            _field3 = data.columns[_j3];
                            psqlData = buildPSQLDynamically(_field3);
                            if (rest_of_query == '') rest_of_query += psqlData.rest_of_query;else {
                              rest_of_query += ' , ';
                              rest_of_query += psqlData.rest_of_query;
                            }
                            fields[_field3['name']] = psqlData.sequelize_field;
                          }

                          if (rest_of_query != '') {
                            alterQuery += rest_of_query;
                            client.query(alterQuery, function (err, res) {
                              if (err) {
                                console.error(err);
                                client.end();
                                return;
                              }

                              console.log('Table is successfully altered');
                              return;
                            });
                          } // Update sequelize model with the new fields just added


                          fields = _objectSpread({}, fields);

                          if (data.hasOwnProperty('otherOptions')) {
                            data.otherOptions['freezeTableName'] = true;
                          } else {
                            data['otherOptions'] = {
                              "freezeTableName": true
                            };
                          }

                          _.set(listModels, data.name, sequelize.define(data.name, fields, data.otherOptions));
                        }

                        return _context4.abrupt("return");

                      case 13:
                      case "end":
                        return _context4.stop();
                    }
                  }
                });
              });
            };

            for (i = 0; i < global.config.schemaConfig.length; i++) {
              _loop(i);
            }
          } catch (err) {
            console.log(err);
          }

        case 1:
        case "end":
          return _context5.stop();
      }
    }
  });
}

function server() {
  var msg, _msg;

  return regeneratorRuntime.async(function server$(_context6) {
    while (1) {
      switch (_context6.prev = _context6.next) {
        case 0:
          console.log("Starting Server For Channel", global.config.blockChainConfiguration.channel);
          _context6.prev = 1;
          _context6.next = 4;
          return regeneratorRuntime.awrap(sequelize.authenticate());

        case 4:
          console.log('Connection has been established successfully.');
          console.log("Setting up MetaServer...");
          _context6.next = 8;
          return regeneratorRuntime.awrap(createMetaServer());

        case 8:
          console.log("Setting up ErrorTable...");
          _context6.next = 11;
          return regeneratorRuntime.awrap(createErrorTable());

        case 11:
          console.log("Setting up DB Tables...");
          _context6.next = 14;
          return regeneratorRuntime.awrap(setUpDBTables());

        case 14:
          console.log("Calling Sequalize Sync in background...");
          _context6.next = 17;
          return regeneratorRuntime.awrap(sequelize.sync());

        case 17:
          msg = 'Schema creation successfully done ...';
          console.log(msg);
          _context6.next = 21;
          return regeneratorRuntime.awrap(updateSchemaFromConfig());

        case 21:
          return _context6.abrupt("return", true);

        case 24:
          _context6.prev = 24;
          _context6.t0 = _context6["catch"](1);
          _msg = 'Schema creation has error ...';
          console.log(_context6.t0, _msg);
          return _context6.abrupt("return", false);

        case 29:
        case "end":
          return _context6.stop();
      }
    }
  }, null, null, [[1, 24]]);
}

function eventHandle(params) {
  var _pgInstance = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : null;

  var data = Object.assign(params['_metaData'], params['eventData'], params['extra']);
  data.key = data.DocumentKey;
  var flattenData = flatten(data);
  var stimulatedTable = global.config.schemaConfig.find(function (o) {
    return o.name === data.DocumentName;
  });

  if (stimulatedTable) {
    for (var _i = 0, _Object$keys = Object.keys(stimulatedTable.columns); _i < _Object$keys.length; _i++) {
      var key = _Object$keys[_i];

      if (stimulatedTable.columns[key].hasOwnProperty('path')) {
        var key_ = "".concat(stimulatedTable.columns[key].path);

        var columnType = _.trim(stimulatedTable.columns[key].typeData.type.toLowerCase());

        if (columnType.localeCompare('array') == 0) {
          var value = _.get(data, key_);

          if (!stimulatedTable.columns[key].typeData.hasOwnProperty('typeOfArray')) {
            data[stimulatedTable.columns[key].name] = JSON.stringify(value);
          } else {
            data[stimulatedTable.columns[key].name] = value;
          }
        } else {
          var _value = flattenData[key_];

          if (global.config.db == "mssql" && stimulatedTable.columns[key].typeData.type.toLowerCase() == "json") {
            data[stimulatedTable.columns[key].name] = JSON.stringify(_value);
          } else {
            data[stimulatedTable.columns[key].name] = _value;
          }
        }
      }
    }
  } // update data into table


  var flag = _.get(listModels, data.DocumentName, undefined);

  if (flag) {
    _.set(data.tranxData, '_rev', undefined);

    _.set(data.tranxData, '~version', undefined);

    if (global.config.db == "mssql") data.tranxData = JSON.stringify(data.tranxData);
    return flag.upsert(data).then(function (success) {
      var msg = "[upsert success][C: ".concat(global.config.blockChainConfiguration.channel, "] ... ").concat(data.DocumentName, " - ").concat(data.key, " - Queue ").concat(global.config.amqp.queueName);
      console.log(msg);
      return 1;
    })["catch"](function (err) {
      console.log("Error: while Executing");
      console.log(err);
      return -2; // process.exit(1);
    });
  } else {
    var msg = "Model [".concat(data.DocumentName, "] not found in [");
    console.log(msg, Object.keys(listModels).join(', '), ']');
    return Promise.resolve(-1);
  }
}