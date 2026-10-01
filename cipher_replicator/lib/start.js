'use strict';


let errorTable;
const db = require('./db/postgres');
const helper = require('../eventConnect/helper');
const Sequelize = require('sequelize');
const _ = require('lodash');
const sequelize = db.sequelize;
// sequelize.options.logging = false
const pgInstance = require("./db/rawPostgres");
const schemaConfig = require("./schemaConfig.json");
const { DataTypes } = require('sequelize');
const logger = helper.getLogger('Start.js');
const { REVERSE_REPLICATOR_STATUS } = require('../utils/constants');
//  import flatten from 'flat'

let flatten

async function loadFlat() {
  try {
    const { flatten } = await import('flat');
    return flatten;
  } catch (err) {
    logger.error(err)
  }
}

module.exports = {
  server,
  eventHandle
};
let listModels = {};

async function createMetaServer() {
  const fields = {
    name: {
      type: DataTypes.STRING(500)
    },
    value: {
      type: DataTypes.TEXT
    },
    seek: {
      type: DataTypes.TEXT
    },
    pvtCollection: {
      type: DataTypes.STRING(500)

    }
  }
  let index = {
    indexes: [{
      unique: true,
      fields: ["name", "pvtCollection"]
    }]
  }
  sequelize.define('meta', fields, index);
}

async function createMetaProxyServer() {
  const fields = {
    name: {
      type: DataTypes.STRING(500)
    },
    value: {
      type: DataTypes.TEXT
    },
    seek: {
      type: DataTypes.TEXT
    },
    pvtCollection: {
      type: DataTypes.STRING(500)

    }, error: {
      type: DataTypes.TEXT

    }
  }
  let index = {
    indexes: [{
      unique: true,
      fields: ["name", "pvtCollection"],

    }]
  }
  sequelize.define('meta_proxy', fields, index, { freezeTableName: true });
}

async function createErrorTable() {
  try {
    let errorTableFields = [];
    for (let j = 0; j < schemaConfig.errorTable.columns.length; j++) {
      let field = schemaConfig.errorTable.columns[j];
      if (field['typeData']['type'] === 'STRING') {
        field['typeData']['type'] = DataTypes.STRING(field['typeData']['length']);
      } else {
        field['typeData']['type'] = Sequelize[field['typeData']['type'] == "JSON" && global.config.db == "mssql" ? 'TEXT' : field['typeData']['type']];
      }
      errorTableFields[field['name']] = field['typeData'];
    }
    sequelize.define(schemaConfig.errorTable.name, errorTableFields, schemaConfig.errorTable.otherOptions);
    errorTable = sequelize.models[schemaConfig.errorTable.name];
  } catch (ex) {
    logger.error(ex)
  }
}

async function setUpDBTables() {
  try {
    // console.log("replicatorQueue Name -> ", global.config.replicatorQueueName);

    let fieldList = {};
    for (let j = 0; j < schemaConfig[global.config.replicatorQueueName].length; j++) {
      let field = schemaConfig[global.config.replicatorQueueName][j];
      if (field['typeData']['type'] === 'STRING') {
        field['typeData']['type'] = DataTypes.STRING(field['typeData']['length']);
      } else {
        field['typeData']['type'] = DataTypes[field['typeData']['type'] == "JSON" && global.config.db == "mssql" ? 'TEXT' : field['typeData']['type']];
      }
      fieldList[field['name']] = field['typeData'];
    }
    fieldList = {
      ...fieldList
    }
    sequelize.define(global.config.replicatorQueueName, fieldList, { freezeTableName: true });

    logger.info("replicatorQueue Name -> ", global.config.replicatorQueueName);

    // let fieldListProxy = {};
    // for (let j = 0; j < schemaConfig[global.config.replicatorQueueName].length; j++) {
    //   let field = schemaConfig[global.config.replicatorQueueName][j];
    //   if (field['typeData']['type'] === 'STRING') {
    //     field['typeData']['type'] = DataTypes.STRING(field['typeData']['length']);
    //   } else {
    //     field['typeData']['type'] = DataTypes[field['typeData']['type']];
    //   }
    //   fieldListProxy[field['name']] = field['typeData'];
    // }
    // fieldListProxy = {
    //   ...fieldListProxy
    // }

    // sequelize.define(global.config.replicatorQueueName, fieldListProxy, { freezeTableName: true });

    let additionalFields = {}
    for (let j = 0; j < schemaConfig.metaDataFields.length; j++) {
      let field = schemaConfig.metaDataFields[j];
      if (field['typeData']['type'] === 'STRING') {
        field['typeData']['type'] = DataTypes.STRING(field['typeData']['length']);
      } else {
        field['typeData']['type'] = DataTypes[field['typeData']['type'] == "JSON" && global.config.db == "mssql" ? 'TEXT' : field['typeData']['type']];
      }
      additionalFields[field['name']] = field['typeData'];
    }

    for (let i = 0; i < global.config.schema.length; i++) {
      const data = global.config.schema[i];
      let fields = {};
      //Normal Columns
      for (let j = 0; j < data.columns.length; j++) {
        let field = data.columns[j];
        if (field['typeData']['type'] === 'STRING') {
          field['typeData']['type'] = DataTypes.STRING(field['typeData']['length']);
        } else {
          field['typeData']['type'] = DataTypes[field['typeData']['type'] == "JSON" && global.config.db == "mssql" ? 'TEXT' : field['typeData']['type']];
        }
        fields[field['name']] = field['typeData'];
      }
      //Generic Columns
      fields = {
        ...fields,
        ...additionalFields
      }
      // if (data.hasOwnProperty('otherOptions')) {
      //   data.otherOptions['freezeTableName'] = true;
      // } else {
      //   // data['otherOptions'] = {
      //   //   "freezeTableName": true
      //   // }
      // }
      // logger.info(data.name, fields)
      _.set(listModels, data.name, sequelize.define(data.name, fields, data.otherOptions));
    }
  } catch (ex) {
    logger.error(ex)
  }
}

function buildPSQLDynamically(field, tableName) {
  try {
    // console.log("FIELD==>", field);

    let sequelize_field = {};
    const db = global.config.dbType;
    const type = field.typeData.type.toLowerCase();
    const length = parseInt(field.typeData.length || 0);
    const hasLength = field.typeData.hasOwnProperty("length");

    const columnName = field.name;
    let dataType = "";
    let sequelizeType;

    const getColumnType = (baseType) => {
      if (db === "pg") {
        return `ADD COLUMN IF NOT EXISTS "${columnName}" ${baseType}`;
      } else {
        return `
          IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '${tableName}' AND COLUMN_NAME = '${columnName}'
          )
          BEGIN
            ALTER TABLE [${tableName}] ADD [${columnName}] ${baseType};
          END
        `;
      }
    };

    switch (type) {
      case "json":
        if (db === "pg") {
          dataType = "json";
          sequelizeType = Sequelize.JSON;
        } else {
          dataType = "text";
          sequelizeType = Sequelize.TEXT;
        }
        break;

      case "bigint":
        dataType = "bigint";
        sequelizeType = Sequelize.BIGINT;
        break;

      case "decimal":
        dataType = "decimal";
        sequelizeType = Sequelize.DECIMAL;
        break;

      case "date":
        dataType = "date";
        sequelizeType = Sequelize.DATEONLY;
        break;

      case "time":
        dataType = "time";
        sequelizeType = Sequelize.TIME;
        break;

      case "datetime":
        dataType = db === "pg" ? "timestamp" : "datetime";
        sequelizeType = Sequelize.DATE;
        break;

      case "integer":
        dataType = db === "pg" ? "integer" : "int";
        sequelizeType = Sequelize.INTEGER;
        break;

      case "float":
        dataType = "float";
        sequelizeType = Sequelize.FLOAT;
        break;

      case "boolean":
        dataType = "boolean";
        sequelizeType = Sequelize.BOOLEAN;
        break;

      case "varchar":
      case "string":
      case "nvarchar":
        const effectiveLength = hasLength ? length : 255;
        dataType = `${type === "nvarchar" ? "nvarchar" : "varchar"}(${effectiveLength})`;
        sequelizeType = Sequelize.STRING(effectiveLength);
        break;

      case "char":
      case "nchar":
        dataType = `${type}(${hasLength ? length : 255})`;
        sequelizeType = Sequelize.STRING(length || 255);
        break;

      case "array":
        if (db === "pg" && field.typeData.typeOfArray) {
          let sub = field.typeData.typeOfArray.toLowerCase();
          let arrType;
          switch (sub) {
            case "integer": arrType = Sequelize.ARRAY(Sequelize.INTEGER); break;
            case "bigint": arrType = Sequelize.ARRAY(Sequelize.BIGINT); break;
            case "float": arrType = Sequelize.ARRAY(Sequelize.FLOAT); break;
            default: arrType = Sequelize.ARRAY(Sequelize.TEXT);
          }
          dataType = `${sub}[]`;
          sequelizeType = arrType;
        } else {
          dataType = "text";
          sequelizeType = Sequelize.TEXT;
        }
        break;

      default:
        dataType = "text";
        sequelizeType = Sequelize.TEXT;
    }

    const rest_of_query = getColumnType(dataType);

    return {
      rest_of_query,
      sequelize_field: {
        type: sequelizeType,
        allowNull: true,
      },
    };
  } catch (ex) {
    logger.error("Error in buildPSQLDynamically:", ex);
    return null;
  }
}

async function updateSchemaFromConfig() {
  try {
    const dbType = global.config.dbType;
    // console.log("DbType:", dbType);

    // console.log('Final global.config.schemaConfig:', JSON.stringify(global.config.schemaConfig, null, 2));
    let client = pgInstance.getDb();

    for (let i = 0; i < global.config.schemaConfig.length; i++) {
      const data = global.config.schemaConfig[i];

      let fields = {}, isNew = false;



      // console.log("client ===> ", client);
      let query = '';

      if (dbType === 'postgres') {
        query = `
          SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = '${data.name}'
          ) AS exists
        `;
      } else if (dbType === 'mssql') {
        // console.log("<- GOING INSIDE ->")
        query = `
          IF EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dbo' 
            AND TABLE_NAME = '${data.name}'
          )
            SELECT 1 AS [exists]
          ELSE
            SELECT 0 AS [exists]
        `;
      }

      const result = await new Promise((resolve, reject) => {
        client.query(query, (err, res) => {
          if (err) {
            reject(err);
          } else {
            resolve(res);
          }
        });
      });

      let exists;
      if (dbType === 'postgres') {
        exists = result.rows[0].exists;
      } else if (dbType === 'mssql') {
        exists = result.recordset[0].exists === 1;
      }

      // console.log("exists --->", exists);

      if (!exists) {
        isNew = true;

        let queryCreateTable = '';
        if (dbType === 'postgres') {
          queryCreateTable = `CREATE TABLE "${data.name}" (id SERIAL)`;
        } else if (dbType === 'mssql') {
          queryCreateTable = `CREATE TABLE [${data.name}] (id INT IDENTITY(1,1))`;
        }



        await new Promise((resolve, reject) => {
          client.query(queryCreateTable, (err) => {
            if (err) reject(err);
            else resolve();
          });
        });

        logger.log('Table is created successfully');
      }

      // let alterQuery = dbType === 'postgres' ? `ALTER TABLE "${data.name}"` : `ALTER TABLE [${data.name}]`;
      let alterQuery = '';
      let rest_of_query = '';

      for (const field of schemaConfig.metaDataFields) {
        const fieldName = field.name;
        const { type, allowNull, defaultValue, unique } = field.typeData || {};

        // Handle type translation if needed
        let dbFieldType;
        if (type === 'STRING') {
          dbFieldType = dbType === 'postgres' ? 'text' : 'VARCHAR(MAX)';
        } else if (type === 'TEXT') {
          dbFieldType = dbType === 'postgres' ? 'text' : 'VARCHAR(MAX)';
        } else if (type === 'JSON') {
          dbFieldType = dbType === 'postgres' ? 'JSONB' : 'NVARCHAR(MAX)';
        } else if (type === 'BIGINT') {
          dbFieldType = 'BIGINT';
        } else {
          dbFieldType = type;
        }

        const nullClause = allowNull === false ? 'NOT NULL' : 'NULL';
        const defaultClause = defaultValue !== undefined
          ? (dbType === 'postgres'
            ? `DEFAULT '${defaultValue}'`
            : `DEFAULT '${defaultValue}'`)
          : '';
        const uniqueClause = unique === true ? 'UNIQUE' : '';

        const fullDefinition = [dbFieldType, nullClause, defaultClause, uniqueClause]
          .filter(Boolean)
          .join(' ');

        if (isNew) {
          rest_of_query += (rest_of_query ? ', ' : '') +
            (dbType === 'postgres'
              ? `ADD COLUMN IF NOT EXISTS "${fieldName}" ${fullDefinition}`
              : `ADD COLUMN [${fieldName}] ${fullDefinition}`);
        } else {
          rest_of_query += (rest_of_query ? '; ' : '') +
            (dbType === 'postgres'
              ? `ALTER TABLE "${data.name}" ADD COLUMN IF NOT EXISTS "${fieldName}" ${fullDefinition}`
              : `IF NOT EXISTS (
             SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
             WHERE TABLE_NAME = '${data.name}' AND COLUMN_NAME = '${fieldName}'
           ) ALTER TABLE [${data.name}] ADD [${fieldName}] ${fullDefinition}`);
        }

        fields[fieldName] = field.typeData;
      }

      if (rest_of_query.trim()) {
        if (dbType === 'mssql' && !isNew) {
          const queries = rest_of_query.split(';').map(q => q.trim()).filter(Boolean);

          for (const q of queries) {
            console.log("Executing MSSQL metaDataFields query:", q);
            await new Promise((resolve, reject) => {
              client.query(q, (err) => {
                if (err) {
                  logger.error("MetaDataFields alter error (MSSQL):", err);
                  reject(err);
                } else {
                  logger.info("MetaDataField added/updated successfully (MSSQL)");
                  resolve();
                }
              });
            });
          }
        } else {
          const finalQuery = isNew
            ? (dbType === 'postgres'
              ? `ALTER TABLE "${data.name}" ${rest_of_query}`
              : `ALTER TABLE [${data.name}] ${rest_of_query}`)
            : rest_of_query;

          console.log("Executing metaDataFields query:", finalQuery);
          await new Promise((resolve, reject) => {
            client.query(finalQuery, (err) => {
              if (err) {
                logger.error("MetaDataFields alter error:", err);
                reject(err);
              } else {
                logger.info("MetaDataFields added/updated successfully");
                resolve();
              }
            });
          });
        }
      }

      if (data.columns.length > 0 || isNew) {
        if (dbType === 'postgres') {
          let alterParts = [];
          for (let j = 0; j < data.columns.length; j++) {
            let field = data.columns[j];
            let psqlData = buildPSQLDynamically(field, data.name);
            alterParts.push(psqlData.rest_of_query);
            fields[field['name']] = psqlData.sequelize_field;
          }

          if (alterParts.length > 0) {
            alterQuery = `ALTER TABLE "${data.name}" ${alterParts.join(', ')}`;
            // console.log("alterQuery", alterQuery);

            await new Promise((resolve, reject) => {
              client.query(alterQuery, (err) => {
                if (err) {
                  logger.error(err);
                  reject(err);
                } else {
                  logger.info('Table is successfully altered');
                  resolve();
                }
              });
            });
          }
        } else if (dbType === 'mssql') {
          for (let j = 0; j < data.columns.length; j++) {
            let field = data.columns[j];
            let psqlData = buildPSQLDynamically(field, data.name);
            fields[field['name']] = psqlData.sequelize_field;

            let singleAlterQuery = psqlData.rest_of_query;

            // console.log("singleAlterQuery", singleAlterQuery);

            await new Promise((resolve, reject) => {
              client.query(singleAlterQuery, (err) => {
                if (err) {
                  logger.error(err);
                  // console.log("logger.error(err) ==>", err);
                  reject(err);
                } else {
                  logger.info(`Column ${field.name} added successfully`);
                  resolve();
                }
              });
            });
          }
        }

        fields = { ...fields };

        // console.log("fields ===>", fields);
        _.set(listModels, data.name, sequelize.define(data.name, fields, data.otherOptions));
      }
    }
    if (dbType === 'postgres') {
      if (client) {
        console.log("<--- CLOSING PG CON --->")
        await client.end();
      }
    } else if (dbType === 'mssql') {
      if (client) {
        console.log("<--- CLOSING MSSQL CON --->")
        await client.close();
      }
    }
  } catch (err) {
    logger.error(err);
  }
}


async function server() {
  logger.info("Starting Server For Channel", global.config.blockChainConfiguration.channel)
  try {

    await sequelize.authenticate()
    logger.info('Connection has been established successfully.');
    logger.warn("Setting up MetaServer...")
    await createMetaServer();
    logger.warn("Setting up MetaProxyServer...")
    // await createMetaProxyServer();
    logger.warn("Setting up ErrorTable...")
    flatten = await loadFlat();
    await createErrorTable();
    logger.warn("Setting up DB Tables...")
    await setUpDBTables();
    logger.warn("Calling Sequalize Sync in background...")
    await sequelize.sync();
    const msg = 'Schema creation successfully done ...';
    logger.info(msg);
    await updateSchemaFromConfig()
    return true;
  } catch (ex) {
    const msg = 'Schema creation has error ...';
    logger.error(ex, msg)
    return false;
  }
}


async function eventHandle(params, _pgInstance = null) {
  try {
    let data = Object.assign(params['_metaData'], params['eventData'], params['extra']);
    data.key = data.DocumentKey;

    let flattenData = flatten(data);
    let stimulatedTable = global.config.schemaConfig.find(o => o.name === data.DocumentName);

    if (stimulatedTable) {
      for (var key of Object.keys(stimulatedTable.columns)) {
        if (stimulatedTable.columns[key].hasOwnProperty('path')) {
          let key_ = `${stimulatedTable.columns[key].path}`;
          let columnType = _.trim(stimulatedTable.columns[key].typeData.type.toLowerCase());

          if (columnType.localeCompare('array') === 0) {
            let value = _.get(data, key_);
            if (!stimulatedTable.columns[key].typeData.hasOwnProperty('typeOfArray')) {
              data[stimulatedTable.columns[key].name] = JSON.stringify(value);
            } else {
              data[stimulatedTable.columns[key].name] = value;
            }
          } else {
            let value = flattenData[key_];
            if (global.config.db === "mssql" && stimulatedTable.columns[key].typeData.type.toLowerCase() === "json") {
              data[stimulatedTable.columns[key].name] = JSON.stringify(value);
            } else {
              data[stimulatedTable.columns[key].name] = value;
            }
          }
        }
      }
    }

    let flag = _.get(listModels, data.DocumentName, undefined);

    if (flag) {
      _.set(data.tranxData, '_rev', undefined);
      _.set(data.tranxData, '~version', undefined);
      if (global.config.db === "mssql") {
        data.tranxData = JSON.stringify(data.tranxData);
      }

      data = updateEmptyDates(flag, data);
      console.log('Final data (res):', JSON.stringify(data, null, 2));

      let reverseReplicatorTables = _.get(global.config, 'reverseReplicatorTables') || {};
      if (reverseReplicatorTables && reverseReplicatorTables.hasOwnProperty(data.DocumentName)) {
        console.log('data.replicatorID', data.replicatorID)
        if (data.replicatorID) {
          let tableName = reverseReplicatorTables.documentName;
          let identifier = reverseReplicatorTables[data.DocumentName];
          let rrq = _.get(listModels, tableName, undefined);

          console.log('rrq =>', listModels, tableName, identifier, data)

          if (rrq) {
            let queueRecord = {
              key: data.replicatorID,
              uniqueIdentifier: identifier,
              uniqueIdentifierValue: data[identifier],
              tableName: data.DocumentName,
              smartContractFunction: data.functionName,
              status: REVERSE_REPLICATOR_STATUS.CONFIRMED,
              transactionID: data.transactionID,
              transactionHash: data.transactionHash
            };
            try {
              // await rrq.upsert(queueRecord);
              await rrq.update({
                status: REVERSE_REPLICATOR_STATUS.CONFIRMED, transactionID: data?.transactionID,
                transactionHash: data?.transactionHash
              }, {
                where: {
                  key: data.replicatorID
                }
              })
              logger.info('Reverse replicator queue record upserted:', queueRecord.key);
            } catch (err) {
              logger.error('Error from reverse replicator upsert:', err);
            }
          }
        }
      }

      // return flag.upsert(data)
      //   .then(success => {
      //     const msg = `[upsert success][C: ${global.config.blockChainConfiguration.channel}] ... ${data.DocumentName} - ${data.key} - Queue ${global.config.amqp.queueName}`;
      //     logger.info(msg);
      //     return 1;
      //   })
      //   .catch(err => {
      //     logger.error("Error: while Executing");
      //     logger.error(err);
      //     logger.error('Error: while Executing. Current Data ' + data.key + ' Error: ' + (err.stack || err));
      //     return -2;
      //   });

    } else {
      const msg = `Model [${data.DocumentName}] not found in [${Object.keys(listModels).join(', ')}]`;
      logger.warn(msg);
      return Promise.resolve(1); // Safe fallback
    }

  } catch (err) {
    logger.error("catch of eventHandle", err);
    return -1;
  }
}



function updateEmptyDates(model, data) {
  const dateFields = Object.keys(model.rawAttributes).filter(
    key => model.rawAttributes[key].type.constructor.key === 'DATE'
  );

  // console.log('dateFields==>' , dateFields)

  for (const field of dateFields) {
    if (data[field] === "") {
      data[field] = null;
    }
  }

  return data;
}
