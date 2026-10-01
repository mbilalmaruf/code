'use strict';


let errorTable;
const db = require('./db/postgres');
const Sequelize = require('sequelize');
const _ = require('lodash');
const sequelize = db.sequelize;

module.exports = {
  server,
  eventHandle
};
let listModels = {};

function createMetaServer() {
  const fields = {
    name: {
      type: Sequelize.STRING(400),
      unique: true
    },
    value: {
      type: Sequelize.STRING
    },
    seek: {
      type: Sequelize.TEXT
    },
    pvtCollection: {
      type: Sequelize.STRING
    }
  }
  sequelize.define('meta', fields, {});
}

function createErrorTable() {
  let errorTableFields = [];
  for (let j = 0; j < global.config.errorTable.columns.length; j++) {
    let field = global.config.errorTable.columns[j];
    if (field['typeData']['type'] === 'STRING') {
      field['typeData']['type'] = Sequelize.STRING(field['typeData']['length']);
    } else {
      field['typeData']['type'] = Sequelize[field['typeData']['type']];
    }
    errorTableFields[field['name']] = field['typeData'];
  }
  sequelize.define(global.config.errorTable.name, errorTableFields, global.config.errorTable.otherOptions);
  errorTable = sequelize.models[global.config.errorTable.name];
}

function setUpDBTables() {
  console.log("yep yep -> ", global.config.replicatorQueue);

  let fieldList = {};
  for (let j = 0; j < global.config[global.config.replicatorQueueName].length; j++) {
    let field = global.config[global.config.replicatorQueueName][j];
    if (field['typeData']['type'] === 'STRING') {
      field['typeData']['type'] = Sequelize.STRING(field['typeData']['length']);
    } else {
      field['typeData']['type'] = Sequelize[field['typeData']['type']];
    }
    fieldList[field['name']] = field['typeData'];
  }
  fieldList = { ...fieldList }
  sequelize.define(global.config.replicatorQueueName, fieldList);



  let additionalFields = {}
  for (let j = 0; j < global.config.metaDataFields.length; j++) {
    let field = global.config.metaDataFields[j];
    if (field['typeData']['type'] === 'STRING') {
      field['typeData']['type'] = Sequelize.STRING(field['typeData']['length']);
    } else {
      field['typeData']['type'] = Sequelize[field['typeData']['type']];
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
        field['typeData']['type'] = Sequelize.STRING(field['typeData']['length']);
      } else {
        field['typeData']['type'] = Sequelize[field['typeData']['type']];
      }
      fields[field['name']] = field['typeData'];
    }
    //Generic Columns
    fields = { ...fields, ...additionalFields }
    _.set(listModels, data.name, sequelize.define(data.name, fields, data.otherOptions));

  }
}

function server() {
  createMetaServer();
  createErrorTable();
  setUpDBTables();

  return sequelize.sync()
    .then((res) => {
      const msg = 'Schema creation successfully done ...';
      console.log(msg);
      return true;
      //console.info(msg);
    })
    .catch((err) => {
      const msg = 'Schema creation has error ...';
      if (global.config.db == "mssql")
        err = JSON.stringify(err)
      console.log(msg, err.stack || err);
      return false;
    });
}

function eventHandle(params) {
  // console.log("event handle begin.......");
  let data = Object.assign(params['_metaData'], params['eventData'], params['extra']);
  data.key = data.DocumentKey;

  console.log("listModels ============ ", listModels)
  console.log("data.DocumentName ============ ", data.DocumentName)

  let flag = _.get(listModels, data.DocumentName, undefined);
  if (flag) {

    _.set(data.tranxData, '_rev', undefined);
    _.set(data.tranxData, '~version', undefined);

    if (global.config.db == "mssql")
      data.tranxData = JSON.stringify(data.tranxData)


    return flag.upsert(data).then(success => {

      const msg = `[upsert success] ... ${data.DocumentName} - ${data.key} - Queue ${global.config.amqp.queueName}`;
      console.log(msg);
    }).catch(err => {
      console.log(err)
      let error = err;
      const msg = 'upserting to database has error ...';
      if (global.config.db == "mssql")
        error = JSON.stringify(err);
      throw err;
    });
  } else {
    const msg = 'Ignoring Message due to bad request!!!';
    console.log(msg);
    return Promise.resolve();
    // throw msg
  }
}

