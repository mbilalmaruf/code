'use strict';

const Sequelize = require('sequelize');
const config = require('../config');
const _ = require('lodash');
const crypto = require('../../lib/helpers/crypto');
let exitTime = _.get(global, "config.exitTime", 200000);
let dbType = _.get(global, "config.dbType", "postgres").toLowerCase();

console.log("dbType->", dbType);

let db = {
  sequelize: null,
  dataTypes: []
};

if (dbType === "postgres") {
  const decryptedString = crypto.decrypt(global.config.connectionString);
  console.info("Decrypted PostgreSQL Connection String ===>>> : ", decryptedString);

  db.sequelize = new Sequelize(decryptedString, {
    logging: console.log,
    dialect: 'postgres',
    pool: {
      max: 5,
      min: 0,
      acquire: exitTime * 2,
      idle: exitTime * 2
    }
  });

} else if (dbType === "mssql") {
  const mssqlConfigEncrypted = _.get(global, "config.mssqlConfig", {});
  const decryptedConnObjStr = crypto.decrypt(mssqlConfigEncrypted || '');

  let connObj;
  try {
    connObj = JSON.parse(decryptedConnObjStr);
  } catch (e) {
    console.error("Failed to parse decrypted MSSQL connection string as JSON");
    throw e;
  }

  console.info("Parsed MSSQL Connection Config ===>>> : ", connObj);

  db.sequelize = new Sequelize(connObj.database, connObj.user, connObj.password, {
    host: connObj.server,
    port: connObj.port || 1433,
    dialect: 'mssql',
    logging: false,
    dialectOptions: connObj.options || {
      encrypt: true,
      trustServerCertificate: true,
    },
    pool: {
      max: 5,
      min: 0,
      acquire: exitTime * 2,
      idle: exitTime * 2
    },
    requestTimeout: connObj.requestTimeout || 60000,
    connectionTimeout: connObj.connectionTimeout || 60000,
  });
} else if (dbType === "oracle") {
  db.sequelize = new SequelizeOracle(
    crypto.decrypt(_.get(global, 'config.oracle.db', '')),
    crypto.decrypt(_.get(global, 'config.oracle.user', '')),
    crypto.decrypt(_.get(global, 'config.oracle.password', '')),
    {
      dialect: 'oracle',
      host: crypto.decrypt(_.get(global, 'config.oracle.db', '')),
      dialectOptions: {
        connectionString: crypto.decrypt(_.get(global, 'config.oracle.connectionString', '')),
        options: {
          connectTimeout: 300000,
          requestTimeout: 3000,
          query_timeout: 300000,
        }
      },
      retry: {
        match: [/Deadlock/i],
        max: 3,
        backoffBase: 1000,
        backoffExponent: 1.5,
      },
      pool: {
        max: 100,
        min: 0,
        acquire: 60000,
        idle: exitTime * 2
      },
      define: {
        freezeTableName: true
      },
    }
  );
} else {
  throw new Error(`Unsupported DB Type: ${dbType}`);
}

function dataTypes() {
  const types =
    dbType === "oracle"
      ? SequelizeOracle.DataTypes || {}
      : Sequelize.DataTypes || {};

  return Object.keys(types).map(t => _.toLower(t));
}

db.dataTypes = dataTypes();

module.exports = db;
