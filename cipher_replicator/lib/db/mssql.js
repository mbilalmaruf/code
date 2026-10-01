'use strict';

const Sequelize = require('sequelize');
const config = require('../config');
const _ = require('lodash');
const crypto = require('../../lib/helpers/crypto');

let connectionString = crypto.decrypt(config.get('connectionString'));
let exitTime = _.get(global, "config.exitTime", undefined);
let dbType = _.get(global, "config.db", "mssql")

let db = {}
try {
  db = {
    sequelize: dbType.toLowerCase() == "mssql" ? new Sequelize(connectionString, {
      logging: false,
      dialectOptions: {
        options: {
          requestTimeout: 60000,
          encrypt: true
        }
      },
      pool: {
        max: 5,
        min: 0,
        acquire: exitTime * 2 || 200000 * 2,
        idle: exitTime * 2 || 200000 * 2
      }
    })
      :
      new SequelizeOracle(crypto.decrypt(_.get(global, 'config.oracle.db', undefined)), crypto.decrypt(_.get(global, 'config.oracle.user', undefined)), crypto.decrypt(_.get(global, 'config.oracle.password', undefined)), {
        dialect: 'oracle',
        host: crypto.decrypt(_.get(global, 'config.oracle.db', undefined)),
        dialectOptions: {
          connectionString: crypto.decrypt(_.get(global, 'config.oracle.connectionString', undefined)),
          options: {
            connectTimeout: 300000,
            requestTimeout: 3000,
            query_timeout: 300000,
          }
        },
        retry: {
          match: [/Deadlock/i],
          max: 3, // Maximum rety 3 times
          backoffBase: 1000, // Initial backoff duration in ms. Default: 100,
          backoffExponent: 1.5, // Exponent to increase backoff each try. Default: 1.1
        },
        pool: {
          max: 100,
          min: 0,
          acquire: exitTime * 2 || 200000 * 2,
          idle: exitTime * 2 || 200000 * 2
        },
        define: {
          //prevent sequelize from pluralizing table names
          freezeTableName: true
        },
      }),

    dataTypes: dataTypes()
  }
} catch (ex) {

}

function dataTypes() {
  let types = [];
  for (let type in dbType.toLowerCase() == "mssql" ? Sequelize.DataTypes : SequelizeOracle.DataTypes) {
    type = _.toLower(type);
    types.push(type);
  }
  return types;
}

module.exports = db;



