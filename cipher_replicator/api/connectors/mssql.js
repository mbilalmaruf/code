'use strict';
const factory = require('../client/index');
const crypto = require('../../lib/helpers/crypto');
const _ = require('lodash')

module.exports.connection = async function (poolname = 'default') {

  let dbConfig = crypto.decrypt(
    _.get(global.config , 'mssqlConfig')
);
  return new Promise(async (resolve, reject) => {
    try {

      let sqlconnection = await factory.createClient('mssql', dbConfig, poolname);
      return resolve(sqlconnection);
    }
    catch (e) {
      console.log("Error in sql connection", e);
      return reject(e);
    }
  });
};
