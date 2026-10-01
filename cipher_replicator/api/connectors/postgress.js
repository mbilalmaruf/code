'use strict';
const factory = require('../client/index');
const crypto = require('../../lib/helpers/crypto');
const rp = require('request-promise');
const _ = require('lodash')

module.exports.connection = async function () {
  // let dbConfig = crypto.decrypt(config.get('connectionString'));
  let arg = [''];

  let connectionURL =  _.get(global.config , 'connectionString' , '')
  let encryptedPath = _.get(connectionURL , 'url.encryptedData')
  let iv = _.get(connectionURL , 'url.iv')
  let authTag = _.get(connectionURL , 'url.authTag')
  connectionURL = crypto.decrypt(encryptedPath , iv , authTag);
  const connectionURLList = _.get(global.config , 'connectionStringList' , [])
  // Make sure at least 1 is present.
  if (!connectionURL && connectionURLList.length == 0) {
      throw new Error("No PG connection string found.");
  }

  if (connectionURLList.length === 0) {
      arg = [connectionURL]
  } else {
      arg = connectionURLList.map(i => {
        let connectionURL =  i
        let encryptedPath = _.get(connectionURL , 'url.encryptedData')
        let iv = _.get(connectionURL , 'url.iv')
        let authTag = _.get(connectionURL , 'url.authTag')
        return crypto.decrypt(encryptedPath , iv , authTag)
      });
  }

  return new Promise(async(resolve, reject) => {
    try {
      let pgConnection = factory.createClient('pg', arg);
      return resolve(pgConnection);
    } catch (e) {
      // ErrorCapturing.instance().recordError(e, '001');
      const options = {
        method: 'POST',
        uri: `${global.errorCaptureURL}/captureReplicationError`,
        headers: {
            'User-Agent': 'Request-Promise',
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'x-auth-key': global.avanzaISC.password
        },
        body: {
            typeCode: "001",
            payload: e.stack
        },
        json: true
      };

      console.log("option =>>>> ", options);
      await rp(options);
      
      return reject(e);
    }
  });
};
