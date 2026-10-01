'use strict';
const dbConfig = require('./lib/db/config');
// const request = require('request-promise');
const request = require('axios');
const { fork } = require('child_process');
const express = require('express');
// const helper = require('./eventConnect/helper')
// const //  logger = helper.get//  logger('Process.js');
const _ = require('lodash');
const { config } = require('process');
require('./child/logger');
const app = express();
global.config = {};
global.procList = [];
tryConnection();

const serverStatus = async callback => {
  if (global.error == null || global.error == '') callback({ state: 'healthy' });
  else callback({ state: 'Unhealthy', error: global.error });
};

const appServer = app.listen(9581, function () {
  console.log('server running at http://%s:%s\n', appServer.address().address, appServer.address().port)
  //  logger.info('server running at http://%s:%s\n', appServer.address().address, appServer.address().port);
});

app.use(
  '/health',
  require('express-healthcheck')({
    test: serverStatus
  })
);

function tryConnection() {
  getConfigs()
    .then(async (res) => {
      // console.log('Config Response (res):', JSON.stringify(res, null, 2));
      // console.log('replicatorMode:', _.get(res, 'replicatorMode', 'FORWARD'));

      //  console.log('Body ->', body);

      //  logger.info('network(s) loaded successfully!!');
      //  logger.info('network(s) loaded successfully!!');
      try {

        if ((_.get(res, 'replicatorMode', 'FORWARD')).toLowerCase() == 'reverse') {
          console.log(JSON.stringify(res));
          global.rootConfig = res;
          global.config = res.replicatorConfig[0];
          console.log('replicator started in reverse mode',)
          const reverseReplicatorConfig = _.get(res, 'reverseReplicatorConfig', [])
          if (reverseReplicatorConfig.length > 0) {
            let startSchedulers = require('./schedulers/dataTableScheduler')
            startSchedulers(reverseReplicatorConfig, res)
          }
        }

        if ((_.get(res, 'replicatorMode', 'FORWARD')).toLowerCase() == 'forward') {
          console.log('replicator started in forward mode');
          // const config = {};
          // const repConfig = res.replicatorConfig?.[0] || {};
          // const fieldsToSet = ['cryptoTemp', 'dbType', 'mongodb', 'db', 'amqp', 'connectionString', 'connectionStringList', 'schemaProfileName'];
          // fieldsToSet.forEach(field => {
          //   const val = _.get(repConfig, field);
          //   if (val !== undefined) {
          //     _.set(config, field, val);
          //   }
          // });
          // // console.log('Final config:', JSON.stringify(config, null, 2));

          
          const options = {
            method: 'POST',
            url: res.fetchNetworkURL,
            data: { header: res.authentications.avanzaISC, type: "Hyperledger" },
            headers: { 'Content-Type': 'application/json' }
          };
          const body = await request(options);

          // startProcess({ globe: config, netConfig: body.data });
          for (let config of _.get(res, 'replicatorConfig', [])) {
            startProcess({ globe: config, netConfig: body.data });
          }
        }

      }
      catch (err) {
        //  logger.error({ error: err.stack || err }, 'server not started, please check error');
        console.error({ error: err.stack || err }, 'server not started, please check error');
      }
    })
    .catch((err) => {
      //  logger.error({ error: err.stack || err }, 'server not started, will retry after one second');
      console.error({ error: err.stack || err }, 'server not started, will retry after one second');
      setTimeout(function () {
        return tryConnection();
      }, 1000);
    });
}

async function getConfigs() {


  // const resp = await dbConfig.get();

  return new Promise(async (resolve, reject) => {
    let { err, body } = await dbConfig.get();
    // dbConfig.get((err, response, body) => {
    // //  logger.info("getCOnfig",body)

    if (!err && typeof body === 'object') {
      global.terminateInterval = _.get(body, 'terminateInterval', 80000)
      resolve(body);
    }
    err = err || body;
    reject(err);
    // });
  });
}

function startProcess(repConfig) {
  // console.log('Final repConfig:', JSON.stringify(repConfig, null, 2));

  const listen = fork('./child/index.js');
  //  logger.info("New Process Initailized...", listen.pid);
  global.procList.push(listen)
  listen.send(repConfig);
  listen.on('message', err => {
    //  logger.warn("Listen Process Exited !!",err);
  });
  listen.on('close', (code, signal) => {
    //  logger.error(`child process terminated due to receipt of signal ${signal} code: ${code}, restart in 3 seconds`);
    setTimeout(() => {
      startProcess(repConfig)
    }, 3000)
  });
  return listen;
}

setInterval(function () {
  //  logger.warn("Killing all processes!!");
  if (global.terminateInterval) {
    global.procList.forEach((elem, index) => {
      //  logger.warn("PID ", elem.pid ,"Terminated" );
      elem.kill('SIGINT');
      global.procList.splice(index, 1);
    })
  } else {
    //  logger.warn("Terminate Interval not defined");
  }
}, global.terminateInterval || 60000);