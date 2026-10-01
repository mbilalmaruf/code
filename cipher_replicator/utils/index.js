const express = require('express'),
  router = express.Router();
const _ = require('lodash');
const vaultUpdate = require('../../app.js');
const mongoDB = require('../api/connectors/mongoDB');
const configUtil = require('../../config');
const os = require("os");

const exit = () => {
  process.exit(1);
};

const configupdate = () => {
  vaultUpdate.getConfigs().then((config) => {
    global.config = config;
    console.info("Vault Config Updated!!")
    API.LoadConfig().then(() => {
      console.info("API Config Updated!!")
    })
   
  })
};

const serverStatus = async (callback) => {
  try {
    mongoDB.connection(configUtil.get('mongodb.url')).then((mongoose) => {
      if (mongoose.readyState != 1 || _.get(global, 'generalException', "") != "") {
        callback({ status: 'unhealthy', error: generalException, svc: "cipher-api", pid: process.pid, hostname: os.hostname() });
      } else {
        callback({ status: 'healthy', time: new Date().toISOString(), svc: "cipher-api", pid: process.pid, hostname: os.hostname() });
      }
    })
  } catch (ex) {
    console.error(ex);
    callback({ status: 'unhealthy', error: ex });
  }
};

router.get('/', require('express-healthcheck')({
  test: serverStatus
}));

module.exports = { router, serverStatus, exit, configupdate };