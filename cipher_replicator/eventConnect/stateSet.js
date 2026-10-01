'use strict';

const query = require('./query-transaction');
const _ = require('lodash');
const mongoose = require('mongoose');
const config = require('../lib/config/index');
const crypto = require('../lib/helpers/crypto');
const Graph = require('./models/StateSet')
const helper = require('./helper')
const logger = helper.getLogger('StateSet')


const mongoDB = require('../api/client/mongoose')(crypto.decrypt(config.get('mongodb.url')));


const syncGraph = async (result) => {
   try {
      var typeId = result.data._id
      typeId = typeId.slice(9);

      const typeData = await GraphDeleteAndInsert(
         { typeName: typeId },
         result.data
      );

      logger.info("typeData------------>", typeData);
   } catch (e) {
      logger.error(e)
      throw Error(e);
   }
}

async function sync(evt, username) {
   let result = await callHyperledger([evt], username);
   let data = _.get(result, "data.data", "[]");

   logger.info(">>>>result>>>>>>>>>", result.data);

   // evt.events.forEach(event => {
   //    let collectionName = event.Collection

      if (_.get(evt.events, "[0].Collection") == "GLOBAL")
         return syncGraph(result)
   // });
}



async function GraphDeleteAndInsert(query, set) {
   _.set(set, 'isForign', true);
   delete set._id


   Object.keys(set.typeData).forEach(k =>   { 
      
    let obj = JSON.parse(set.typeData[k])
    let id = obj._id
    delete obj._id

        Graph.findOneAndUpdate({ _id: id }, obj, { upsert: true }, function (err, doc) {
         if (err) return logger.info("Error Updating / Inserting", err)

         // return logger.info('Succesfully saved')
         });
   
   });


 }

function callHyperledger(args, username) {
   let config = global.config.blockChainConfiguration;

   return new Promise((resolve, reject) => {
      let databaseName = `${config.channel}_${config.smartContract}`
      logger.info("DBname ------", databaseName)
      logger.info("other args ------", args[0].events[0].Key)
      global.couch.get(databaseName, args[0].events[0].Key).then(({ data, headers, status }) => {
         let response = {
            success: true,
            data: data
         };
         logger.info("syncedDATA-Record ------>>>", data)
         return resolve(response)

      }, err => {

         //logger.info("looking for > ", args[0], ">", dbNameResolved)
         //logger.info(">>>>NOT FOUND FOR>>>>----->", args[0], ' - ', args[1]);
         logger.error(err);
         return reject(err);
      });

   });
   // return query.queryChaincode(config.channel, config.smartContract, [], 'GetTypeDataSync', username, config.network);
}

exports.sync = sync;