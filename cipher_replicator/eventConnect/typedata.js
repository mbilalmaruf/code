'use strict';

const query = require('./query-transaction');
const _ = require('lodash');
const mongoose = require('mongoose');
const config = require('../lib/config/index');
const crypto = require('../lib/helpers/crypto');
const Letter = require('./models/Letter')
const ADHReport = require('./models/ADHReport')
const TypeData = require('./models/TypeData')
const helper = require('./helper.js')
const logger = helper.getLogger('TypeData')



const mongoDB = require('../api/client/mongoose')(crypto.decrypt(config.get('mongodb.url')));

const syncADHReport = async (result) => {
   try {
      const templateId = result.data.letterTemplate.templateId;
      const letter = await letterDeleteAndInsert(
         { templateId },
         result.data.letterTemplate
      );

      if (letter && letter._id) {
         // if (result.data._id === result.data.name) {
         //    delete result.data._id;
         // }
         result.data._id = result.data.objectId;
         // delete result.data.key;
         const data = { ...result.data, letterTemplate: templateId };
         logger.info({ result, data })

         const report = await reportDeleteAndInsert(
            { _id: data._id, name: data.name },
            data,
         );

         logger.info("report------------>", report);
      }

      logger.info("letter------------>", letter);
      logger.info("All Synchronized successfully!!!");
   } catch (e) {
      logger.error(e);
      logger.error("Sync Failed!!!");
      throw Error(e);
   }
}

const syncTypeData = async (result) => {
   try {
      var typeId = result.data._id
      typeId = typeId.slice(9);

      const typeData = await typeDataDeleteAndInsert(
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

      if (_.get(evt.events, "[0].Collection") == "ADHocReport")
         return syncADHReport(result)
      if (_.get(evt.events, "[0].Collection") == "GLOBAL")
         return syncTypeData(result)
   // });
}

const letterDeleteAndInsert = async (query, set) => {
   try {
      let queryUpsert = {};
      const letter = await Letter.findOne(query);

      if (letter && letter._id) {
         queryUpsert = { _id: letter._id };
      } else {
         queryUpsert = query;
      }

      const dataupsert = await Letter.updateOne(
         queryUpsert,
         { $set: set },
         { upsert: true }
      );

      if (dataupsert && dataupsert.upserted) {
         return dataupsert.upserted[0];
      }

      return { _id: letter._id };
   } catch (err) {
      logger.error(err);
   }
};

const reportDeleteAndInsert = async (query, set) => {
   try {
      let queryUpsert = {};
      const report = await ADHReport.findById(query);

      if (report && report._id) {
         queryUpsert = { _id: report._id };
      } else {
         queryUpsert = query;
      }

      const dataupsert = await ADHReport.updateOne(
         queryUpsert,
         { $set: set },
         { upsert: true }
      );

      if (dataupsert && dataupsert.upserted) {
         return dataupsert.upserted[0];
      }

      return { _id: report._id };
   } catch (err) {
      logger.error(err);
   }
};

async function typeDataDeleteAndInsert(query, set) {
   _.set(set, 'isForign', true);
   delete set._id


   Object.keys(set.typeData).forEach(k =>   { 
      
      let obj = JSON.parse(set.typeData[k])

      delete obj._id

        TypeData.findOneAndUpdate({ typeName: obj.typeName }, obj, { upsert: true }, function (err, doc) {
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