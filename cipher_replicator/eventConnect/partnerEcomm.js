'use strict';

const query = require('./query-transaction');
const _ = require('lodash');
const mongoose = require('mongoose');
const config = require('../config/index');
const crypto = require('../lib/helpers/crypto');
const helper = require('./helper')
const logger = helper.getLogger('PartnerEcomm')
// const Letter = require('./models/Letter')
const Entity = require('./models/Entity');
// const TypeData = require('./models/TypeData')


const mongoDB = require('../api/client/mongoose')(crypto.decrypt(config.get('mongodb.url')));


const syncEntity = async (result) => {
   try {
      const Entity = await entityDeleteAndInsert({ spCode: result.key }, result);

      logger.info("syncEntity------------>", Entity);
   } catch (e) {
      logger.error(e)
      throw Error(e);
   }
}

async function sync(evt, username) {
   let result = await callHyperledger([evt], username);
   let data = _.get(result, "data.data", "[]");

   logger.info("ORGANIZTION DATA", result.data)

   //    logger.info(">>>>result>>>>>>>>>", result.data);
   let ownerOrg = config.get('ownerOrgs') ? config.get('ownerOrgs') : []

   if (result.data.additionalData && result.data.additionalData.ECRInfo && result.data.additionalData.ECRInfo.businessType
      && (result.data.additionalData.ECRInfo.businessType.toLowerCase() == "3pl" || result.data.additionalData.ECRInfo.businessType.toLowerCase() == "broker"
         || result.data.additionalData.ECRInfo.businessType.toLowerCase() == "courier")) {
      logger.info("Partner Key", result.data.key, ownerOrg, config.get('ownerOrgs'))
      if (ownerOrg.includes(result.data.key)) {
         logger.info("syncing Partner Organizations")
         evt.events.forEach(event => {
            return syncEntity(result.data)
         });
      }
   } else if (result.data.additionalData && result.data.additionalData.ECRInfo && result.data.additionalData.ECRInfo.businessType
      && result.data.additionalData.ECRInfo.businessType.toLowerCase() == "ecommerce") {
      let finalArr = []
      for (var key of Object.keys(result.data.additionalData)) {
         logger.info(key + " -> " + result.data.additionalData[key])
         if (Array.isArray(result.data.additionalData[key])) {
            for (var val of result.data.additionalData[key]) {
               logger.info("VALLLLLLLL", val)
               if (val.orgCode && val.orgCode != "")
                  finalArr.push(val.orgCode)
            }
         }
      }

      finalArr = [...new Set(finalArr)]
      logger.info("FInal ARRRRRRRRAY", finalArr)

      const intersection = ownerOrg.filter(element => finalArr.includes(element));
      logger.info("Intersectiooooooooooooooooon", intersection)

      if (intersection.length > 0) {
         logger.info("syncing Ecommerce Organizations")
         evt.events.forEach(event => {
            return syncEntity(result.data)
         });
      }


      // if()
   } 

   // evt.events.forEach(event => {
   //   return syncEntity(result.data)
   // });
}

async function entityDeleteAndInsert(query, set) {

   logger.info("quer------------------y", query)
   
   let obj = {
      spCode: _.get(set, "key", ""),
      orgType: _.get(set, "additionalData.ECRInfo.businessType", ""),
      entityName: _.get(set, "value.orgName", ""),
      arabicName: _.get(set, "value.orgName", ""),
      publicKey: _.get(set, "value.certificate", ""),
      status: _.get(set, "value.status", ""),
      isActive: _.get(set, "value.isActive", false),
      additionalData: _.get(set, "additionalData", {}),
      dateUpdated: Date.now(),
   }

   let org = await Entity.findOne(query)

   if (org && org != undefined){
      obj.additionalData.declarationEP = _.get(org, "additionalData.declarationEP", "")
   }

   logger.info("Object to be Inserted=================>>", obj);

   // return Entity.findOneAndUpdate(query, obj, { upsert: true }, function (err, doc) {
   //    if (err) return logger.info("Error Updating / Inserting", err)

   //    // return logger.info('Succesfully saved')
   // });
   return Entity.findOneAndUpdate(query, obj, { upsert: true });
}

function callHyperledger(args, username) {
   let config = global.config.blockChainConfiguration;

   return new Promise((resolve, reject) => {
      let databaseName = `${config.channel}_${config.smartContract}`
      logger.info("DBname ------", databaseName)
      logger.info("other args ------", args[0].events[0].Collection + "_" + args[0].events[0].Key)
      global.couch.get(databaseName, args[0].events[0].Collection + "_" + args[0].events[0].Key).then(({ data, headers, status }) => {
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