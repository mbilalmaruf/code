'use strict';
const helper = require('./helper.js');
const crypto = require('./crypto');
const amq = require('amqplib');
let connection;
let shouldbedelivered=0
const isReachable = require('is-reachable');
// const amq = require('../api/connectors/queue');
const amqp = require('amqp-connection-manager');
const query = require('./query-transaction');
const typedata = require('./typedata');
const logger = helper.getLogger('event-emitter');
const follow = require('follow');
const cryptoHash = require('crypto');

const entity = require('./partnerEcomm')
const errorCodes = require('./errorCode')
const graph = require('./stateSet')
const _ = require('lodash');
const fs = require('fs')
const path = require('path')

const queueReplication = require('fastq').promise(worker, _.get(global, "config.workerCount.queueReplication", 5));
const queueEvents = require('fastq').promise(workerEvent, _.get(global, "config.workerCount.queueEvents", 1));
const queueCustomEvents = require('fastq').promise(workerCustomEvent, _.get(global, "config.workerCount.queueCustomEvents", 1));
let meta;
let meta_proxy;
let replicatorqueue;
let myUser;
const {
    Op
} = require('sequelize');
const Entity = require('./models/Entity.js');
const User = require('./models/User.js');

let allowed = _.get(global, "config.replicatorPermission.allowed", []);
let denied = _.get(global, "config.replicatorPermission.denied", []);
let resumeTimeOut = _.get(global, "config.resumeTimeOut", undefined);
let inactivity_ms = _.get(global, "config.inactivity_ms", undefined);
let customEventList = ["typeDataSync", "stateSetTransitionSync", "errorCodeSync", "entitySync"]
let prioritizeCollectionRetry = _.get(global, "config.prioritizeCollectionRetry", [{"Key":"returnsummary", "Collection": "dsMSP_implicit"}]); // event [key, collection] on which we need to retry.


const snooze = ms => new Promise(resolve => setTimeout(resolve, ms));
let count = 0;
async function worker({ result, collection, change, collectionMapSequence, newCollectionName, currentSeek, scname }) {
    let response = await ProcessData(result, collection, change, collectionMapSequence, newCollectionName, currentSeek);
    if (response == 1) {
        try {
            await meta.upsert({
                name: scname,
                seek: currentSeek,
                pvtCollection: newCollectionName
            })
        } catch (ex) {
            // console.log('Error while upserting:', ex)
            logger.error('Error while upserting. Error: ' + ex.stack ?
            ex.stack : ex);
        }
    } else {
        logger.error('not updating meta due to error. Current Seek' + currentSeek + ' Result:'+ result + ' Error: ' + response.stack ?
        response.stack : response );
        // console.log("not updating meta due to error");
    }
}

async function workerEvent({ result, username, collection, change, collectionMapSequence, newCollectionName, currentSeek, scname,lengthOfEvents },cb) {
    try{
       
    logger.info("result ======== ", result);
    let response = await qryChain(result, username)
    // Adding this check because sometimes block is available but service is not able to fetch data for that event
    // so need to add some prioritize collection, so we can initiate retry
    if (response.data) {
        replicatorqueue
            .create({
                key: result.Key+"_"+collection,
                status: response.data == "" ? "Valid": "InVaild",
                block_num: result.block_num,
                txid: result.txID,
                eventName: result.eventName,
                isprocessed: true,
                retry: lengthOfEvents
            })
            .catch(ex => {
                logger.error(ex);
            });
        if(_.isEmpty(response.data) && checkKeyAndCollectionExist(result)){
            logger.info(`Prioritize collection hit | key - ${result.Key}, Collection - ${result.Collection}`)            
            throw new Error("not updating meta due to data not found for prioritize collection")
        }       

        let output = await ProcessData(response.data, collection, change, collectionMapSequence, newCollectionName, currentSeek, result.eventName);
        logger.info(`Processor Response>  ${output} block - ${currentSeek}`)
        if (output == 1 || output == -1) {
            try {
                await meta.upsert({
                    name: scname,
                    seek: currentSeek,
                    pvtCollection: scname
                })
            } catch (ex) {
                logger.error('Error while upserting. Error: ' + ex.stack ?
            ex.stack : ex);
                // logger.info('Error while upserting:', ex)
            }
        } else {
            await meta_proxy.upsert({
                name: scname,
                seek: result.Key,
                pvtCollection: scname,
                error: toString(error)
            })
            logger.error('not updating meta_proxy due to error. Current Seek' + currentSeek + ' Result:'+ result + ' Error: ' + response.stack ?
            response.stack : response );
            throw new Error("not updating meta due to error")
        }
      }
    } catch (ex){
        await meta_proxy.upsert({
            name: scname,
            seek: result.Key,
            pvtCollection: scname,
            error: toString(ex)
        })
        logger.error('not updating meta_proxy due to errorError: ' + ex.stack ?
        ex.stack : ex );
        logger.info("I Quit!",ex);
        process.exit(1)
    }
}

async function workerCustomEvent({ evt, username, currentSeek, scname },cb) {
    try{
        await generalCustomEventHandlerFunction(evt, username);
        try {
            await meta.upsert({
                name: scname,
                seek: currentSeek,
                pvtCollection: scname
            })
        } catch (ex) {
            logger.error('Error while upserting:', ex)
        }
    } catch (ex){
        throw new Error(ex) 
    }
}
async function generalCustomEventHandlerFunction(evt, username){
    if (evt.eventName == 'typeDataSync') {
        logger.info("Performing Typedata Synchronization!!!!")
        return typedata.sync(evt, username);
    }
    if (evt.eventName == 'stateSetTransitionSync') {
        logger.info("Performing StateSetTransition Synchronization!!!!")
        return graph.sync(evt, username);
    }
    if (evt.eventName == 'errorCodeSync') {
        logger.info("Performing ErrorCode Synchronization!!!!")
        return errorCodes.sync(evt, username);
    }
    if (evt.eventName == 'entitySync') {
        logger.info("Performing Entity Synchronization!!!!")
        return entity.sync(evt, username);
    }
}

function sleep (time) {
    return new Promise((resolve) => setTimeout(resolve, time));
}


let getRegisteredUsersOneTime = function (username, org) {
    if (!myUser) {
        myUser = helper.getRegisteredUsers(username, org);
    }
    logger.debug('the user object is ' + JSON.stringify(myUser));
    return myUser;
};
let listenPeerEvents = async function (username, org, eventPeer, ccid, eventname, callback, channelName) {
    replicatorqueue = require('../lib/db/postgres').sequelize.models[global.config.replicatorQueueName];
    meta = require('../lib/db/postgres').sequelize.models['meta'];
    meta_proxy = require('../lib/db/postgres').sequelize.models['meta_proxy'];

    let queryInterval = undefined;
    queryInterval = setTimeout(QueryQueue, 5000, username, callback);
    let currentValue = 0;
    let currentSeek = '';
    let data;
    try {
        let data = await meta.findOne({
            where: {
                name: `${channelName}_${ccid}`,
                pvtCollection: `${channelName}_${ccid}`
            },
            raw: true
        });
        currentSeek = data.seek;
        logger.info("seek found!!!", currentSeek);
        currentValue = _.get(currentSeek.split("_"), "[0]", 0);
    } catch (err) {
        logger.error("seek not found!!!");
    }
    logger.info('username... ' + JSON.stringify(username));
    logger.info('Orgination... ' + JSON.stringify(org));
    logger.info('channelName... ' + JSON.stringify(channelName));
    logger.info('ccid... ' + JSON.stringify(ccid));
    logger.info('eventname... ' + JSON.stringify(eventname));
    logger.info('Successfully All the configurations');
   
    // var channel = client.getChannel(channelName);
    // let eventhubs = channel.getChannelEventHubsForOrg();
    // let eh = eventhubs[0];
    let collectionMapSequence = {};
    let collectionSeq = {};
    logger.info(`Pub-sub started at blkNo-${currentValue}`);
    global.config.replicatorSource = global.config.replicatorSource || "EVENTHUB_GETDATABYKEY";
    global.config.replicatorTarget = global.config.replicatorTarget || "EVENT_QUEUE_AND_DB";
    let LastProcessed;
    logger.info("global.config.replicatorSource ==== ", global.config.replicatorSource)
    if (global.config.replicatorSource == "EVENTHUB_FETCHFROMCOUCH" || global.config.replicatorSource == "EVENTHUB_GETDATABYKEY") {

        try {
            let gateway = await helper.getClientForOrg(org, username, channelName);
            const network = await gateway.getNetwork(channelName);
            await network.addBlockListener(
                async (event) => {
                    let blockNum = event.blockNumber.low
                    let status = "VALID"
                    let txID
                    currentValue = parseInt(blockNum) || 0;
                    // eh.registerChaincodeEvent(
                    //     ccid,
                    //     eventname,
                    //     async (event, blockNum, txID, status) => {
                    let lastProcessedIndex = -1;
                    currentValue = parseInt(blockNum) || 0;
                    logger.info("currentValue ============ ", currentValue)
                    if (_.get(currentSeek.split("_"), "[0]", 0) == currentValue) {
                        lastProcessedIndex = _.get(currentSeek.split("_"), "[1]", -1);
                    }

                    for (const envelope of event.blockData.data.data) {
                        const payload = envelope.payload;
                        const channelHeader = payload.header.channel_header;
                        txID = channelHeader.tx_id

                        // logger.info(`Transaction ID: ${channelHeader.tx_id}`);
                        // logger.info(`Channel ID: ${channelHeader.channel_id}`);

                        if (payload.data.actions) {
                            //.forEach((action) =>
                            for (const action of payload.data.actions) {
                                const event = action.payload.action.proposal_response_payload.extension.events;
                                // console.log(`Channel ID: ${JSON.stringify(event)}`);
                                const chaincodeName = action.payload.chaincode_proposal_payload.input.chaincode_spec.chaincode_id.name;

                                if (chaincodeName == ccid && event && event.payload) {
                                    // console.log(`✅ Found Transaction for Contract [${chaincodeName}]`);
                                    // console.log(`Event Name:`, JSON.stringify(event.payload, null, 2));
                                    try {
                                        // const decodedEvent = JSON.parse(event.payload.toString('utf8'));
                                        // console.log(`Event Name: ${event.event_name}`);
                                        // console.log(`Decoded Event Data: ${JSON.stringify(decodedEvent)}`);

                                        if (status && status === 'VALID') {
                                            LastProcessed = currentValue;
                                            let evt = JSON.parse(event.payload.toString('utf8'));
                                            const msg = `[got event][C: ${channelName}-${currentValue}] ... ${evt.eventName} - ${event.tx_id} - SMC: ${event.chaincode_id} - ${shouldbedelivered}`;
                                            logger.info(msg)
                                            logger.info("CurrentValue Event", currentValue, evt);
                                            if (customEventList.includes(evt.eventName)) {
                                                if (_.get(currentSeek.split("_"), "[0]", 0) == currentValue) {
                                                    return;
                                                }
                                                try {
                                                    if (evt.additionalData) _.set(element, 'additionalData', evt.additionalData);
                                                    await queueCustomEvents.push({
                                                        evt,
                                                        username: username,
                                                        scname: `${channelName}_${event.chaincode_id}`,
                                                        currentSeek: currentValue
                                                    })
                                                } catch (err) {
                                                    logger.error("replicatorqueue Custom Event=", err)
                                                    replicatorqueue
                                                        .create({
                                                            key: _.get(evt, "events[0]", []),
                                                            status: status,
                                                            block_num: blockNum,
                                                            txid: txID,
                                                            eventName: evt.eventName,
                                                            isprocessed: false
                                                        })
                                                        .catch(ex => {
                                                            logger.error(ex);
                                                        })
                                                    return;
                                                }
                                                return;
                                            }
                                            // if (evt.eventName == 'stateSetTransitionSync') {
                                            //     console.log("Performing StateSetTransition Synchronization!!!!")
                                            //     return graph.sync(evt, username);
                                            // }

                                            // if (evt.eventName == 'errorCodeSync') {
                                            //     console.log("Performing ErrorCode Synchronization!!!!")
                                            //     return errorCodes.sync(evt, username);
                                            // }

                                            // if (evt.eventName == 'entitySync') {
                                            //     console.log("Performing Entity Synchronization!!!!")
                                            //     return entity.sync(evt, username);
                                            // }


                                            if (evt.events) {
                                                if (global.config.eventToReplicate && !global.config.eventToReplicate.includes(evt.eventName)) {// eventData.eventName == "DECLARATION_STATUS_CHANGE" || eventData.eventName == "CLAIM_STATUS_CHANGE" || eventData.eventName == "Eventhandler_returnRequest"){
                                                    logger.warn("Not Replicating Event as it not part of replicatorTarget", evt.eventName, global.config.replicatorTarget)
                                                    return;
                                                }
                                                logger.warn("bypassed event rejection")
                                                evt.events.forEach(async (element, index) => {
                                                    // if((_.get(currentSeek.split("_"), "[0]", 0) == currentValue && index <= lastProcessedIndex) ||(global.config.eventToReplicate && !global.config.eventToReplicate.includes(element.Collection))){ return; }
                                                    if (_.get(currentSeek.split("_"), "[0]", 0) == currentValue && index <= lastProcessedIndex) { return; }
                                                    let key = element;
                                                    try {
                                                        _.set(element, 'block_num', blockNum);
                                                        _.set(element, 'txid', txID);
                                                        _.set(element, 'eventName', evt.eventName);
                                                        if (evt.additionalData) _.set(element, 'additionalData', evt.additionalData);

                                                        logger.info("inside evt.events", element)
                                                        queueEvents.push({
                                                            result: element,
                                                            collection: element.Collection,
                                                            username: username,
                                                            scname: `${channelName}_${event.chaincode_id}`,
                                                            currentSeek: currentValue + "_" + index,
                                                            lengthOfEvents: evt.events.length
                                                        })

                                                    } catch (err) {
                                                        replicatorqueue
                                                            .create({
                                                                key: key,
                                                                status: status,
                                                                block_num: blockNum,
                                                                txid: txID,
                                                                eventName: evt.eventName,
                                                                isprocessed: false
                                                            })
                                                            .catch(ex => {
                                                                logger.info(ex);
                                                            });
                                                        logger.error(err);
                                                    }
                                                });
                                            } else {
                                                logger.warn(`Ignoring no event data found against block commit - ${LastProcessed}!!`);
                                                return callback([]);
                                            }
                                        }
                                    } catch (err) {
                                        logger.error('Error parsing event payload:', err);
                                    }
                                }
                            };
                        }
                    }
                }, {
                startBlock: currentValue
            });
        } catch (err) {
            logger.error('Oh snap!' + err);
            logger.warn('reconnecting in 10 seconds!!!');
            setTimeout(() => {
                listenPeerEvents(username, org, eventPeer, ccid, eventname, callback, channelName);
            }, 10000);
        }
    }
    // Event Hub with query from Smart contract
    let url=crypto.decrypt(global.config.amqp.url)
    console.log('amqp url' , url)
    let amqpOptions = {clientProperties: {connection_name: `cipher_replicator_${process.pid}`}};
    if(!connection){
        if (url.startsWith('amqps://')) {
            amqpOptions = {
                ...amqpOptions,
                connectionOptions: {
                cert: fs.readFileSync(path.resolve(__dirname, './certs/client.crt')),
                key: fs.readFileSync(path.resolve(__dirname, './certs/client.key')),
                ca: [fs.readFileSync(path.resolve(__dirname, './certs/ca.crt'))],
    
            }};
        }
        console.log('connection' , amqpOptions)
        connection = await amqp.connect([url], amqpOptions)
    }


    const ch = await connection.createChannel();
    await ch.assertExchange('logs', 'fanout', {
        durable: true
    });
    await ch.assertQueue(global.config.amqp.queueName, {
        durable: true
    });
    await sleep(3000);
    // await connection.close()
    // connection=undefined;
    // await sleep(3000);

    if (global.config.replicatorSource == "EVENTHUB_GETDATABYKEY") {
        logger.info('Event Hub Connected-local hyp!!');
        eh.connect(true);
    }
    //Event Hub with query from Couch DB
    else if (global.config.replicatorSource == "EVENTHUB_FETCHFROMCOUCH") {
        await query.couchInit(() => {
            logger.info('Event Hub Connected - local couch!!')
            // eh.connect(true);
        });
    }
    //Case of Replicator Service
    else if (global.config.replicatorSource == "COUCH_FOLLOW") {
        let configConnection = global.config.blockChainConfiguration;
        query.couchInit(async (dataCouch) => {
            for (const collection in dataCouch) {

                logger.info(collection);
                const dbConnection = `${configConnection.couch.protocall}://${configConnection.couch.username}:${configConnection.couch.password}@${configConnection.couch.ip}:${configConnection.couch.port}/${dataCouch[collection]}`;
                const channelName = dataCouch[collection].substring(0, dataCouch[collection].indexOf('_'))
                const collectionName = dataCouch[collection].substring(dataCouch[collection].indexOf('$$p') + 3, dataCouch[collection].length);

                let newCollectionName = ''
                for (let i = 0; i < collectionName.length; i++) {
                    if (collectionName.charAt(i) == '$') {
                        newCollectionName += collectionName.charAt(i + 1).toUpperCase();
                        i++;
                    } else {
                        newCollectionName += collectionName.charAt(i)
                    }
                }

                _.set(collectionSeq, channelName + "_" + newCollectionName, 0);
                if (allowed && allowed.length || denied && denied.length) {
                    let partyName = newCollectionName && newCollectionName !== "" ? newCollectionName.split("_")[0] : undefined;
                    if (allowed && allowed.length) {
                        if (!(allowed.includes(partyName))) {
                            continue;
                        }
                    } else {
                        if (denied && denied.length && denied.includes(partyName)) {
                            continue;
                        }
                    }
                }

                data = await meta.findAll({
                    where: {
                        name: global.config.blockChainConfiguration.smartContract,
                        pvtCollection: channelName + "_" + newCollectionName
                    },
                    raw: true
                });
                let mapSequence = {};
                data.forEach(element => {
                    if (element.pvtCollection != '-') {
                        _.set(mapSequence, element.pvtCollection, element.seek)
                    }
                });


                const currSeek = _.get(mapSequence, channelName + "_" + newCollectionName, 'now'); //88245
                logger.info('Start Seek', currSeek + "...", "for collection ", newCollectionName)
                logger.info('Seek', currSeek)
                let followConfig = {
                    db: dbConnection,
                    include_docs: true,
                    max_retry_seconds: 3,
                    inactivity_ms: inactivity_ms || 1000 * 3,
                    since: currSeek,
                    filter: function (doc, req) {
                        return (doc["documentName"] || doc["DocumentName"]);
                    }
                };
                // currSeek === 'now' || String(currSeek).trim() === '' ? delete followConfig["since"] : {};
                follow(followConfig, async function (error, change) {
                    console.log("inside");
                    if (!error) {
                        currentSeek = change.seq;
                        console.log("adding change to in-memory queue!!", dataCouch[collection])

                        let encryptedData;
                        if(change.doc.isEncrypted){
                            let config = global.config.blockChainConfiguration;
                            let orgCode = global.config.orgCode
                            console.log("orgCode", orgCode)
                            let orgcodeCheck = change.doc.organizations.filter((element, idx)=>{ if(element === orgCode){ return true;}})
                            if(orgcodeCheck.length > 0){
                                encryptedData = await query.queryChaincodeHyp(channelName,config.smartContract, [change.doc.key, orgCode, change.doc.documentName], "GetDataByKey", username, config.network)
                                console.log("encryptedData", encryptedData);                            
                                encryptedData = {...encryptedData.data}
                            }
                            else { 
                                encryptedData = {}
                                console.log("Replication of the record having key: " + change.doc.key + " and collection: " + change.doc.documentName + " is skipped as it doesn't belong to "+ orgCode );
                                logger.error("Your organization do not have access to read this record");
                            }
                        }

                        console.log('upserting::::::::::::', change.doc)

                        //MongoDB Insertion
                        let documentName = change.doc.documentName || change.doc.DocumentName;
                        let document = change.doc;

                        console.log("documentName => ", documentName);

                        if (documentName === 'entity') {
                            delete document["_id"];
                            await Entity.findOneAndUpdate({spCode: document?.key}, document, {upsert: true}, function (err, doc) {
                                if (err) return console.log("Error Updating / Inserting", err)
                            })
                        }

                        if (documentName === 'user') {
                            delete document["_id"];
                            document["passwordPolicy"] = helper.parseObjectIds(document["passwordPolicy"]);
                            document["createdBy"] = helper.parseObjectIds(document["createdBy"]);
                            document["updatedBy"] = helper.parseObjectIds(document["updatedBy"]);
                            document["groups"] = document["groups"]?.map((group) => helper.parseObjectIds(group));
                            document["userList"] = document["userList"]?.map((user) => helper.parseObjectIds(user));

                            console.log("document after parse => ", document);

                            await User.findOneAndUpdate({userID: document.userID}, document, {upsert: true}, function (err, doc) {
                                if (err) return console.log("Error Updating / Inserting", err)
                            })
                        }

                        queueReplication.push({
                            result: encryptedData ? encryptedData : change.doc,
                            collection: newCollectionName,
                            change: change.seq,
                            collectionMapSequence: collectionMapSequence,
                            newCollectionName: channelName + "_" + newCollectionName,
                            currentSeek: currentSeek,
                            scname: global.config.blockChainConfiguration.smartContract
                        })
                    } else {
                        logger.error("cannot follow !!", error)
                    }
                })
                // break;
            }
        });
        logger.warn('Event Hub Connected - local couch follw mode!!')
        // eh.connect(true);
    }
    else {
        logger.warn("Invalid Service Type")
    }
};
let ProcessData = async function (result, collection, change, collectionMapSequence, newCollectionName, currentSeek, type) {
    try{
        result = _.set(result, 'eventName', type);
        result = _.set(result, '__collection', collection);
        let documentKey = result.documentKey || result.key || result.Key;
        let documentName = result.documentName || result.DocumentName;
        let eventData = {
            // adding eventName bcz of existing work rely on this
            eventName: result.eventName,
            _metaData: {
                block_num: 1000,
                txnid: documentKey,
                status: 'VALID'
            },
            eventData: {
                InternalEventKey: result.Key,
                InternalEventName: type,
                DocumentKey: documentKey,
                DocumentName: documentName,
                ...result
            },
            extra: {
                tranxData: result
            }
        };
        
        const start = require('../lib/start');
        let final;
        let flag=false;  
        
        logger.info("Inside Process Data",eventData)
        _.set(collectionMapSequence, newCollectionName, currentSeek)
            // logic to send to queue and replicate    
        if(global.config.replicatorTarget=="EVENT_QUEUE_AND_DB" || global.config.replicatorTarget=="EVENT_QUEUE"){
            flag=true
            if (global.config.eventToReplicate && global.config.eventToReplicate.includes(collection)){// eventData.eventName == "DECLARATION_STATUS_CHANGE" || eventData.eventName == "CLAIM_STATUS_CHANGE" || eventData.eventName == "Eventhandler_returnRequest"){
                logger.info("Inside event")
            final = await connectQueueService(eventData)
            }else {
                logger.warn("Not Replicating Event as it not part of replicatorTarget", collection, global.config.replicatorTarget)
            }
        }
        if(global.config.replicatorTarget=="EVENT_QUEUE_AND_DB" || global.config.replicatorTarget=="DB_ONLY" ){
            flag=true
            return start.eventHandle(eventData)
        }
        if(!flag){
            throw new Error("SERVICE MODE MISMATCH" + global.config.replicatorTarget)
        }
        return -1;
    }catch (ex){
        return ex.message;
    }
}
let QueryQueue = async (username, callback) => {
    replicatorqueue = require('../lib/db/postgres').sequelize.models[global.config.replicatorQueueName];
    let list = await replicatorqueue
        .findAll({
            where: {
                isprocessed: false,
                retry: {
                    [Op.lte]: global.config.retryCnt || 3
                }
            },
            limit: global.config.retryChunkSize || 5,
            order: [
                ['id', 'ASC'],
                ['block_num', 'ASC']
            ],
            raw: true
        })
        .catch(ex => {
            logger.error(ex);
        });

        logger.info("INSIDEQUERYQUEUE", list,list.length)
    for (let qresp of list) {
        if (qresp && qresp.key) {
            if (global.config.db == 'mssql') {
                qresp.key = JSON.parse(qresp.key);
            }
            if(customEventList.includes(qresp.eventName)){
                await generalCustomEventHandlerFunction(qresp.key, username);
            } else {
                let data = await qryChain(qresp.key, username);
            }
            deleteDBQueue(qresp);
        }
    }
    if (list && list.length)
        await snooze(2000);
    else
        await snooze(8000);

    return QueryQueue(username);
};
// hyperledger
let qryChain = async (element, username) => {
    return callHyperledger(element, username)
};
function updateDBQueueFailure(qresp) {
    if (qresp.id) {
        replicatorqueue.update({
            isprocessed: false,
            retry: qresp.retry + 1
        }, {
            returning: true,
            where: {
                id: qresp.id
            }
        });
    }
}
function deleteDBQueue(qresp) {
    if (qresp.id) {
        replicatorqueue.destroy({
            where: {
                id: qresp.id
            }
        });
    }
}
async function callHyperledger(args, username) {
    let config = global.config.blockChainConfiguration;
    return query.queryChaincode(config.channel, config.smartContract, [args.Key, args.Collection], 'GetDataByKey', username, config.network);
}

async function connectQueueService(request) {
    
        let url=crypto.decrypt(global.config.amqp.url)
        let amqpOptions = {clientProperties: {connection_name: `cipher_replicator_${process.pid}`}};
        if(!connection){
            if (url.startsWith('amqps://')) {
                amqpOptions = {
                    ...amqpOptions,
                    connectionOptions: {
                    cert: fs.readFileSync(path.resolve(__dirname, './certs/client.crt')),
                    key: fs.readFileSync(path.resolve(__dirname, './certs/client.key')),
                    ca: [fs.readFileSync(path.resolve(__dirname, './certs/ca.crt'))],
        
                }};
            }
            connection = await amqp.connect([url], amqpOptions)
        }
        shouldbedelivered++
        const channel = await connection.createChannel();
        await channel.assertExchange('logs', 'fanout', {
            durable: true
        });
        await channel.assertQueue(global.config.amqp.queueName, {
            durable: true
        });
        await channel.bindQueue(global.config.amqp.queueName, 'logs');
        await channel.publish('logs', '', Buffer.from(JSON.stringify(request)));
  
        logger.info(`Message Published successfully! - txid ${_.get(request,'_metaData.txnid',"N/A")}` )
        // connection.close();
        return;
   
}

function checkKeyAndCollectionExist({Key, Collection}){
    prioritizeCollectionRetry.forEach((item)=>{
        if(Key.includes(item.key) && Collection.includes(item.Collection) ){
            return true;
        }
    })
    return false;
}

let InsertEKYCNotificationAudit = async function (values) {
    try {
        // logger.info("values : ", values);
        // const { sequelize } = require('../lib/db/postgres').sequelize.models[global.config.auditPushKycRecordName];
        const dbURL = crypto.decrypt(global.config.connectionString)
        const {sequelize} = require('../lib/db/postgres.js')

        const AuditPushKycRecord = sequelize.define('auditPushKycRecord', {
            key: {
                type: DataTypes.STRING,
                allowNull: true
            },
            profile: {
                type: DataTypes.STRING,
                allowNull: true
            },
            orgCode: {
                type: DataTypes.STRING,
                allowNull: true
            },
            revision: {
                type: DataTypes.STRING,
                allowNull: false,
                unique: true
            },
            fullKey: {
                type: DataTypes.STRING,
                allowNull: false
            },
            isRecordRead: {
                type: DataTypes.BOOLEAN,
                allowNull: true,
                defaultValue: false
            },
        }, {});

        await sequelize.sync();

        logger.info("PUSHKYCRECORDS VALUES", values)

        let data = {
            key: values[0],
            profile: values[1],
            orgCode: values[2],
            revision: values[3],
            fullKey: values[4]
        };

        return await AuditPushKycRecord.create(data);

        // let query = 'INSERT INTO "auditPushKycRecord" ("[key]", "identityType", "profile", "orgCode") VALUES (?, ?, ?, ?)';
        // return await sequelize.query(query, { replacements: values });
    } catch (error) {
        logger.error('Error inserting EKYCNotificationAudit record:', error);
        return error
    }
}

exports.listenPeerEvents = listenPeerEvents;
exports.getRegisteredUsersOneTime = getRegisteredUsersOneTime;