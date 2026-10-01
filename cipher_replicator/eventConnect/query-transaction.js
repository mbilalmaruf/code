'use strict';
const util = require('util');
const helper = require('./helper.js');
const NodeCouchDb = require('node-couchdb');
const logger = helper.getLogger('Query');
const _ = require('lodash');
const { deepParseJson } = require('deep-parse-json');
const crypto = require('./crypto');

const snooze = ms => new Promise(resolve => setTimeout(resolve, ms));

const queryChaincode = async function (channelName, chaincodeName, args, fcn, username, network) {
	if (global.config.replicatorSource !="EVENTHUB_GETDATABYKEY") {
		return await queryCouch(channelName, chaincodeName, args, fcn, username, network);
	} else {
		return await queryChaincodeHyp(channelName, chaincodeName, args, fcn, username, network);
	}
};

let couchInit =async function (callback) {
	if (global.config.replicatorSource !="EVENTHUB_GETDATABYKEY") {
		global.collectionList = {};
		let configConnection = global.config.blockChainConfiguration;
		// logger.info(JSON.stringify(configConnection.couch))
		global.couch = new NodeCouchDb({
			host: configConnection.couch.ip, // IP address
			protocol: configConnection.couch.protocall, //protocall
			port: configConnection.couch.port, //port
			timeout: configConnection.couch.timeout || 30000, //timeout
			auth: {
				user: configConnection.couch.username, //username
				pass: configConnection.couch.password //password
			}
		});

		return global.couch.listDatabases().then((list) => {
			if (!list.error) {
				let publicCollection = configConnection.channel+"_"+configConnection.smartContract
				_.set(global.collectionList, publicCollection.replace(/[^a-zA-Z0-9]/g, ''), publicCollection )
				list.forEach(element => {
					let key = element.replace(/[^a-zA-Z0-9]/g, '');
					let contractName = configConnection.smartContract.replace(/([A-Z])/g,"%24$1").replace(/%24/g, '$').toLowerCase() + "$$p";
					if (element.indexOf('$$h') == -1  && element.indexOf(contractName) > -1 &&
					element.indexOf(configConnection.channel.replace(/([A-Z])/g,"%24$1").replace(/%24/g, '$').toLowerCase()) == 0) {
						logger.info(element, '--> ', element.replace(/[^a-zA-Z0-9]/g, ''))
						_.set(global.collectionList, key, element)
					}
				});

				if (callback)
					callback(global.collectionList);

				return global.collectionList;
				
			} else {
				// logger.info(list)
                logger.error({ fs: 'QueryTransaction', func: 'listDatabases' }, list.error);
				process.exit(0);
			}

		});
	} else {
		logger.info("Couch as a datasource not selected!!!")
	}
};

const queryCouch = async function (channelName, chaincodeName, args, fcn, username, network) {
	let databaseName = ""
	if(args[1]==""){
         databaseName = `${channelName}_${chaincodeName.replace(/([A-Z])/g,"%24$1").replace(/%24/g, '$').toLowerCase().replace("$$p","")}`
	}else{
	 databaseName = `${channelName}_${chaincodeName.replace(/([A-Z])/g,"%24$1").replace(/%24/g, '$').toLowerCase()}$$p${args[1].replace(/([A-Z])/g,"%24$1").replace(/%24/g, '$').toLowerCase()}`
	}
	 let dbNameResolved = databaseName;
	 let generalConfig = global.config;
	 if(generalConfig.localCouch && !global.couch){
		await couchInit()
		logger.info("couch init successfully");
	 }
	// logger.info("queryChaincode",databaseName)
	if (dbNameResolved) {
		return new Promise((resolve, reject) => {
			global.couch.get(dbNameResolved, args[0]).then(({ data, headers, status }) => {
				let response = {
					success: true,
					data: data
				};
				logger.info(`[Returning Data][DB:${dbNameResolved}][KEY:${args[0]}] data found!`)
				return resolve(response)
			}, err => {
				logger.warn(`[Querying][DB:${dbNameResolved}][KEY:${args[0]}] data not found for key!!`)
				logger.error(err);
				//  todo queue in repllicator queue
				
				return resolve({
					success: true,
					data: {}
				});
				// return reject(err);
			});
		}).catch(err => logger.error(err));
	} else {
		logger.warn(`[Querying][DB:${dbNameResolved}][KEY:${args[0]}] local couch not initialized!!`)
		return null;
	};
}
const queryChaincodeHyp = async function (channelName, chaincodeName, args, fcn, username, network) {
	try {
		// first setup the client for this org
		
		let client = await helper.getClientForOrg(network, username, channelName);
		logger.debug({ fs: 'QueryTransaction', func: 'getClientForOrg' }, 'Successfully got the fabric client for the organization ' + network);
		var channel = client.getChannel(channelName);
		if (!channel) {
			let message = util.format('Channel %s was not defined in the connection profile', channelName);
			logger.error({ fs: 'QueryTransaction', func: 'getChannel' }, message);
			throw new Error(message);
		}
		let admin = await helper.getOrgAdmin(network, client);
		if (!admin) {
			let message = util.format('admin error');
			logger.error({ fs: 'QueryTransaction', func: 'getOrgAdmin' }, message);
			throw new Error(message);
		}
		// send query
		const transient_data = {
			'PrivateArgs': Buffer.from(args.join("|")) // string <-> byte[]
		};
		var request = {
			targets: await helper.newPeers(undefined, network, username, channelName), //queryByChaincode allows for multiple targets
			chaincodeId: chaincodeName,
			fcn: fcn,
			args: [],
			transientMap: transient_data
		};
		let response_payloads = await channel.queryByChaincode(request);
		logger.warn("Error Occured Please Check Chaincode >", JSON.stringify(response_payloads))
		if (response_payloads) {
			for (let i = 0; i < response_payloads.length; i++) {
				logger.info({ fs: 'QueryTransaction', func: 'queryByChaincode' }, response_payloads[i].toString('utf8'));
				if (response_payloads[i].status == 500) {
					logger.error("Error Occured Please Check Chaincode >", JSON.stringify(response_payloads[i]))
				}
			}

			let load = JSON.parse(response_payloads[0].toString('utf8')) || {}
			for (let key in load) {
				if (load[key] instanceof String && load[key].indexOf("{") > -1)
					load[key] = deepParseJson(load[key]) || load[key];
			}

			let response = {
				success: true,
				data: load || {}
				};
				return response;
		} else {
			logger.error({ fs: 'QueryTransaction', func: 'queryByChaincode' }, 'response_payloads is null');
			let response = {
				success: true,
				data: {}
			};
			return response;
		}
	} catch (error) {
		logger.error({ fs: 'QueryTransaction', func: 'queryByChaincode' }, 'Failed to query due to error: ' + error.stack ? error.stack : error);
		return error.toString();
	}
};

exports.queryChaincode = queryChaincode;
exports.couchInit = couchInit;
exports.queryChaincodeHyp = queryChaincodeHyp;