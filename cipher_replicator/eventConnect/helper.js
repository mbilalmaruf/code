'use strict';
const log4js = require('log4js');
const path = require('path');
const rootDir = path.join(__dirname, '../');
log4js.configure({
	appenders: {
        console: { 
            type: 'console', 
            layout: { type: 'colored' }  // ✅ Colored console logs for better visibility
        },
        file: {
            type: 'dateFile',            // ✅ Rotating file appender
            filename: 'logs/app.log',    // Log file path
            pattern: 'yyyy-MM-dd',       // Rotate daily
            compress: true,              // Compress old logs
            keepFileExt: true,           // Retain `.log` extension for rotated files
            daysToKeep: 14               // Auto-delete logs older than 14 days
        },
		combined: {                    // ✅ Combined appender for both console & file
            type: 'logLevelFilter',
            appender: 'file',
            level: 'info'
        }
    },
    categories: {
        default: {
            appenders: ['console', 'file'],  // ✅ Logs both to console and file
            level: 'debug'
        },
    }
});
const logger = log4js.getLogger('Helper');
const _ = require('lodash');
const util = require('util');
const fs = require('fs-extra');
const FabricCAServices = require('fabric-ca-client');
const { Gateway, Wallets } = require('fabric-network');

// const hfc = require('fabric-client');
const networks = global.netConfig
//hfc.addConfigFile(networks);
// hfc.setLogger(logger);
const { ObjectId, ObjectID } = require('mongodb');


let clients = {};
let channels = {};
let caClients = {};

// set up the client and channel objects for each org
for (let network in networks) {
	let ORGS = networks[network];
	for (let key in ORGS) {
	// 	logger.error(key);
		if (key.indexOf('org') === 0) {



		}
	}
}

function setupPeers(channel, org, client, ORGS) {
	for (let key in ORGS[org]) {
		if (key.indexOf('peer') === 0) {
			let data = '';
			if (ORGS[org][key].isFile === false) {
				data = ORGS[org][key]['tls_cacerts'];
			} else {
				data = fs.readFileSync(path.join(__dirname, ORGS[org][key]['tls_cacerts']));
			}

			let peer = client.newPeer(
				ORGS[org][key].requests,
				{
					pem: Buffer.from(data).toString(),
					// 'ssl-target-name-override': null,
					"grpc.keepalive_time_ms": 120000,
					"grpc.http2.min_time_between_pings_ms": 120000,
					"grpc.keepalive_timeout_ms": 20000,
					"grpc.http2.max_pings_without_data": 0,
					"grpc.keepalive_permit_without_calls": 1,
					'ssl-target-name-override': ORGS[org][key]['server-hostname']
				}
			);

			channel.addPeer(peer);
		}
	}
}

function newOrderer(client, ORGS) {
	let caRootsPath = ORGS.orderer.tls_cacerts;
	let data = '';
	if (ORGS.orderer.isFile === false) {
		data = caRootsPath;
	} else {
		data = fs.readFileSync(path.join(__dirname, caRootsPath));;
	}
	let caroots = Buffer.from(data).toString();
	return client.newOrderer(ORGS.orderer.url, {
		'pem': caroots,
		// 'ssl-target-name-override': null,
		"grpc.keepalive_time_ms": 120000,
		"grpc.http2.min_time_between_pings_ms": 120000,
		"grpc.keepalive_timeout_ms": 20000,
		"grpc.http2.max_pings_without_data": 0,
		"grpc.keepalive_permit_without_calls": 1,
		'ssl-target-name-override': ORGS.orderer['server-hostname']
	});
}

function readAllFiles(dir) {
	let files = fs.readdirSync(dir);
	let certs = [];
	files.forEach((file_name) => {
		let file_path = path.join(dir, file_name);
		let data = fs.readFileSync(file_path);
		certs.push(data);
	});
	return certs;
}

function getOrgName(org) {
	return org;
}

function getKeyStoreForOrg(org) {
	return '/tmp/fabric-client-kvs_' + org;
}
async function newRemotes(urls, forPeers, userOrg, username, channelName) {
	let targets = [];
	let client = await getClientForOrg(userOrg, username, channelName);
	let ORGS = networks[userOrg];
	if (!urls) {

		let orginization = ORGS.org;
		for (let key in orginization) {
			if (key.indexOf('peer') === 0) {
				let data = '';
				if (orginization[key].isFile === false) {
					data = orginization[key]['tls_cacerts'];
				} else {
					data = fs.readFileSync(path.join(__dirname, orginization[key]['tls_cacerts']));
				}
				targets.push(client.newPeer(orginization[key]['requests'], {
					pem: Buffer.from(data).toString(),
					// 'ssl-target-name-override': null,
					"grpc.keepalive_time_ms": 120000,
					"grpc.http2.min_time_between_pings_ms": 120000,
					"grpc.keepalive_timeout_ms": 20000,
					"grpc.http2.max_pings_without_data": 0,
					"grpc.keepalive_permit_without_calls": 1,
					'ssl-target-name-override': orginization[key]['server-hostname']
				}));
			}
		}
		return targets;
	}
	// find the peer that match the urls
	outer:
	for (let index in urls) {
		let peerUrl = urls[index];
		let found = false;
		for (let key in ORGS) {
			if (key.indexOf('org') === 0) {
				// if looking for event hubs, an app can only connect to
				// event hubs in its own org
				if (!forPeers) {
					continue;
				}
				let org = ORGS[key];
				for (let prop in org) {
					if (prop.indexOf('peer') === 0) {
						if (org[prop]['requests'].indexOf(peerUrl) >= 0) {
							// found a peer matching the subject url
							if (forPeers) {
								logger.info(">>>>>>>>>>>>>>>>>" + org[prop]['server-hostname']);
								let data = '';
								if (org[prop].isFile === false) {
									data = org[prop]['tls_cacerts'];
								} else {
									data = fs.readFileSync(path.join(__dirname, org[prop]['tls_cacerts']));
								}


								targets.push(client.newPeer('grpcs://' + peerUrl, {
									pem: Buffer.from(data).toString(),
									// 'ssl-target-name-override': null,
									"grpc.keepalive_time_ms": 120000,
									"grpc.http2.min_time_between_pings_ms": 120000,
									"grpc.keepalive_timeout_ms": 20000,
									"grpc.http2.max_pings_without_data": 0,
									"grpc.keepalive_permit_without_calls": 1,
									'ssl-target-name-override': org[prop]['server-hostname']
								}));

								continue outer;
							} else {
								let eh = client.newEventHub();

								let data = '';

								if (org[prop].isFile === false) {
									data = org[prop]['tls_cacerts'];
								} else {
									data = fs.readFileSync(path.join(__dirname, org[prop]['tls_cacerts']));
								}


								eh.setPeerAddr(org[prop]['events'], {
									pem: Buffer.from(data).toString(),
									// 'ssl-target-name-override': null,
									"grpc.keepalive_time_ms": 120000,
									"grpc.http2.min_time_between_pings_ms": 120000,
									"grpc.keepalive_timeout_ms": 20000,
									"grpc.http2.max_pings_without_data": 0,
									"grpc.keepalive_permit_without_calls": 1,
									'ssl-target-name-override': org[prop]['server-hostname']
								});
								targets.push(eh);

								continue outer;
							}
						}
					}
				}
			}
		}
		if (!found) {
			logger.error(util.format('Failed to find a peer matching the url %s', peerUrl));
		}
	}

	return targets;
}

//-------------------------------------//
// APIs
//-------------------------------------//
let getChannelForOrg = function (org, channelName) {
	return channels[org][channelName];
};

async function buildWallet(Wallets, walletPath) {
	// Create a new  wallet : Note that wallet is for managing identities.
	let wallet
	if (walletPath) {
		wallet = await Wallets.newFileSystemWallet(walletPath)
		logger.info(`Built a file system wallet at ${walletPath}`)
	} else {
		wallet = await Wallets.newInMemoryWallet()
		logger.info('Built an in memory wallet')
	}
	return wallet
}

function initializeGateway(network, networkName) {
	// load the common connection configuration file

	let peerAddressList = [],
		certificateAuthoritiesList = [],
		peers = {},
		caObject = {},
		mspId

	// console.log('world', JSON.stringify(network))
	for (let key in network.org) {
		// console.log('key' , key)
		if (key == 'mspid') {
			mspId = network.org[key]
		} else if (key == 'ca') {
			let caObjectSelected = _.get(network, `org.${key}`, {})
			let caDNSName = caObjectSelected.hostname
			certificateAuthoritiesList.push(caDNSName)
			caObject = {
				[caDNSName]: {
					url: caObjectSelected.url,
					caName: caObjectSelected.caName,
					tlsCACerts: {
						pem: caObjectSelected.tlsCerts
					},
					httpOptions: {
						verify: false
					}
				}
			}
		} else if (key.indexOf('peer') > -1) {
			let peerObjectSelected = _.get(network, `org.${key}`, {})
			// console.log(peerObjectSelected)
			let peerDNSName = _.get(network, `org.${key}.${'server-hostname'}`, '')
			// console.log('perrhostname', peerDNSName)

			// console.log('tls_cacerts 1', peerObjectSelected.tls_cacerts)

			peerAddressList.push(peerDNSName)
			let peerTemp = {
				url: peerObjectSelected.requests,
				tlsCACerts: {
					pem: peerObjectSelected.tls_cacerts
				},
				grpcOptions: {
					'ssl-target-name-override': peerDNSName,
					hostnameOverride: peerDNSName
				}
			}
			//_.set(, peerDNSName, peerTemp)
			peers[peerDNSName] = peerTemp
			// console.log('hi there', peers)
		}
	}
	let response = {
		name: networkName,
		version: '1.0.0',
		client: {
			// organization: mspId.replace('MSP', ''),
			organization: mspId,
			credentialStore: {
				path: './hfc-key-store',
				cryptoStore: {
					path: './hfc-key-store'
				}
			},
			connection: {
				timeout: {
					peer: {
						endorser: '900000'
					}
				}
			}
		},
		organizations: {
			// [mspId.replace('MSP', '')]: {
			[mspId]: {
				mspid: mspId,
				peers: peerAddressList,
				certificateAuthorities: certificateAuthoritiesList
			}
		},
		peers: peers,
		certificateAuthorities: caObject
	}
	// console.log('bestResponse', JSON.stringify(response, null, 2))

	return response
}

function buildCAClient(FabricCAServices, ccp, caHostName) {
	// Create a new CA client for interacting with the CA.
	const caInfo = ccp.certificateAuthorities[caHostName] //lookup CA details from config
	const caTLSCACerts = caInfo.tlsCACerts.pem
	const caClient = new FabricCAServices(
		caInfo.url,
		{ trustedRoots: caTLSCACerts, verify: false },
		caInfo.caName
	)

	logger.info(`Built a CA Client named ${caInfo.caName}`)
	return caClient
}

async function connectToNetwork(networkaaa, networks) {
	try {
		// Load the network configuration from a common connection profile (e.g., JSON)
		// const ccpPath = path.resolve(__dirname, 'connection-profile.json');
		const ccp = networks[networkaaa];
		// console.log(ccp.org, networkaaa);
		// Load the network organizations
		const ORGS = ccp;

		let username = 'admin'

		// console.log(ORGS, username, networkaaa);
		const connectionProfile = initializeGateway(ccp, networkaaa);
		// Set up the CA client
		// const caInfo = ORGS.org//certificateAuthorities['ca.org1.example.com'];
		// const caClient = new FabricCAServices(caInfo.ca.url);
		// const caClient = buildCAClient2(FabricCAServices, networkaaa);
		const caClient = buildCAClient(FabricCAServices, connectionProfile, ccp.org.ca.caName);

		// Set up the wallet to store identities
		const walletPath = path.join(process.cwd(), 'wallet');
		const wallet = await buildWallet(Wallets, walletPath);// Wallets.newFileSystemWallet(walletPath);



		// Enroll the admin user (e.g., 'admin' identity)
		const orgMspId = 'org1MSP';
		// await enrollAdmin(caClient, wallet, orgMspId);
		await enrollAdmin(caClient, wallet, ccp.org.mspid, _.get(ccp, 'users[0].username', undefined), _.get(ccp, 'users[0].secret', undefined));

		// Use the admin identity for registering and enrolling new users
		const adminIdentity = await wallet.get(_.get(ccp, 'users[0].username', undefined));
		if (!adminIdentity) {
			logger.warn('Admin identity not found in the wallet. Please enroll admin first.');
			return;
		}
		// console.log('Admin identity not found in the wallet. Please enroll admin first.',adminIdentity);
		let affiliation = 'org1.department1'
		// Register and enroll a user (e.g., 'user1')
		// await registerAndEnrollUser(caClient, wallet, orgMspId, 'user1', adminIdentity);
		
		await registerAndEnrollUser(caClient, wallet, ccp.org.mspid, username, affiliation, adminIdentity, null, _.get(ccp, 'users[0].username', undefined));

		// Set up wallet to manage identities (This replaces the old CryptoSuite and key store)
		// const walletPath = path.join(process.cwd(), 'wallet');
		// const wallet = await Wallets.newFileSystemWallet(walletPath);

		// Check if the user already exists in the wallet
		// const userExists = await wallet.get(username);
		// if (!userExists) {
		//     console.log(`User ${username} does not exist in the wallet`);
		//     return;
		// }

		// Create a new gateway for connecting to our peer node
		const gateway = new Gateway();
		// console.log("LLLLLLLLLLLLLLAALALALALALLALALA", connectionProfile);
		await gateway.connect(connectionProfile, {
			wallet,
			identity: username,
			discovery: { enabled: true, asLocalhost: false } // Adjust asLocalhost depending on your setup
		});

		// Get the network (channel) our contract is deployed to
		// console.log("LLLLLLLLLLLLLLAALALALALALLALALA");
		// const network = await gateway.getNetwork('avanzachannel');

		// Create a CA client for interacting with the CA
		// const caUrl = ORGS.certificateAuthorities[`${networkName}-ca`].url;
		// const caClient = new FabricCAServices(caUrl);

		logger.info('Successfully connected to network');
		return gateway//, network, caClient ;
	} catch (error) {
		logger.error(`Failed to connect to network: ${error}`);
		process.exit(1);
	}
}

let getClientForOrg = async function (network, username, channelName) {

	let gateway = await connectToNetwork(network, networks)
	return gateway;
};

async function registerAndEnrollUser(
	caClient,
	wallet,
	orgMspId,
	userId,
	affiliation,
	adminCertificate,
	timestamp,
	adminId
) {
	try {
		const crypto = require('crypto')

		console.log('wallet userId' , userId)

		// // Must use an admin to register a new user
		// const adminIdentity = adminCertificate
		// if (!adminIdentity) {
		//   console.log('An identity for the admin user does not exist in the wallet')
		//   console.log('Enroll the admin user before retrying')
		//   let message = 'An identity for the admin user does not exist in the wallet';
		//   let response = {
		//     success: false,
		//     message: message
		// };
		// return response;
		//   // return
		// }

		if (wallet && wallet.get) {
			let identity = await wallet.get(userId)
			if (identity) {
				logger.warn(
					'An identity for the user already exists in the wallet'
				)
				return
			}
		}
		//   console.log("adminIdentity",adminCertificate);

		// build a user object for authenticating with the CA
		//   console.log("adminIdentity.type",adminCertificate.type);
		const provider = wallet
			.getProviderRegistry()
			.getProvider(adminCertificate.type)
		const adminUser = await provider.getUserContext(adminCertificate, adminId)

		// Register the user, enroll the user, and import the new identity into the wallet.
		// if affiliation is specified by client, the affiliation value must be configured in CA
		const secret = await caClient.register(
			{
				affiliation: affiliation,
				enrollmentID: userId,
				role: 'client'
			},
			adminUser
		)
		const enrollment = await caClient.enroll({
			enrollmentID: userId,
			enrollmentSecret: secret
		})
		const pubKeyObject = crypto.createPublicKey({
			key: enrollment.key.toBytes(),
			format: 'pem'
		})

		const publicKey = pubKeyObject.export({
			format: 'pem',
			type: 'spki'
		})

		const x509Identity = {
			credentials: {
				certificate: enrollment.certificate,
				privateKey: enrollment.key.toBytes(),
				publicKey: publicKey
			},
			mspId: orgMspId,
			type: 'X.509'
		}
		await wallet.put(userId, x509Identity)
		logger.info(
			`Successfully registered and enrolled user ${userId} and imported it into the wallet`
		)
		x509Identity.userId = userId
		x509Identity.role = 'user'
		x509Identity.timestamp = timestamp

		//   await wallet.put(adminId, x509Identity)
		// storeCredential.storeCredential(x509Identity)
		let message = `Successfully registered and enrolled user ${userId} and imported it into the wallet`;
		let response = {
			success: true,
			key: x509Identity,
			message: message
		};
		return response;
	} catch (error) {
		logger.error(`Failed to register user : ${error}`)
		let message = `Failed to register user : ${error}`;
		let response = {
			success: false,
			message: message
		};
	}
}
async function enrollAdmin(
	caClient,
	wallet,
	orgMspId,
	adminUserId,
	adminUserPasswd
) {
	try {
		// Check to see if we've already enrolled the admin user.

		let identity
		if (wallet && wallet.get) {
			identity = await wallet.get(adminUserId)
			if (identity) {
				logger.warn(
					'An identity for the admin user already exists in the wallet'
				)
				return
			}
		}

		// Enroll the admin user, and import the new identity into the wallet.
		const enrollment = await caClient.enroll({
			enrollmentID: adminUserId,
			enrollmentSecret: adminUserPasswd
		})
		const x509Identity = {
			credentials: {
				certificate: enrollment.certificate,
				privateKey: enrollment.key.toBytes()
			},
			mspId: orgMspId,
			type: 'X.509'
		}
		await wallet.put(adminUserId, x509Identity)
		logger.info(
			'Successfully enrolled admin user and imported it into the wallet'
		)
	} catch (error) {
		logger.error(`Failed to enroll admin user : ${error}`, error.stack)
	}
}

// let getClientForOrg = function (network, username, channelName) {
// 	// console.log(network, null, 2)
// 	// console.log(channelName, null, 2)
// 	let Cache = _.get(clients, `${network}.${channelName}`, false);
// 	// if (Cache !== false) {
// 	// 	logger.info("returning cached client!!!")
// 	// 	return Promise.resolve(Cache);
// 	// }
// 	// logger.debug('Msp ID : ' +network);
// 	console.log(network);
// 	let ORGS = networks[network];
// 	let generalClient = new hfc();
// 	let cryptoSuite = hfc.newCryptoSuite();

// 	cryptoSuite.setCryptoKeyStore(hfc.newCryptoKeyStore({ path: getKeyStoreForOrg(ORGS.org.name) }));
// 	generalClient.setCryptoSuite(cryptoSuite);

// 	//let generalClient = _.cloneDeep(clients[network]);
// 	let channel = generalClient.newChannel(channelName);
// 	channel.addOrderer(newOrderer(generalClient, ORGS));
// 	setupPeers(channel, 'org', generalClient, ORGS);
// 	_.set(clients, `${network}.${channelName}`, generalClient);
// 	_.set(channels, `${network}.${channelName}`, channel);
// 	let caUrl = ORGS.org.ca;
// 	let caClient = new FabricCAClient(caUrl, null /*defautl TLS opts*/, '' /* default CA */, cryptoSuite);
// 	return assignUserContextUsers(username, network, generalClient, caClient).then((finalClient) => {
// 		_.set(clients, `${network}.${channelName}`, finalClient);
// 		return finalClient;
// 	})
// };

let newPeers = function (urls, network, username, channelName) {
	return newRemotes(urls, true, network, username, channelName);
};

let newEventHubs = function (urls, org, username, channelName) {
	return newRemotes(urls, false, org, username, channelName);
};

let getMspID = function (org) {
	// logger.debug('Msp ID : ' + networks[org].org.mspid);
	return networks[org].org.mspid;
};



// let assignUserContextUsers = function (username, userOrg, client, caClient) {
// 	let admin = networks[userOrg].org.admin;
// 	let keyPEM;
// 	let certPEM;
// 	if (admin.isFile !== true) {
// 		keyPEM = admin.key
// 		certPEM = admin.cert
// 	} else {
// 		let keyPath = path.join(__dirname, admin.key);
// 		keyPEM = Buffer.from(readAllFiles(keyPath)[0]).toString();
// 		let certPath = path.join(__dirname, admin.cert);
// 		certPEM = readAllFiles(certPath)[0].toString();
// 	}
// 	let member;
// 	let enrollmentSecret = null;
// 	return hfc.newDefaultKeyValueStore({
// 		path: getKeyStoreForOrg(getOrgName(userOrg))
// 	}).then((store) => {
// 		client.setStateStore(store);
// 		// clearing the user context before switching
// 		client._userContext = null;

// 		return client.createUser({
// 			username: 'admin',
// 			mspid:  getMspID(userOrg),
// 			cryptoContent: {
// 				privateKeyPEM: keyPEM,
// 				signedCertPEM: certPEM
// 			}})
			
// 		// client.getUserContext(username, true).then((user) => {
// 		// 	if (user && user.isEnrolled()) {
// 		// 		logger.info('Successfully loaded member from persistence');
// 		// 		return user;
// 		// 	} else {
// 		// 		return getAdminUser(userOrg, client, caClient).then(function (adminUserObj) {
// 		// 			member = adminUserObj;
// 		// 			return caClient.register({
// 		// 				enrollmentID: username,
// 		// 				affiliation: 'ibp.PeerOrg1'
// 		// 			}, member);
// 		// 		}).then((secret) => {
// 		// 			enrollmentSecret = secret;
// 		// 			logger.debug(username + ' registered successfully');
// 		// 			return caClient.enroll({
// 		// 				enrollmentID: username,
// 		// 				enrollmentSecret: secret
// 		// 			});
// 		// 		}, (err) => {
// 		// 			logger.error(err);
// 		// 			//return '' + err;
// 		// 			logger.error(username + ' enrollment failed');
// 		// 			return 'Failed to register ' + username + '. Error: ' + err.stack ? err.stack : err;
// 		// 		}).then((message) => {
// 		// 			if (message && typeof message === 'string' && message.includes(
// 		// 				'Error:')) {
// 		// 				logger.error(username + ' enrollment failed');
// 		// 				return message;
// 		// 			}
// 		// 			logger.debug(username + ' enrolled successfully');

// 		// 			member = new User(username);
// 		// 			member._enrollmentSecret = enrollmentSecret;
// 		// 			return member.setEnrollment(message.key, message.certificate, getMspID(userOrg));
// 		// 		}).then(() => {
// 		// 			client.setUserContext(member);
// 		// 			return member;
// 		// 		}, (err) => {
// 		// 			logger.error(util.format('%s enroll failed: %s', username, err.stack ? err.stack : err));
// 		// 			return '' + err;
// 		// 		});;
// 		// 	}
// 		// });
// 	}).then((user) => {
// 		return client;
// 	}, (err) => {
// 		logger.error(util.format('Failed to get registered user: %s, error: %s', username, err.stack ? err.stack : err));
// 		return '' + err;
// 	});
// };



let getOrgAdmin = function (userOrg, client) {
	let admin = networks[userOrg].org.admin;
	let keyPEM;
	let certPEM;
	if (admin.isFile !== true) {
		keyPEM = admin.key
		certPEM = admin.cert
	} else {
		let keyPath = path.join(__dirname, admin.key);
		keyPEM = Buffer.from(readAllFiles(keyPath)[0]).toString();
		let certPath = path.join(__dirname, admin.cert);
		certPEM = readAllFiles(certPath)[0].toString();
	}
	let cryptoSuite = hfc.newCryptoSuite();
	if (userOrg) {
		cryptoSuite.setCryptoKeyStore(hfc.newCryptoKeyStore({ path: getKeyStoreForOrg(getOrgName(userOrg)) }));
		client.setCryptoSuite(cryptoSuite);
	}

	return hfc.newDefaultKeyValueStore({
		path: getKeyStoreForOrg(getOrgName(userOrg))
	}).then((store) => {
		client.setStateStore(store);

		return client.createUser({
			username: 'admin',
			mspid: getMspID(userOrg),
			cryptoContent: {
				privateKeyPEM: keyPEM,
				signedCertPEM: certPEM
			}
		});
	});
};

let setupChaincodeDeploy = function (network) {
	logger.info(path.join(__dirname, "../dist/" + network + "/"));
	process.env.GOPATH = path.join(__dirname, "../dist/" + network + "/");
};

let getLogger = function (moduleName) {
	let logger = log4js.getLogger(moduleName);
	return logger;
};

let getPeerAddressByName = function (org, peer) {
	let address = ORGS[org][peer].requests;
	return address.split('grpcs://')[1];
};

let getOrgByName = function (org) {
	//console.log(JSON.stringify(ORGS))
	return ORGS[org];
}

let getOrgPeers = function (network) {
	return Object.keys(networks[network].org).length - 4;
}

function parseObjectIds (value) {
	if (!value) return null;
	return new ObjectId(value);
}

function returnEntityObject (document){
	return {
		"taxNO1" : "",
		"taxNO2" : "",
		"taxAddress" : "",
		"publicKey" : "",
		"entityName" : document?.name,
		"arabicName" : document?.name,
		"clientKey" : "",
		"clientSecret" : "",
		"spCode" : document?.key,
		"shortCode" : "",
		"orgType" : document?.orgType,
		"isActive" : false,
		"isMember": document.isMember ? document.isMember : false,
		"isGenratedMembership" : false,
		"newUserPolicy" : "Change Password",
		"newLoginPolicy" : "",
		"membershipNo" : {
			"prefix" : "",
			"trailingZeroes" : ""
		},
		"address" : document?.address,
		"entityLogo" : {
			"sizeSmall" : document?.image,
			"sizeMedium" : null
		},
		"parentEntity" : "",
		"commissionTemplate" : "",
		"contacts" : [
			...document.contacts
			// {
			// 	"contactName" : "Moad Al Mazoori",
			// 	"email" : "moad@qgc.com",
			// 	"mobile" : "456415321053",
			// 	"POBox" : "456415321053",
			// 	"contactType" : "Email",
			// 	"emailTemplate" : "62188096f0a23e543ce802f3"
			// }
		],
		"departments" : [
	
		],
		"mappedCodes" : [
	
		],
		"additionalProps" : [
	
		],
		"documents" : [
	
		],
		"dateCreated" : document?.dateCreated,
		"createdBy" : new ObjectId("5a8d83bdd181225bf076fb24"), //document?.createdBy,
		"dateUpdated" : 1739853340115.0,
		"updatedBy" : "",
		"emailOTP" : false,
		"dailyRequestQuotaLimit" : 0,
		"monthlyRequestQuotaLimit" : 0,
		"smsOTP" : false,
		"emailTemplate" : "62188096f0a23e543ce802f3",
		"smsTemplate" : false,
		"emailOTPLogin" : false,
		"smsOTPLogin" : false,
		"emailTemplateLogin" : "",
		"smsTemplateLogin" : false,
		"status" : {
			"value" : "Approved",
			"type" : "Info"
		},
		"callbackEP" : "",
		"buisnessCategory" : [
	
		],
		"cycle" : "",
		"Login" : false,
		"currency" : "",
		"actions" : [
	
		],
		"lastReconDate" : "",
		"status" : {
			"value" : "Approved",
			"type" : "Info"
		},
	}
}

function returnUserObject (document) {
	return {
		"__v" : 0,
		"allowedIPRange" : [
			"*.*.*.*"
		],
		"authType" : "System",
		"countryOfResidence" : "",
		"createdAt" : document?.createdAt,
		"department" : "",
		"documentExpiry" : "",
		"documentNo" : "",
		"documentType" : "",
		"documents" : [
	
		],
		"email" : document?.email,
		"firstName" : document?.firstName,
		"firstScreen" : "/insurancechain/claimlist",
		"groups" : [
			new ObjectId("65a53697d017c8a9bb994500")
		],
		"isActive" : document?.isActive,
		"isFresh" : false,
		"isNewUser" : false,
		"kycStatus" : "Pending",
		"lastName" : document?.lastName,
		"lastResetTime" : 1706253186589.0,
		"mobileNumber" : document?.mobileNumber,
		"nationality" : "",
		"orgCode" : document?.orgCode,
		"orgType" : document?.orgType,
		"password" : document?.password,
		"passwordHashType" : "sha512",
		"passwordPolicy" : new ObjectId("5fd1bea0f0e35434a87c6330"),
		"passwordRetries" : 0,
		"passwordUpdatedAt" : document?.passwordUpdatedAt,
		"pvtKeyAccount" : "0xd41d5423f26b78d074c6a4aba6c172f80923c5c1ce253ae2f6eefd7b5751a8db",
		"quorrumUser" : "0xa1f404748517F58c5049d252cDcF1370Ae33529D",
		"services" : [
	
		],
		"status" : "APPROVED",
		"unit" : "",
		"updatedAt" : document?.updatedAt,
		"userID" : document?.userID,
		"userId" : null,
		"userLanguage" : "EN",
		"userList" : [
	
		],
		"userSubType" : "",
		"userType" : "Human-OP",
		"lastLoginTime" : "September 27th 2024, 12:15:01 pm",
		"updatedBy" : new ObjectId("5a8d83bdd181225bf076fb24"),//document?.updatedBy,
		"passwordRetryAt" : 1717999344968.0
	}
} 

exports.getOrgPeers = getOrgPeers;
exports.getOrgByName = getOrgByName;
exports.getChannelForOrg = getChannelForOrg;
exports.getClientForOrg = getClientForOrg;
exports.getLogger = getLogger;
exports.setupChaincodeDeploy = setupChaincodeDeploy;
exports.getMspID = getMspID;
exports.newPeers = newPeers;
exports.newEventHubs = newEventHubs;
exports.getPeerAddressByName = getPeerAddressByName;
exports.getOrgAdmin = getOrgAdmin;
exports.returnEntityObject = returnEntityObject;
exports.returnUserObject = returnUserObject;
exports.parseObjectIds = parseObjectIds;
