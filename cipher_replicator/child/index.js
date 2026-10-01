'use strict';
const _ = require('lodash');


process.on('message', async (params) => {
    require('./logger');

    // console.log('Final params:', JSON.stringify(params, null, 2));

    if (!params) {
        logger.warn("Error in loading configurations!")
        return;
    }
    global.config = params.globe;
    global.netConfig = params.netConfig;

    const helper = require('../eventConnect/helper')
    const logger = helper.getLogger('index.js/child')
    const pg = require("../lib/db/rawPostgres");
    const schemaProfile = require("../eventConnect/schemaProfile.js");
    await replicatorInstance();
    async function replicatorInstance() {
        // get schema config
        const schemaProfileName = _.get(global.config, 'schemaProfileName', '');
        // console.log("schemaProfileName ->", schemaProfileName);

        const profile = await schemaProfile.get(schemaProfileName);
        logger.info(profile);
        if (!profile) {
            logger.warn(`schemaProfile not found, ${schemaProfileName}`)
            process.exit(1)
        }
        // console.log('Final profile:', JSON.stringify(profile, null, 2));

        global.config.schemaConfig = profile.schemaConfig;
        global.config.schema = profile.schema;

        (global.config.db == 'pg' || global.config.db == 'mssql') && pg.initDb();
        const start = require('../lib/start');
        start.server().then(isSuccess => {
            let meta = require('../lib/db/postgres').sequelize.models['meta'];
            let meta_proxy = require('../lib/db/postgres').sequelize.models['meta_proxy'];
            if (!meta && !meta_proxy) {
                logger.info("Failed to initialize meta tables")
                return false;
            }
            const event = require('../eventConnect/eventEmitter');
            logger.info("Starting Replicator Process")
            if (isSuccess) {
                event.listenPeerEvents(
                    _.get(global.config, 'blockChainConfiguration.user', 'admin'),
                    _.get(global.config, 'blockChainConfiguration.network', 'notfound'),
                    _.get(global.config, 'blockChainConfiguration.ListenPeer', 'notfound'),
                    _.get(global.config, 'blockChainConfiguration.smartContract', 'notfound'),
                    _.get(global.config, 'blockChainConfiguration.chainEvent', 'notfound')
                    , (data) => {
                        data.forEach(element => {
                            start.eventHandle(element);
                        });
                    }, _.get(global.config, 'blockChainConfiguration.channel', 'notfound'))

                logger.info('server started successfully');
            }
        })
    }
});