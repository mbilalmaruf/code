const mongoose = require('mongoose');
const crypto = require('crypto');
const helper = require('../../eventConnect/helper')
const logger = helper.getLogger('Mongoose')

const MGExistingList = {};

module.exports = async function (connectionURL) {
    const createConnection = async () => {
        await mongoose.connect(connectionURL, {
            serverSelectionTimeoutMS: 5000,
            socketTimeoutMS: 45000,
            maxPoolSize: 10
        });

        const connection = mongoose.connection;  // Correct way to access the connection object in Mongoose 8

        connection.on('disconnected', () => {
            logger.error('-> mongoose lost connection');
            process.exit(0);
        });

        connection.on('connected', () => logger.info('-> mongoose connected'));

        return connection;
    };

    const hash = crypto.createHash('md5').update(connectionURL).digest('hex');

    if (MGExistingList[hash]) {
        logger.info('Returning an existing Mongo instance');
        logger.info('Connection state:', MGExistingList[hash].readyState);

        if (MGExistingList[hash].readyState !== 1) {
            MGExistingList[hash] = await createConnection();
        }
    } else {
        try {
            MGExistingList[hash] = await createConnection();
        } catch (err) {
            logger.error('Connection error:', err.message);
        }
    }

    return MGExistingList[hash];
};