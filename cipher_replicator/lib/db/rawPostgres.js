const assert = require("assert");
const _ = require("lodash");
const crypto = require('../helpers/crypto');
const pg = require("pg");
const sql = require("mssql");

let _db;

async function initDb() {
    if (_db) {
        console.warn("Trying to init DB again!");
        return {
            message: "Trying to init DB again!",
            status: true,
            instance: _db
        };
    }

    const dbType = _.get(global, "config.dbType", "postgres").toLowerCase();
    console.log("dbType -->", dbType);

    try {
        if (dbType === "postgres") {
            const connectionStringEncrypted = _.get(global, "config.connectionString", "");
            const connectionString = crypto.decrypt(connectionStringEncrypted);

            const client = new pg.Client({ connectionString });
            await client.connect();
            _db = client;

            console.info("PostgreSQL DB initialized");
            return {
                message: "PostgreSQL DB initialized",
                status: true,
                instance: _db
            };
        }

        else if (dbType === "mssql") {
            const encryptedConfig = _.get(global, "config.mssqlConfig", "");
            const decryptedConnObjStr = crypto.decrypt(encryptedConfig);
            console.log("decryptedConnObjStr->", decryptedConnObjStr);

            let connConfig;
            try {
                connConfig = JSON.parse(decryptedConnObjStr);
            } catch (e) {
                console.error("Failed to parse decrypted MSSQL connection string as JSON");
                throw e;
            }

            connConfig.options = connConfig.options || {
                encrypt: true,
                trustServerCertificate: true,
            };

            _db = await sql.connect(connConfig);

            console.info("MSSQL DB initialized");
            return {
                message: "MSSQL DB initialized",
                status: true,
                instance: _db
            };
        }

        else {
            throw new Error(`Unsupported DB Type: ${dbType}`);
        }

    } catch (err) {
        console.error(`Error initializing ${dbType} DB:`, err);
        return {
            message: `Error initializing ${dbType} DB`,
            status: false,
            error: err,
            instance: null
        };
    }
}

function getDb() {
    // console.log("DB->", _db)
    assert.ok(_db, "DB has not been initialized. Please call initDb first.");
    return _db;
}

module.exports = {
    getDb,
    initDb
};
