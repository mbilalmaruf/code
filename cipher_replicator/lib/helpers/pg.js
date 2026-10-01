const { Pool, types } = require("pg");
const _ = require("lodash");
const crypto = require("crypto");
const ErrorCapturing = require("../../lib/Prometheus/ErrorCapturing");
const rp = require("request-promise");

var PGExistingList = {};

let connection;
let retryCount = 0;

module.exports = async function (connectionURL) {
  const connectionStrings = connectionURL;

  const makeConnection = async (index = 0) => {
    if (retryCount >= 5) {
      throw new Error("PG connection limit reached.");
    }

    const hash = crypto.createHash("md5").update(connectionStrings[index]).digest("hex");

    if (PGExistingList[hash]) {
      return PGExistingList[hash]
    }else{
      try {


        const pool = new Pool({
          max: _.get(global.config, "postgres.maxConnections", 20),
          connectionString: connectionStrings[index],
          connectionTimeoutMillis: _.get(
            global.config,
            "postgres.connectionTimeoutMillis",
            30000
          ),
          idleTimeoutMillis: _.get(
            global.config,
            "postgres.idleTimeoutMillis",
            5000
          ),
        });
  
        const client = await pool.connect();
        const res = await client.query("SELECT pg_is_in_recovery()");
  
        if (res.rows[0].pg_is_in_recovery) {
          client.release();
          throw new Error("Database is in recovery mode.");
        }
  
        types.setTypeParser(types.builtins.INT8, value => parseInt(value, 10));
        types.setTypeParser(types.builtins.NUMERIC, value => parseFloat(value));
        retryCount = 0; // Reset retry count on successful connection
        PGExistingList[hash] = client
        return client;
      } catch (err) {
        retryCount++;
        ErrorCapturing.instance().recordError(err, "001");
  
        const options = {
          method: "POST",
          uri: `${global.errorCaptureURL}/captureReplicationError`,
          headers: {
            "User-Agent": "Request-Promise",
            "Content-Type": "application/json",
            Accept: "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            Connection: "keep-alive",
            "x-auth-key": global.avanzaISC.password,
          },
          body: {
            typeCode: "001",
            payload: err.stack,
          },
          json: true,
        };
  
        console.log("option =>>>> ", options);
        await rp(options);
  
        console.error(`Error connecting to database: ${err.message}`);
        const nextIndex = (index + 1) % connectionStrings.length;
        await new Promise((resolve) => setTimeout(resolve, 5000));
        return makeConnection(nextIndex);
      }
    }
   
  };

  try {
    if (!connection) {
      connection = await makeConnection();
    } else {
      // Check if the existing connection is still valid by executing a simple query
      await connection.query("SELECT 1 + 1");
    }
  } catch (err) {
    // If the existing connection is not valid or an error occurred, attempt to reconnect
    connection = await makeConnection();
  }

  return connection;
};