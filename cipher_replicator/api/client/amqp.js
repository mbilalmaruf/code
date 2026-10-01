const amqplib = require('amqp-connection-manager');
const crypto = require('crypto');
var AMQPExistingList = {};
const rp = require('request-promise');

module.exports = async function (connectionURL, asqueue) {
    const hash = crypto.createHash('sha512').update(connectionURL + asqueue).digest("hex");

    const createConnection = () => {
        const conn = amqplib.connect([connectionURL]);
        conn.on("close", function () {
            console.error("[AMQP] reconnecting");
            return setTimeout(createConnection, 7000);
        });
        let channelWrapper = conn.createChannel(asqueue);
        AMQPExistingList[hash] = channelWrapper;
        return channelWrapper;
    };





    if (!AMQPExistingList[hash]) {
        console.log('Creating a MQ instance');
        try {
            return createConnection();

        } catch (err) {
            console.log(err);
            // ErrorCapturing.instance().recordError(err, '003');
            const options = {
                method: 'POST',
                uri: `${global.errorCaptureURL}/captureReplicationError`,
                headers: {
                    'User-Agent': 'Request-Promise',
                    'Content-Type': 'application/json',
                    'Accept': '*/*',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'x-auth-key': global.avanzaISC.password
                },
                body: {
                    typeCode: "003",
                    payload: err.stack
                },
                json: true
              };
        
              console.log("option =>>>> ", options);
              await rp(options);
            setTimeout(createConnection, 7000);
        }


    }
    return AMQPExistingList[hash];
};