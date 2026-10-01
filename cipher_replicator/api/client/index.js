
const amqp = require('./amqp');
// const pg = require('./pg');
const mssql = require('./mssql')

module.exports = {
    createClient:  async function (type, connectionURL,asqueu) {
        let client;
        switch (type) {
            case 'amqp':
                client = await amqp(connectionURL,asqueu);
                break;
            // case 'postgres':
            // case 'pg':
            //     client = await pg(connectionURL);
            //     break;
            case 'mssql':
                client = await mssql.getPool(null, connectionURL);
                break;
            default:
            break;
        }
        return client;
    }
}