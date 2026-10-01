const _ = require('lodash');
const { cron , dataTableService } = require('../services');
const pg = require('../api/connectors/postgress');
const mssql = require('../api/connectors/mssql')

const initiate = async (params) => {
    try {
        const dbType = _.get(global.config , 'db' , 'pg' ) 
        let isRunning = false;
        let conn = dbType == 'pg' ? await pg.connection() : await mssql.connection()
        cron.start(async () => {
            console.log(params.documentName + ' dataTableService cron execution in progress: ', isRunning);
            if (!isRunning) {
                try {
                    isRunning = true;
                    await dataTableService.handleDataTableExecution(params , conn , dbType);
                    isRunning=false;
                } catch (error) {
                    console.log(params.name + ' dataTableService cron execution catch !!!', error);
                    isRunning = false;
                }
            }
        }, _.get(global.config, 'cronInterval', '*/5 * * * * *'));
    } catch (error) {
        console.log(error.message);
    }
};

process.on('message', ({ config, params }) => {
    try {
        global.config = config;
        initiate(params);
    } catch (err) {
        process.send({ error: err.stack || err });
    }
});
