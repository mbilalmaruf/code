const CronService = require('./cron.service');
const dataTableService = require('./DataTable.service');

const service = {};

service.cron = CronService;
service.dataTableService = dataTableService;

module.exports = service;
