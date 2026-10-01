const schedule = require('node-schedule');

let cronJob;
class Cron {
    constructor() {}

    static async start(job, interval) {
        cronJob = schedule.scheduleJob(interval, job);
    }
    static async stop() {
        cronJob.cancel();
    }
}


module.exports = Cron;
