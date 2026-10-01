const { fork } = require('child_process');
const config = global.config;
const _ = require("lodash");

function startSchedulers (datatables){

    // console.log('datatables===>' , datatables)
   
    function forkTables(params) {
        console.log('forking tables==>',params)
        let datatableController = fork('controllers/DataTable.controller.js');
        datatableController.send({ config, params });
        datatableController.on('close', () => {
            console.log('reverse replicator process for ' + params.documentName + ' closed !!!');
            setTimeout(forkTables.bind(null, params), 1000);
        });
    }


    try {
        startDataTableSchedule(datatables)
    } catch (error) {
        console.error('Error fetching tables:', error);
    }



    function startDataTableSchedule(datatables){
        for (let i = 0; i < datatables.length; i++) {
            forkTables(datatables[i]);
        }
    } 
};


module.exports = startSchedulers