'use strict';

const _ = require('lodash');
const date = require('../../lib/helpers/dates');
const fromDate = 'fromDate';
const toDate = 'toDate';
const reconStatus = 'transactionStatus';

function matching(filters, payload) {

    const query = {};
    for (const param in payload) {
        const key = _.find(filters, {id: param});
        if (!_.isEmpty(key)) {
            if(param === "transactionStatus"){
                for(let i=0 ; i<payload[param].length ; i++){
                    if(payload[param][i] === "true"){
                        payload[param][i] = true;
                    }
                    if(payload[param][i] === "false"){
                        payload[param][i] = false;
                    }
                }
            }
            if (param === fromDate) {
                query[key.couchField] = query[key.couchField] || {};
                query[key.couchField] = Object.assign(query[key.couchField], {$gte: date.ddMMyyyyFromDate(payload[param]) / 1000});
            }
            if (param === toDate) {
                query[key.couchField] = query[key.couchField] || {};
                query[key.couchField] = Object.assign(query[key.couchField], {$lte: (date.ddMMyyyyFromDate(payload[param]) / 1000) + 86400});
            }
            if (param === reconStatus && payload[param][0] !== 'All') {
                query[key.couchField] = query[key.couchField] || {};
                query[key.couchField] = Object.assign(query[key.couchField], {'$in': payload[param]});
            }
            if (param !== toDate && param !== fromDate) {
                if (payload[param][0] !== 'All') {
                    query[key.couchField] = _.isArray(payload[param]) ? {$in: payload[param]} : {$in: [payload[param]]};
                }
            }
        }
    }
    return query;
};

module.exports = matching;
