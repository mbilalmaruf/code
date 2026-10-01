'use strict';

const mongoose = require('mongoose');
const Schema = mongoose.Schema;
const schema = new Schema({
    graph: {
        type: Schema.Types.Mixed,
        required: true
    },
    userID: {
        type: String
    },
    makerGroup: {
        type: String
    },
    transitionDescription:{
        type: String,
        required: true
    },
    transitionName:{
        type: String,
        required: true
    },
    syncFlag: {
        type: Boolean,
        default: false,
        required: true
    }
});

const graph = mongoose.model('graph', schema, 'graph');

module.exports = graph;