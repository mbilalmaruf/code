'use strict';

const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const schema = new Schema({
  useCase: {
    type: String
  },
  code: {
    type: String
  },
  descriptionAr: {
    type: String
  },
  description: {
    type: String
  }
});


const ErrorCodes = mongoose.model('ErrorCodes', schema, 'ErrorCodes');

module.exports = ErrorCodes;
