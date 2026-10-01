"use strict";

const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const schema = new Schema({
   templateId: {
      type: String,
      require: true,
   },
   templateName: {
      type: String,
      require: true,
   },
   templateMarkup: {
      type: String,
      require: false,
   },
   templatePath: {
      type: String,
      require: false,
   },
   outputFileName: {
      type: String,
      require: false,
   },
   filePath: {
      type: String,
      require: false,
   },
   templateType: {
      type: String,
      require: false,
   },
   sampleJson: {
      type: String,
      require:  false,
   },
   socialMediaLinks: {
      type: String,
      require: false,
   },
   generalSectionData: {
      type: String,
      require: false,
   },
   reportLocation: {
      type: String,
      require: false,
   },
});

const Letters = mongoose.model("Letter", schema, "Letter");

module.exports = Letters;
