"use strict";

const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const schema = new Schema({
   schemaProfileName: {
      type: String,
      require: true,
   },
   schemaProfile: {
      type: Schema.Types.Mixed,
      require: true,
   },
   schema: {
      type: Schema.Types.Mixed,
      require: true,
   }
}, { timestamps: true });

const SchemaProfile = mongoose.model("SchemaProfile", schema, "SchemaProfile");

module.exports = SchemaProfile;
