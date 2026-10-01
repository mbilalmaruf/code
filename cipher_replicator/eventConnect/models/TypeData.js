"use strict";

const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const schema = new Schema({
   typeName: {
      type: String,
      require: true,
   },
   data: {
      type: Schema.Types.Mixed,
      require: true,
   },
   isForign: { type: Boolean },

   type: {
      type: String,
   },
   syncFlag: {
      type: Boolean,
      default: false,
      required: true
   }
   
}, { timestamps: true });

const TypeData = mongoose.model("TypeData", schema, "TypeData");

module.exports = TypeData;
