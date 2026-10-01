"use strict";

const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const schema = new Schema(
   {
      name: {
         type: String,
         required: true,
         // unique: true,
      },
      description: {
         type: String,
         required: true,
      },
      reportType: {
         type: String,
         required: true,
      },
      queryCount: {
         type: Number,
      },
      queryStrValues: [
         {
            type: String,
         },
      ],
      queryStrLabel: {
         type: String,
      },
      connectionType: {
         type: String,
         required: true,
      },
      scheduleTime: {
         type: String,
      },
      scheduleTimeDisplay: {
         type: String,
      },
      email: {
         type: String,
      },
      isScheduled: {
         type: Boolean,
      },
      letterTemplate: {
         type: String,
      },
      connectionString: {
         type: Schema.Types.ObjectId,
         ref: "EndpointDefination",
      },
      group: [
         {
            type: Schema.Types.ObjectId,
            ref: "Group",
         },
      ],
      queryStr: {
         type: String,
         required: true,
      },
      mappingRoute: {
         type: String,
      },
      filters: [
         {
            type: Schema.Types.Mixed,
            required: true,
         },
      ],
      isSynced: { type: Boolean },
      createdAt: {
         type: Date,
         default: Date.now,
         index: true,
      },
      createdBy: {
         type: Schema.Types.ObjectId,
         ref: "User",
      },
      createdID: {
         type: String,
      },
      updatedAt: {
         type: Date,
         default: Date.now,
      },
      updatedBy: {
        type: Schema.Types.ObjectId, ref: 'User'
      }
   },
   { timestamps: true }
);

const ADHReport = mongoose.model("ADHReport", schema, "ADHReport");

module.exports = ADHReport;
