'use strict';

const mongoose = require('mongoose');
const Schema = mongoose.Schema;
const _ = require('lodash');
let schema = {
  firstName: {
    type: String,
  },
  network: {
    type: String
  },
  clientSecret: {
    type: String
  },
  lastName: {
    type: String
  },
  mobileNumber: {
    type: String
  },
  verificationCode: {
    type: String
  },
  quorrumUser: {
    type: String
  },
  hypUser: {
    type: String
  },
  network: {
    type: String
  },
  email: {
    type: String,
  },
  addAffiliation: {
    type: String
  },
  userID: {
    type: String,
  },
  stampImageBase64: {
    type: String,
  
  },
  certificatePass: {
    type: String,
  
  },
  certificateBase64: {
    type: String,
  
  },
  lastResetTime: {
    type: Number,
    // default: dates.now
  },
  orgType: {
    type: String,
  },
  orgCode: {
    type: String,
  },
  userSubType: {
    type: String
  },
  department: {
    type: String
  },
  unit: {
    type: String
  },
  userType: {
    type: String,
  },
  isActive: {
    type: Boolean,
    default: false
  },
  allowedIPRange: [String],
  passwordPolicy: {
    type: Schema.Types.ObjectId,
    ref: 'PasswordPolicy'
  },
  passwordRetries: {
    type: Number,
    default: 0
  },
  password: {
    type: String
    // required: msgConst.user.password
  },
  passwordHashType: {
    type: String
    // required: msgConst.user.password
  },
  passwordUpdatedAt: {
    type: Number,
    // default: dates.newDate
  },
  lastLoginTime: {
    type: String
  },
  passwordReset: {
    type: String
  },
  nationality: {
    type: String
  },
  countryOfResidence: {
    type: String
  },
  documentType: {
    type: String
  },
  documentNo: {
    type: String
  },
  userList: [{
    type: Schema.Types.ObjectId,
    ref: 'User'
  }],
  profilePic: {
    type: String
  },
  coverPic: {
    type: String
  },
  bio: {
    type: String
  },
  linkdinLink:{
    type:String
  },
  facebookLink:{
    type:String
  },
  DOB:{
    type:Date
  },
  designation:{
    type:String
  },
  firstScreen: {
    type: String,
    default: "/hyperledger/workboard",
  },
  entityID: {
    type: Schema.Types.ObjectId,
    ref: 'Entity'
  },
  endpoint: {
    type: String
  },
  OTPVerified: {
    type: String,
    default: "Not Verified"
  },
  acquirerID: {
    type: Schema.Types.ObjectId,
    ref: 'Acquirer'
  },
  settlementID: {
    type: String // TODO add reference to settlement collection when model created
  },
  isNewUser: {
    type: Boolean
  },
  authType: {
    type: String,
  },
  passwordRetryAt: {
    type: Number
  },
  createdAt: {
    type: Number,
    // default: dates.newDate
  },
  createdBy: {
    type: Schema.Types.ObjectId,
    ref: 'User'
  },
  updatedAt: {
    type: Number,
    // default: dates.newDate
  },

  updatedBy: {
    type: Schema.Types.ObjectId,
    ref: 'User'
  },
  status: {
    type: String
  },
  groups: [{
    type: Schema.Types.ObjectId,
    ref: 'Group'
  }],
  services: [Schema.Types.Mixed],
  group: {
    type: [String],
  },
  documents: [{
    documentType: {
      type: String
    },
    documentNo: {
      type: String
    },
    documentExpiry: {
      type: String
    },
    hash: {
      type: String
    },
    status: {
      type: String,
      default: "Pending"
    },
  }],
  pvtKeyAccount: {
    type: String
  },
  documentExpiry: {
    type: String
  },
  buisnessCategory: {
    type: String
  },
  kycStatus: {
    type: String,
    default: 'Pending'
  },
  userGender: {
    type: String
  },
  userLanguage: {
    type: String,
    default: 'EN'
  },
  rejectReason:{
    type:String,
    default:""
  },
  isFresh:{
    type:Boolean,
    default:false
  },
  socialLinks: [{
    name: {type: String, default: ""},
    link: {type: String, default: ""}
  }],
  userId: {
    type: String
  }

};

const User = mongoose.model('User', schema, 'User');

module.exports = User;
