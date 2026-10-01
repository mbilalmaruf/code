'use strict';

const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const schema = new Schema({
  taxNO1: {
    type: String
  },
  taxNO2: {
    type: String
  },
  taxAddress: {
    type: String
  },
  publicKey: {
    type: String
  },
  shortCode: {
    type: String
  },
  newLoginPolicy: {
    type: String
  },
  businessCategory: {
    type: Array
  },
  parentEntity: {
    type: String
  },
  clientKey: {
    type: String
  },
  clientSecret: {
    type: String
  },
  cycle: {
    type: String
  },
  currency: {
    type: String
  },
  commissionTemplate: {
    type: String
  },
  mappedCodes: [{
    mappingType: {
      type: String
    },
    mappingCode: {
      type: String
    }
  }],
  additionalProps: [{
    property: {
      type: String
    },
    value: {
      type: String
    }
  }],
  documents: [{
    documentName: {
      type: String
    },
    fileType: {
      type: String
    },
    retrievalPath: {
      type: String
    },
    documentHash: {
      type: String
    }
  }],
  status: {
    value: {
      type: String
    },
    type: {
      type: String
    }
  },
  actions: {
    type: Array
  },
  dateCreated: {
    type: Date
  },
  createdBy: {
    type: String
  },
  dateUpdated: {
    type: Date
  },
  updatedBy: {
    type: String
  },
  lastReconDate: {
    type: String
  },
  entityName: {
    type: String,
    require: true
  },
  arabicName: {
    type: String
  },
  spCode: {
    type: String
  },
  orgType: {
    type: String
  },
  newUserPolicy: {
    type: String
  },
  membershipNo: {
    prefix: {
      type: String
    },
    trailingZeroes: {
      type: String
    }
  },
  address: {
    line1: {
      type: String
    },
    line2: {
      type: String
    },
    line3: {
      type: String
    },
    line4: {
      type: String
    },
    latitude: {
      type: String
    },
    longitude: {
      type: String
    }
  },
  uuid: {
    type: String
  },
  isPartOfGeneralChannel: {
    type: Boolean
  },
  callbackEP: {
    type: String
  },
  isActive: {
    type: Boolean,
    default: false
  },
  isMember: {
    type: Boolean
  },
  isGenratedMembership: {
    type: Boolean
  },
  isConsolidate: {
    type: Boolean
  },
  cutOf: {
    type: Boolean
  },
  emailOTP: {
    type: Boolean,
    default: false
  },
  smsOTP: {
    type: Boolean,
    default: false
  },
  emailTemplate: {
    type: Boolean,
    default: false
  },
  smsTemplate: {
    type: Boolean,
    default: false
  },
  emailOTPLogin: {
    type: Boolean,
    default: false
  },
  smsOTPLogin: {
    type: Boolean,
    default: false
  },
  login: {
    type: Boolean,
    default: false
  },
  smsTemplateLogin: {
    type: Boolean,
    default: false
  },
  dailyRequestQuotaLimit: {
    type: Number,
    default: 0
  },
  monthlyRequestQuotaLimit: {
    type: Number,
    default: 0
  },
  departments: [{
    departmentName: String,
    departmentCode: String,
    departmentAddress: String,
    departmentLongitude: String,
    departmentLatitude: String,
    units: [{
      unitName: String,
      unitCode: String
    }],
  }],
  entityLogo: {
    sizeSmall: {
      type: String
    },
    sizeMedium: {
      type: String
    }
  },
  contacts: [
    {
      contactName: {
        type: String
      },
      email: {
        type: String
      },
      mobile: {
        type: String
      },
      displayMenu: {
        type: Boolean
      },
      emailTemplate: {
        type: String
      }
    }
  ]
});

// schema.index({ useCase: 1, route: 1 }, { unique: true })
const Entity = mongoose.model('Entity', schema, 'Entity');

module.exports = Entity;
