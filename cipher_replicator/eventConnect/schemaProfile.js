'use strict';

const _ = require('lodash');
const config = require('../lib/config/index');
const crypto = require('../lib/helpers/crypto');

require('mongoose');
require('../api/client/mongoose')(crypto.decrypt(config.get('mongodb')));
const SchemaProfile = require('./models/SchemaProfile')
const get = async (profileName) => {
      return await SchemaProfile.findOne({schemaProfileName: profileName}).lean(true);
}

exports.get = get;