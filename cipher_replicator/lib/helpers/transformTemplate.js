const model = require('../models/APITemplates');
//const Handlebars = require('handlebars');
const promisedHandlebars = require('promised-handlebars');
const Handlebars = promisedHandlebars(require('handlebars'), { Promise: global.Promise });

const crypto = require('./crypto')
const dates = require('./dates')
/**
 * Allows to bind payload data to templates
 * @param {String} id : _id of the API template mongo record
 * @param {JSON} payload : The payload to be passed for binding
 * @returns {JSON} {err: Error, data: ParsedData}
 */

Handlebars.registerHelper('EpochToHuman', function(value){
  value = parseInt(value)||0;
  value *= 1000;
  return dates.MSddMMyyyy(value);
});

Handlebars.registerHelper('encrypt', function(value){
  return crypto.encrypt(value)
});

async function transformTemplate(template, payload, customFunction) {
  let data= {};
  if(customFunction!==undefined){
    customFunction.map((item)=>{
      Handlebars.registerHelper(item.name, item.function);
    })
  }
  try {

    let compiledTemplate = Handlebars.compile(JSON.stringify(template));
    data = await compiledTemplate(payload);
  } catch (err) {
    console.error('Conversion error', err);
  }

  try {
    data = JSON.parse(data);
  } catch(err){
    //console.log('JSON conversion err', err);
  }
  return data;
}

module.exports = transformTemplate;