'use strict';

const entity = 'Entity';
const acquirer = 'Acquirer';

function searchCriteriaOrgType(filters, decoded) {
  for (const param in decoded) {
    if (decoded[param] === entity) {
      filters[decoded[param]] = [];
      filters[decoded[param]].push(decoded['orgCode']);
    }
    if (decoded[param] === acquirer) {
      filters[decoded[param]] = [];
      filters[decoded[param]].push(decoded['orgCode']);
    }
  }
  return filters;
}

module.exports = searchCriteriaOrgType;
