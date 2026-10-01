'use strict';

const _ = require('lodash');

function criteriaLength(criteria) {
  for (const param in criteria) {
    if (_.isArray(criteria[param])) {
      if (criteria[param][0] === 'All') {
        criteria[param] = 'All';
      }
      if (criteria[param].length > 5) {
        criteria[param] = criteria[param].slice(1, 5) + ' ...';
      }
    }
  }
  return criteria;
};

module.exports = criteriaLength;
