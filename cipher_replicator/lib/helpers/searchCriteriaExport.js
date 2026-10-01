'use strict';

const entity = 'entity';
const acquirer = 'Acquirer';

function searchCriteriaExport(filters, decoded) {
    for (const param in decoded) {
        if (decoded[param] === "Entity" || decoded[param] === "entity") {
            filters[decoded[param]] = decoded['orgCode'];
        }
        if (decoded[param] === "Acquirer" || decoded[param] === "acquirer") {
            filters[decoded[param]] = decoded['orgCode'];
        }
    }
    return filters;
}

module.exports = searchCriteriaExport;
