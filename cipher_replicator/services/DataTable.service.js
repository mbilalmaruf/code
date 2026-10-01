const _ = require('lodash');
const moment = require('moment');
const { REVERSE_REPLICATOR_STATUS } = require('../utils/constants');
const rp = require('request-promise');
const inflection = require('inflection');
const { executeQuery, buildQuery } = require('../lib/helpers/dbHelper');

async function handleDataTableExecution(params, conn, dbType) {
    try {
        console.log("handleDataTableExecution", params);

        const documentName = params?.documentName;

        // 1. Count Query
        const countQuery = `SELECT count(*) as count FROM reversereplicatorqueues WHERE "tableName" = {1} AND status = {2}`;
        const countResult = await executeQuery(conn, countQuery, [documentName, REVERSE_REPLICATOR_STATUS.PENDING], dbType);
        const queueCount = parseInt(countResult[0]?.count || 0);
        console.log(documentName, 'queueRecords >>>>', queueCount);

        if (queueCount > 0) {
            const thresholdLimit = params?.thresholdLimit || 1;

            if (queueCount < thresholdLimit) {
                const lastProcessedTime = _.get(global, `lastProcessedTime.${documentName}`);
                if (lastProcessedTime) {
                    const diff = moment().diff(moment(lastProcessedTime), 'seconds');
                    const thresholdDuration = params?.thresholdDuration || 1;
                    if (diff < thresholdDuration) return;
                }
            }

            // 2. Queue Fetch
            const queueFetchQuery = `
                SELECT * FROM reversereplicatorqueues 
                WHERE "tableName" = {1} AND status = {2} 
                ORDER BY id 
                ${dbType === 'mssql' ? `OFFSET 0 ROWS FETCH NEXT {3} ROWS ONLY` : `LIMIT {3}`}
            `;
            const queueRecords = await executeQuery(
                conn,
                queueFetchQuery,
                [documentName, REVERSE_REPLICATOR_STATUS.PENDING, params?.limit],
                dbType
            );

            // 3. Unique filter
            const seen = new Set();
            const filteredRecords = queueRecords.filter(r => {
                if (!seen.has(r.uniqueIdentifierValue)) {
                    seen.add(r.uniqueIdentifierValue);
                    return true;
                }
                return false;
            });

            // 4. Group by smartContractFunction + collection
            const requests = filteredRecords.reduce((acc, r) => {
                const fn = r.smartContractFunction;
                const collection = r.collection || '';
                const key = `${fn}:${collection}`;
                acc[key] = acc[key] || {
                    mapping: {},
                    ids: [],
                    values: [],
                    identifier: r.uniqueIdentifier,
                };
                acc[key].mapping[r.uniqueIdentifierValue] = r.key;
                acc[key].ids.push(r.id);
                acc[key].values.push(`${r.uniqueIdentifierValue.replace(/'/g, "''")}`);
                return acc;
            }, {});

            const tableName = params?.pluralize ? inflection.pluralize(documentName) : documentName;
            const tableAlias = 't';
            const jsonCastingData = _.get(params, 'JSONCasting', []);

            console.log('requests', requests)


            // console.log('requests==>' , requests)

            for (let key in requests) {
                const [fn, collection] = key.split(':');
                const { mapping, ids, values, identifier } = requests[key];

                // 5. Fields
                let fields = _.get(params, `fields[${fn}]`, []).map(f => `${tableAlias}."${f}"`);
                if (!params?.excludeIdentifier) {
                    fields.push(`${tableAlias}."${identifier}"`, `${tableAlias}."key"`);
                }
                // 6. Joins
                const joinDetails = _.get(params, `join[${fn}]`, []);
                let joinClause = '';
                joinDetails.forEach((j, i) => {
                    const joinTable = j.pluralize ? inflection.pluralize(j.table) : j.table;
                    const joinAlias = `j${i}`;
                    joinClause += `${j.type} JOIN "${joinTable}" ${joinAlias} ON ${tableAlias}."${identifier}" = ${joinAlias}."${identifier}" `;
                    fields.push(...j.fields.map(f => `${joinAlias}."${f}"`));
                });

                let fetchQuery = ` SELECT ${fields.join(', ')}  FROM "${tableName}" ${tableAlias}  ${joinClause} WHERE 1=1 `;

                identifier?.split("_")?.map((el, i) => {
                    // console.log('values.join(', ')' , values.join(',').split("_")[i])
                    fetchQuery = fetchQuery + ` AND ${tableAlias}."${el}" IN ('${values.join(',').split("_")[i]}') `
                })

                fetchQuery = fetchQuery + `${params?.whereClause || ''}`

                // console.log('fetch query', fetchQuery)


                const records = await executeQuery(conn, fetchQuery, [], dbType);
                // console.log('records >>>', records);


                // const data = records.map(r => ({
                //     ...r,
                //     documentName,
                //     replicatorID: mapping[r[identifier]],
                //     // uniqueIdentifier: identifier,
                //     // uniqueIdentifierValue: r[identifier],
                // }));

                const data = records.map(record => {
                    // console.log('record identifiers', identifier, r[identifier], records);

                    // const upperCaseRecord = Object.keys(r).reduce((acc, key) => {
                    //     acc[key.toUpperCase()] = r[key];
                    //     return acc;
                    // }, {});

                    // // console.log('upperCaseRecord =>', upperCaseRecord);


                    // let identifierParts = identifier.toUpperCase().split("_");
                    // // console.log('identifierParts =>', identifierParts);

                    // let identifierValues = identifierParts.map(part => {
                    //     return upperCaseRecord[part] ? upperCaseRecord[part] : "";
                    // });

                    // // console.log('identifierValues =>', identifierValues);


                    // const replicatorID = identifierValues.join("_");
                    // // console.log('replicatorID =>', replicatorID);

                    const upperCaseRecord = Object.fromEntries(
                        Object.entries(record).map(([key, value]) => [key.toUpperCase(), value])
                    );

                    // console.log('upperCaseRecord =>', upperCaseRecord);

                    const replicatorID = identifier
                        .toUpperCase()
                        .split("_")
                        .map(part => upperCaseRecord[part] || "")
                        .join("_");

                    const updatedRecord = {
                        ...record,
                        documentName,
                        replicatorID: mapping[replicatorID]
                    };

                    jsonCastingData.forEach(field => {
                        if (updatedRecord[field]) {
                            try {
                                updatedRecord[field] = JSON.parse(updatedRecord[field]);
                            } catch (err) {
                                console.warn(`JSON parse failed for field "${field}":`, err);
                            }
                        }
                    });

                    // console.log('Updated Record:', mapping , replicatorID);
                    console.log('Updated Record:', updatedRecord);

                    return updatedRecord;
                });



                // console.log('data after JSON', data)

                // 7. HTTP request
                const options = {
                    method: 'POST',
                    uri: _.get(global.config, 'reverseReplicationSubmitEndpoint'),
                    headers: {
                        'User-Agent': 'Request-Promise',
                        'Content-Type': 'application/json',
                        Accept: '*/*',
                        'Accept-Encoding': 'gzip, deflate, br',
                        Connection: 'keep-alive',
                    },
                    body: {
                        header: _.get(global.config, 'authentications.avanzaISC', {}),
                        body: {
                            collection: collection,
                            data: data,
                            function: fn,
                        },
                    },
                    json: true,
                };

                const response = await rp(options);
                console.log(documentName, fn, 'response >>>', response);

                // 8. Update status if successful
                if (_.get(response, 'errorCode') == 200) {
                    const updateQuery = `
                        UPDATE reversereplicatorqueues 
                        SET status = {1}, "postedAt" = ${dbType === 'mssql' ? 'getdate()' : "'now()'"} 
                        WHERE id IN (${ids.join(', ')})
                    `;
                    await executeQuery(conn, updateQuery, [REVERSE_REPLICATOR_STATUS.POSTED], dbType);
                }
            }

            // 9. Update lastProcessedTime
            _.set(global, `lastProcessedTime.${documentName}`, moment().format());
        }
    } catch (err) {
        console.error(err);
    }
}

exports.handleDataTableExecution = handleDataTableExecution;
