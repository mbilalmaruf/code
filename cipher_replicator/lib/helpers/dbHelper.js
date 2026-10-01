function getParamPlaceholder(index, dbType) {
    if (dbType === 'postgres') return `$${index}`;
    if (dbType === 'mssql') return `@param${index}`;
    throw new Error('Unsupported DB type');
}

function buildQuery(template, dbType) {
    return template.replace(/\{(\d+)\}/g, (_, index) => getParamPlaceholder(index, dbType));
}

async function executeQuery(conn, queryTemplate, params, dbType) {
    const query = buildQuery(queryTemplate, dbType);

    if (dbType === 'pg' && typeof conn.query === 'function') {
        const result = await conn.query(query, params);
        return dbType === 'mssql' ? result.recordset : result.rows;
    }

    if (dbType === 'mssql' && typeof conn.request === 'function') {
        let request = conn.request();
        params.forEach((val, i) => {
            request.input(`param${i + 1}`, val); // bind @param1, @param2, etc.
        });

        const result = await request.query(query);
        return result.recordset;
    }

    throw new Error('Unsupported connection object');
}

module.exports = {
    getParamPlaceholder,
    buildQuery,
    executeQuery,
};
