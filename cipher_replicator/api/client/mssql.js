const sql = require('mssql');

async function getPool(name, config) {
    const pool = new sql.ConnectionPool(JSON.parse(config))
    const close = pool.close.bind(pool)
    pool.close = (...args) => {
        return close(...args)
    }
    console.debug('creating pool');
    return await pool.connect()
}


// close all pools
function closeAll() {
    return Promise.all(Object.values(pools).map((pool) => {
        return pool.close()
    }))
}

module.exports = {
    closeAll,
    getPool
}