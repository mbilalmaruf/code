const axios = require('axios'); // Correct import for axios
const config = require('../../config/config.json');

async function get(callback) {
  const options = {
    timeout: 2000,
    headers: { 'Content-Type': 'application/json' }
  };

  try {
    const resp = await axios.post(config.keyVault.url, {
      env: config.keyVault.env,
      header: config.keyVault.header
    }, options);

    return { err: null, body: resp.data };
  } catch (error) {
    console.error('Error fetching data:', error.message);
    return { err: error.message, body: null };
  }
}

module.exports = {
  get: get
};