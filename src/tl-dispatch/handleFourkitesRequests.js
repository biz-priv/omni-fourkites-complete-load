const axios = require('axios');

async function handleFourkitesRequest(url, params, auth) {
    try {
        console.info("Fourkites Api Url :\n", JSON.stringify(url));
        console.info("Fourkites Api Body :\n", JSON.stringify(params));
        let response = await axios.post(url, params, auth);
        console.info("Fourkites Api Response : ",response);
        return true;
    } catch (error) {
        console.error("Fourkites Api Error : \n" + JSON.stringify(error));
        return false;
    }
}

module.exports = { handleFourkitesRequest }