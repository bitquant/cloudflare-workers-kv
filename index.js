var uuidv4 = require('uuid/v4');

var ACCOUNT_ID = null;
var EMAIL = null;
var API_KEY = null;
var NAMESPACE_ID = null;
var BINDING = null;

var BASE_PATH = 'https://api.cloudflare.com/client/v4/accounts'
var BLOCK_SIZE = 2000000 // KV max size
var BLOCK_REGEX = /^id=[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12};length=[0-9]{1,}$/


async function init(config) {

    BINDING = config.variableBinding;
    NAMESPACE_ID = config.namespaceId;
    ACCOUNT_ID = config.accountId;
    EMAIL = config.email;
    API_KEY = config.apiKey;
}

async function getWithRestApi(key) {

    const response = await fetch(`${BASE_PATH}/${ACCOUNT_ID}/storage/kv/namespaces/${NAMESPACE_ID}/values/${key}`, {
        headers: {
            'X-Auth-Email':  EMAIL,
            'X-Auth-Key': API_KEY
        }
    });

    if (!response.ok) {
        return null;
    }

    return response.text();
}

function parseBlockMeta(value) {
    let blockId = value.slice(3, 39);
    let blockCount = parseInt(value.slice(47), 10);
    return { blockId, blockCount };
}

function getBlockMeta(blockId, blockCount) {
    return `id=${blockId};length=${blockCount}`;
}

function getBlockKey(blockId, blockIndex) {
    return `id=${blockId};index=${blockIndex}`;
}

async function get(key) {

    let value = await getKV(key);

    if (value === null || value.search(BLOCK_REGEX) === -1) {
        return value;
    }

    let { blockId, blockCount } = parseBlockMeta(value);
    let promiseList = [];

    for (let blockIndex = 0; blockIndex < blockCount; blockIndex++) {
        let blockKey = getBlockKey(blockId, blockIndex);
        let blockPromise = getKV(blockKey, 'arrayBuffer')
        promiseList.push(blockPromise)
    }

    let blockList = await Promise.all(promiseList);
    let finalValue = '';
    let byteArraySize = 0;

    for (let blockData of blockList) {
        if (blockData === null) {
            let err = new Error(`key '${key}' has missing data blocks and needs deletion`);
            err.blockRecord = value;
            throw err;
        }
        if (blockData instanceof ArrayBuffer) {
            byteArraySize += blockData.byteLength;
        }
        else {
            finalValue += blockData;
        }
    }

    if (byteArraySize > 0) {
        let resultArray = new Uint8Array(byteArraySize);
        let offset = 0;
        for (let blockData of blockList) {
            resultArray.set(new Uint8Array(blockData), offset);
            offset += blockData.byteLength;
        }
        let decoder = new TextDecoder();
        finalValue = decoder.decode(resultArray);
    }

    return finalValue;
}

async function putWithRestApi(key, value) {

    const response = await fetch(`${BASE_PATH}/${ACCOUNT_ID}/storage/kv/namespaces/${NAMESPACE_ID}/values/${key}`, {
        headers: {
            'X-Auth-Email': EMAIL,
            'X-Auth-Key': API_KEY
        },
        method: 'PUT',
        body: value
    });

    if (!response.ok) {
        throw new Error(`${NAMESPACE_ID}:${key} not set to ${value} status: ${response.status}`);
    }

    let body = await response.json();

    if (body.success !== true) {
        throw new Error(`${NAMESPACE_ID}:${key} not set to ${value} success: ${body.success}`);
    }

    return undefined;
}

async function put(key, value) {

    let oldValue = await getKV(key);
    let oldBlock = undefined;

    if (oldValue !== null && oldValue.search(BLOCK_REGEX) === 0) {
        oldBlock = oldValue;
    }

    let encoder = new TextEncoder();
    let encoded = encoder.encode(value);

    if (encoded.length <= BLOCK_SIZE) {
        await putKV(key, value);
        return oldBlock;
    }

    let blockList  = [];
    let blocks = Math.floor(encoded.length / BLOCK_SIZE);
    let lastBlock = (encoded.length % BLOCK_SIZE) > 0 ? 1 : 0;
    let totalBlocks = blocks + lastBlock;

    for (let i = 0; i < totalBlocks; i++) {
        let startIndex = i * BLOCK_SIZE;
        let endIndex = startIndex + BLOCK_SIZE;
        let block = encoded.slice(startIndex, endIndex);
        blockList.push(block);
    }

    let blockId = uuidv4();
    let blockIndex = 0;

    for (let block of blockList) {
        let blockKey = getBlockKey(blockId, blockIndex);
        try {
            await putKV(blockKey, block.buffer);
        }
        catch (ex) {
            throw new Error(`${blockKey} block put error: ${ex.message}`)
        }
        blockIndex++;
    }

    let blockMeta = getBlockMeta(blockId, blockList.length)
    await putKV(key, blockMeta);

    return oldBlock;
}

async function delWithRestApi(key) {

    const response = await fetch(`${BASE_PATH}/${ACCOUNT_ID}/storage/kv/namespaces/${NAMESPACE_ID}/values/${key}`, {
        headers: {
            'X-Auth-Email': EMAIL,
            'X-Auth-Key': API_KEY
        },
        method: 'DELETE'
    });

    // Check if key not found
    if (response.status === 404) {
        return false; // key does not exist
    }

    if (!response.ok) {
        throw new Error(`${NAMESPACE_ID}:${key} not deleted, status: ${response.status}`);
    }

    let body = await response.json();

    if (body.success !== true) {
        throw new Error(`${NAMESPACE_ID}:${key} not deleted, success: ${body.success}`);
    }

    return true; // key deleted
}

async function delWithNameSpace(key) {

    try {
        await self[BINDING].delete(key);
        return true; // key deleted
    }
    catch (ex) {
        if (ex.message.includes('404')) {
            return false; // key does not exist
        }
        throw ex;
    }
}

async function del(key) {

    let value = await getKV(key);

    if (value === null) {
        return false;
    }

    if (value.search(BLOCK_REGEX) === -1) {
        return delKV(key);
    }

    let { blockId, blockCount } = parseBlockMeta(value);

    for (let blockIndex = 0; blockIndex < blockCount; blockIndex++) {
        let blockKey = getBlockKey(blockId, blockIndex);
        await delKV(blockKey);
    }

    return delKV(key);
}

async function clean(block) {

    let { blockId, blockCount } = parseBlockMeta(block);

    for (let blockIndex = 0; blockIndex < blockCount; blockIndex++) {
        let blockKey = getBlockKey(blockId, blockIndex);
        await delKV(blockKey);
    }
}

async function putMultiWithRestApi(keyValuePairs) {

    const response = await fetch(`${BASE_PATH}/${ACCOUNT_ID}/storage/kv/namespaces/${NAMESPACE_ID}/bulk`, {
        headers: {
            'X-Auth-Email': EMAIL,
            'X-Auth-Key': API_KEY,
            'Content-Type': 'application/json'
        },
        method: 'PUT',
        body: JSON.stringify(keyValuePairs)
    });

    if (!response.ok) {
        throw new Error(`${NAMESPACE_ID}:multiple keys not set status: ${response.status}`);
    }

    let body = await response.json();

    if (body.success !== true) {
        throw new Error(`${NAMESPACE_ID}:multiple keys not set success: ${body.success}`);
    }

    return undefined;
}

async function putMulti(keyValuePairs) {

    return putMultiKV(keyValuePairs);
}

var getKV = (typeof self === 'undefined') ? getWithRestApi : (key, type) => self[BINDING].get(key, type);
var putKV = (typeof self === 'undefined') ? putWithRestApi : (key, value) => self[BINDING].put(key, value);
var delKV = (typeof self === 'undefined') ? delWithRestApi : delWithNameSpace;
var putMultiKV = putMultiWithRestApi;


exports.init = init;
exports.get = get;
exports.put = put;
exports.del = del;
exports.clean = clean;
exports.putMulti = putMulti;
