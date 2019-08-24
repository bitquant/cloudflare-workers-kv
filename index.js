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

async function getWithRestApi(key, type) {

    const response = await fetch(`${BASE_PATH}/${ACCOUNT_ID}/storage/kv/namespaces/${NAMESPACE_ID}/values/${key}`, {
        headers: {
            'X-Auth-Email':  EMAIL,
            'X-Auth-Key': API_KEY
        }
    });

    if (!response.ok) {
        return null;
    }

    if (type === 'text' || type === undefined) {
        return response.text();
    }
    else if (type === 'json') {
        return response.json();
    }
    else if (type === 'arrayBuffer') {
        return response.arrayBuffer();
    }
    else if (type === 'stream') {
        return response.body;
    }
    else {
        throw new Error(`error getting value for key ${key}, unsupported type: ${type}`)
    }
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

async function get(key, type) {

    let arrayBuffer = await getKV(key, 'arrayBuffer');

    if (arrayBuffer === null) {
        return null;
    }

    let stringValue = new TextDecoder().decode(arrayBuffer);

    if (stringValue.search(BLOCK_REGEX) === -1) {

        if (type === 'text' || type === undefined) {
            return stringValue;
        }
        if (type === 'json') {
            return JSON.parse(stringValue)
        }
        if (type === 'arrayBuffer') {
            return arrayBuffer;
        }
        if (type === 'stream') {
            return new Response(arrayBuffer).body;
        }

        throw new Error(`error getting value for key ${key}, unsupported type: ${type}`)
    }

    let { blockId, blockCount } = parseBlockMeta(stringValue);
    let promiseList = [];

    for (let blockIndex = 0; blockIndex < blockCount; blockIndex++) {
        let blockKey = getBlockKey(blockId, blockIndex);
        let blockPromise = getKV(blockKey, 'arrayBuffer')
        promiseList.push(blockPromise)
    }

    let blockList = await Promise.all(promiseList);
    let byteArraySize = 0;

    for (let blockData of blockList) {
        if (blockData === null) {
            let err = new Error(`key '${key}' has missing data blocks and needs deletion`);
            err.blockRecord = stringValue;
            throw err;
        }
        byteArraySize += blockData.byteLength;
    }

    let resultArray = new Uint8Array(byteArraySize);
    let offset = 0;
    for (let blockData of blockList) {
        resultArray.set(new Uint8Array(blockData), offset);
        offset += blockData.byteLength;
    }

    let finalValue;

    if (type === 'text' || type === undefined || type === 'json') {
        finalValue = new TextDecoder().decode(resultArray);
        if (type === 'json') {
            finalValue = JSON.parse(finalValue)
        }
    }
    else if (type === 'arrayBuffer') {
        finalValue = resultArray.buffer;
    }
    else if (type === 'stream') {
        finalValue = new Response(resultArray.buffer).body;
    }
    else {
        throw new Error(`error getting large value for key ${key}, unsupported type: ${type}`)
    }

    return finalValue;
}

async function putWithRestApi(key, value, params) {

    let query = '';

    if (params !== undefined) {
        if (params.expiration !== undefined) {
            query += `?expiration=${params.expiration}`
        }
        if (params.expirationTtl !== undefined) {
            query += `${query.length > 0 ? '&' : '?'}expiration_ttl=${params.expirationTtl}`
        }
    }

    const response = await fetch(`${BASE_PATH}/${ACCOUNT_ID}/storage/kv/namespaces/${NAMESPACE_ID}/values/${key}${query}`, {
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

async function put(key, value, params) {

    let oldValue = await getKV(key, 'text');
    let oldBlock = undefined;

    if (oldValue !== null && oldValue.search(BLOCK_REGEX) === 0) {
        oldBlock = oldValue;
    }

    let encoded = null;

    if (typeof value === 'string') {
        encoded = new TextEncoder().encode(value);
    }
    else if (value instanceof ArrayBuffer) {
        encoded = new Uint8Array(value)
    }
    else if (value.buffer instanceof ArrayBuffer) { // ArrayBufferView
        encoded = new Uint8Array(value.buffer)
    }
    else {
        encoded = new Uint8Array(await new Response(value).arrayBuffer())
    }

    if (encoded.length <= BLOCK_SIZE) {
        await putKV(key, encoded, params);
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

    if (params !== undefined) {
        // Blocks need to expire after the header block expires so +600s
        var blockParams = Object.assign({}, params);
        if (params.expiration !== undefined) {
            blockParams.expiration += 600;
        }
        if (params.expirationTtl !== undefined) {
            blockParams.expirationTtl += 600;
        }
    }

    for (let block of blockList) {
        let blockKey = getBlockKey(blockId, blockIndex);
        try {
            await putKV(blockKey, block.buffer, blockParams);
        }
        catch (ex) {
            throw new Error(`${blockKey} block put error: ${ex.message}`)
        }
        blockIndex++;
    }

    let blockMeta = getBlockMeta(blockId, blockList.length)
    await putKV(key, blockMeta, params);

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

    keyValuePairs.map(function(kv) {
        if (kv.expirationTtl !== undefined) {
            kv.expiration_ttl = kv.expirationTtl;
            delete kv.expirationTtl;
        }
        return kv;
    })

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

async function listWithRestApi(params) {

    let query = '';

    if (params !== undefined) {

        if (params.cursor !== undefined) {
            query += `?cursor=${params.cursor}`
        }

        if (params.prefix !== undefined) {
            query += `${query.length > 0 ? '&' : '?'}prefix=${params.prefix}`
        }

        if (params.limit !== undefined) {
            query += `${query.length > 0 ? '&' : '?'}limit=${params.limit}`
        }
    }

    const response = await fetch(`${BASE_PATH}/${ACCOUNT_ID}/storage/kv/namespaces/${NAMESPACE_ID}/keys${query}`, {
        headers: {
            'X-Auth-Email': EMAIL,
            'X-Auth-Key': API_KEY
        }
    });

    if (!response.ok) {
        throw new Error(`${NAMESPACE_ID}: unable to list keys status: ${response.status}`);
    }

    let body = await response.json();

    if (body.success !== true) {
        throw new Error(`${NAMESPACE_ID}: list keys not successful`);
    }

    return {
        count: body.result_info.count,
        cursor: body.result_info.cursor,
        keys: body.result
    };
}

async function list(params) {

    return listKV(params)
}

var getKV = (typeof self === 'undefined') ? getWithRestApi : (key, type) => self[BINDING].get(key, type);
var putKV = (typeof self === 'undefined') ? putWithRestApi : (key, value, params) => self[BINDING].put(key, value, params);
var delKV = (typeof self === 'undefined') ? delWithRestApi : delWithNameSpace;
var putMultiKV = putMultiWithRestApi;
var listKV = listWithRestApi;


exports.init = init;
exports.get = get;
exports.put = put;
exports.del = del;
exports.clean = clean;
exports.putMulti = putMulti;
exports.list = list;
