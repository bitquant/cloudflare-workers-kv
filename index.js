var ACCOUNT_ID = null;
var EMAIL = null;
var API_KEY = null;
var NAMESPACE = null;

var BASE_PATH = 'https://api.cloudflare.com/client/v4/accounts'
var CHUNK_SIZE = 64000 // KV max size
var CHUNK_LABEL = '___CHUNK___'
var META_LABEL = '___META___'


async function init(namespace, account, email, apiKey) {

    // NAMESPACE is the variable binding name when running in Cloudflare
    // NAMESPACE is the namespace ID when running locally

    NAMESPACE = namespace;
    ACCOUNT_ID = account;
    EMAIL = email;
    API_KEY = apiKey;
}

async function getWithRestApi(key) {

    const response = await fetch(`${BASE_PATH}/${ACCOUNT_ID}/storage/kv/namespaces/${NAMESPACE}/values/${key}`, {
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

async function get(key) {

    let value = await getKV(key);

    if (value === null || !value.startsWith(META_LABEL)) {
        return value;
    }

    let chunks = parseInt(value.slice(META_LABEL.length), 10);
    let promiseList = [];

    for (let i = 0; i < chunks; i++) {
        let chunkKey = `${key}${CHUNK_LABEL}${i}`;
        let chunkPromise = getKV(chunkKey, 'arrayBuffer')
        promiseList.push(chunkPromise)
    }

    let chunkList = await Promise.all(promiseList);
    let finalValue = '';
    let byteArraySize = 0;

    for (let chunkData of chunkList) {
        if (chunkData instanceof ArrayBuffer) {
            byteArraySize += chunkData.byteLength;
        }
        else {
            finalValue += chunkData;
        }
    }

    if (byteArraySize > 0) {
        let resultArray = new Uint8Array(byteArraySize);
        let offset = 0;
        for (let chunkData of chunkList) {
            resultArray.set(new Uint8Array(chunkData), offset);
            offset += chunkData.byteLength;
        }
        let decoder = new TextDecoder();
        finalValue = decoder.decode(resultArray);
    }

    return finalValue;
}

async function putWithRestApi(key, value) {

    const response = await fetch(`${BASE_PATH}/${ACCOUNT_ID}/storage/kv/namespaces/${NAMESPACE}/values/${key}`, {
        headers: {
            'X-Auth-Email': EMAIL,
            'X-Auth-Key': API_KEY
        },
        method: 'PUT',
        body: value
    });

    if (!response.ok) {
        throw new Error(`${NAMESPACE}:${key} not set to ${value} status: ${response.status}`);
    }

    let body = await response.json();

    if (body.success !== true) {
        throw new Error(`${NAMESPACE}:${key} not set to ${value} success: ${body.success}`);
    }

    return undefined;
}

async function put(key, value) {

    let encoder = new TextEncoder();
    let encoded = encoder.encode(value);

    if (encoded.length <= CHUNK_SIZE) {
        return putKV(key, value);
    }

    let chunkList  = [];
    let chunks = Math.floor(encoded.length / CHUNK_SIZE);
    let lastChunk = (encoded.length % CHUNK_SIZE) > 0 ? 1 : 0;
    let totalChunks = chunks + lastChunk;

    for (let i = 0; i < totalChunks; i++) {
        let chunkStartIndex = i * CHUNK_SIZE;
        let chunkEndIndex = chunkStartIndex + CHUNK_SIZE;
        let chunk = encoded.slice(chunkStartIndex, chunkEndIndex);
        chunkList.push(chunk);
    }

    let metaValue = `${META_LABEL}${chunkList.length}`;
    await putKV(key, metaValue);

    let chunkId = 0;
    for (let chunk of chunkList) {
        let chunkKey = `${key}${CHUNK_LABEL}${chunkId}`;
        await putKV(chunkKey, chunk.buffer);
        chunkId++;
    }

    return undefined;
}

async function delWithRestApi(key) {

    const response = await fetch(`${BASE_PATH}/${ACCOUNT_ID}/storage/kv/namespaces/${NAMESPACE}/values/${key}`, {
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
        throw new Error(`${NAMESPACE}:${key} not deleted, status: ${response.status}`);
    }

    let body = await response.json();

    if (body.success !== true) {
        throw new Error(`${NAMESPACE}:${key} not deleted, success: ${body.success}`);
    }

    return true; // key deleted
}

async function delWithNameSpace(key) {

    try {
        await self[NAMESPACE].delete(key);
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

    if (!value.startsWith(META_LABEL)) {
        return delKV(key);
    }

    let chunks = parseInt(value.slice(META_LABEL.length), 10);

    for (let i = 0; i < chunks; i++) {
        let chunkKey = `${key}${CHUNK_LABEL}${i}`;
        await delKV(chunkKey);
    }

    return delKV(key);
}

var getKV;
var putKV;
var delKV;

if (typeof self === 'undefined') {
    getKV = getWithRestApi;
    putKV = putWithRestApi;
    delKV = delWithRestApi;
}
else {
    getKV = (key, type) => self[NAMESPACE].get(key, type);
    putKV = (key, value) => self[NAMESPACE].put(key, value);
    delKV = delWithNameSpace;
}

exports.init = init;
exports.get = get;
exports.put = put;
exports.del = del;
