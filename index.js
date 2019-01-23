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

    if (response.ok) {
        return response.text();
    }

    return null;
}

async function get(key) {

    let value = (typeof self === 'undefined') ?
        await getWithRestApi(key) : await self[NAMESPACE].get(key);

    if (value === null || !value.startsWith(META_LABEL)) {
        return value;
    }

    let chunks = parseInt(value.slice(META_LABEL.length), 10);
    let promiseList = [];

    for (let i = 0; i < chunks; i++) {
        let chunkKey = `${key}${CHUNK_LABEL}${i}`;
        let promise = (typeof self === 'undefined') ?
            getWithRestApi(chunkKey) : self[NAMESPACE].get(chunkKey, 'arrayBuffer');
        promiseList.push(promise)
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
        let result = (typeof self === 'undefined') ?
            putWithRestApi(key, value) : self[NAMESPACE].put(key, value);
        return result;
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
    let promiseList = [];
    let chunkId = 0;

    let keyPromise = (typeof self === 'undefined') ?
        putWithRestApi(key, metaValue) : self[NAMESPACE].put(key, metaValue);
    promiseList.push(keyPromise);

    for (let chunk of chunkList) {
        let chunkKey = `${key}${CHUNK_LABEL}${chunkId}`;
        let result = (typeof self === 'undefined')
            ? putWithRestApi(chunkKey, chunk.buffer)
            : self[NAMESPACE].put(chunkKey, chunk.buffer);
        promiseList.push(result);
        chunkId++;
    }

    return Promise.all(promiseList).then(() => undefined);
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
        return undefined;
    }

    if (!response.ok) {
        throw new Error(`${NAMESPACE}:${key} not deleted, status: ${response.status}`);
    }

    let body = await response.json();

    if (body.success !== true) {
        throw new Error(`${NAMESPACE}:${key} not deleted, success: ${body.success}`);
    }

    return undefined;
}

async function del(key) {

    let value = (typeof self === 'undefined') ?
        await getWithRestApi(key) : await self[NAMESPACE].get(key);

    if (value === null || !value.startsWith(META_LABEL)) {
        if (typeof self === 'undefined') {
            return delWithRestApi(key)
        }
        return self[NAMESPACE].delete(key);
    }

    let chunks = parseInt(value.slice(META_LABEL.length), 10);
    let promiseList = [];

    for (let i = 0; i < chunks; i++) {
        let chunkKey = `${key}${CHUNK_LABEL}${i}`;
        let promise = (typeof self === 'undefined') ?
            delWithRestApi(chunkKey) : self[NAMESPACE].delete(chunkKey);
        promiseList.push(promise)
    }

    let chunkList = await Promise.all(promiseList);

    if (typeof self === 'undefined') {
        return delWithRestApi(key)
    }
    return self[NAMESPACE].delete(key);
}

exports.init = init;
exports.get = get;
exports.put = put;
exports.del = del;
