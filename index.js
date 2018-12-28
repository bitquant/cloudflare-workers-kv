var ACCOUNT_ID = null;
var EMAIL = null;
var API_KEY = null;
var NAMESPACE = null;

var BASE_PATH = 'https://api.cloudflare.com/client/v4/accounts'


async function init(namespace, account, email, apiKey) {

    // NAMESPACE is the variable binding name when running in Cloudflare
    // NAMESPACE is the namespace ID when running locally

    NAMESPACE = namespace;
    ACCOUNT_ID = account;
    EMAIL = email;
    API_KEY = apiKey;
}

async function get(key) {

    if (typeof self === 'undefined') {
        const response = await fetch(`${BASE_PATH}/${ACCOUNT_ID}/storage/kv/namespaces/${NAMESPACE}/values/${key}`,
            { headers: { 'X-Auth-Email': EMAIL, 'X-Auth-Key': API_KEY } });
        if (response.ok) {
            return response.text();
        }
        else {
            throw new Error(`${NAMESPACE}:${key} not found`);
        }
    }
    else {
        return self[NAMESPACE].get(key);
    }
}

async function put(key, value) {
    if (typeof self === 'undefined') {
        const response = await fetch(`${BASE_PATH}/${ACCOUNT_ID}/storage/kv/namespaces/${NAMESPACE}/values/${key}`,
            { headers: { 'X-Auth-Email': EMAIL, 'X-Auth-Key': API_KEY, 'Content-Type': 'text/plain' },
              method: 'PUT', body: `${value}`});
        if (response.ok) {
            return response.text();
        }
        else {
            throw new Error(`${NAMESPACE}:${key} not set to ${value}`);
        }
    }
    else {
        return self[NAMESPACE].put(key, value);
    }
}

async function del(key) {

    if (typeof self === 'undefined') {
        const response = await fetch(`${BASE_PATH}/${ACCOUNT_ID}/storage/kv/namespaces/${NAMESPACE}/values/${key}`,
            { headers: { 'X-Auth-Email': EMAIL, 'X-Auth-Key': API_KEY }, method: 'DELETE' });
        if (response.ok) {
            return response.text();
        }
        else {
            throw new Error(`${NAMESPACE}:${key} not deleted`);
        }
    }
    else {
        return self[NAMESPACE].delete(key);
    }
}

exports.init = init;
exports.get = get;
exports.put = put;
exports.del = del;
