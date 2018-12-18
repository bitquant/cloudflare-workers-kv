function get(namespace, key, type) {
    return self[namespace].get(key, type);
}

function put(namespace, key, value) {
    return self[namespace].put(key, value);
}

function del(namespace, key) {
    return self[namespace].delete(key);
}

exports.get = get;
exports.put = put;
exports.del = del;

