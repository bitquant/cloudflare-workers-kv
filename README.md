# cloudflare-workers-kv

Use Cloudflare Workers KV in your local environment or within a Cloudflare Workers environment.  The package also provides support for handling values larger than the 25 MB limit by breaking the value into smaller chunks.

## Install
```
$ npm install cloudflare-workers-kv --save
```

## Usage
```javascript
var kv = require('cloudflare-workers-kv');

// These dependencies are needed when running in Node
global.fetch = require('node-fetch');
var util = require('util');
global.TextEncoder = util.TextEncoder;
global.TextDecoder = util.TextDecoder;

(async () => {
    kv.init({
        variableBinding: '<variable name>',
        namespaceId: '<namespace id>',
        accountId: '<account id>',
        email: '<email>',
        apiKey: '<API key>'
    });

    // Write to the KV store
    await kv.put('test-kv-key', 'test-key-value');

    // Write to the KV store with an expiration
    await kv.put('test-kv-key-exp', 'test-key-value', { expirationTtl: 60 });

    // Read from the KV store
    var data = await kv.get('test-kv-key');

    // Delete from the KV store
    await kv.del('test-kv-key');

    // Bulk write to the KV store - does not support values larger than 25 MB
    await kv.putMulti([
        { key: 'key-abc', value: 'value-abc' },
        { key: 'key-xyz', value: 'value-xyz' },
        { key: 'key-123', value: 'value-123', expirationTtl: 60 }
    ])

    // List all keys
    await kv.list();

    // List keys with cursor, prefix and result limit
    await kv.list({prefix: "abc", limit: 75, cursor: "2d840f03-df70-4d93-afe4-5f83856f6214"})
})();

```

## Multi Environment Support
The library can be used within a worker running in Cloudflare as well as within any local test environment.  When running in a local test environment the library uses the Cloudflare KV rest API.  


## Large Value Support
If a value to be written exceeds the 25 MB Cloudflare limit, the value will be broken into chunks and stored as multiple values.  When reading back the value multiple reads will occur in parallel and the value will be pieced back together for use.  When deleting a large value the library will take care of deleting all chunks that were created.  If a key with a large value is overwritten with a new value the library will provide a "clean-up" id. It takes up to 60 seconds for KV changes to be written to all data centers.  In order to maintain value consistency the old chunks are not removed immediately after being overwritten. Use `kv.clean('<cleanup id>')` to remove the old chunks.  It is recommended to call `clean` no earlier than 60 seconds after receiving a clean-up id.

*NOTE*: kv.putMulti() does not support values larger than 25 MB!

## License
MIT license; see [LICENSE](./LICENSE).
