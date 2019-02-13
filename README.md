# cloudflare-workers-kv

Use Cloudflare Workers KV in your local environment or within a Cloudflare Workers environment.  The package also provides support for handling values larger than the 64 kB limit by breaking the value into smaller chunks.

## Install
```
$ npm install cloudflare-workers-kv --save
```

## Usage
```javascript
var kv = require('cloudflare-workers-kv');

kv.init({
    variableBinding: '<variable name>',
    namespaceId: '<namespace id>',
    accountId: '<account id>',
    email: '<email>',
    apiKey: '<API key>'
});


// Write to the KV store
await kv.put('test-kv-key', 'test-key-value');

// Read from the KV store
var data = await kv.get('test-kv-key');

// Delete from the KV store
await kv.del('test-kv-key');

```

## Multi Environment Support
The library can be used within a worker running in Cloudflare as well as within any local test environment.  When running in a local test environment the library uses the Cloudflare KV rest API.  


## Large Value Support
If a value to be written exceeds the 64 kB Cloudflare limit, the value will be broken into chunks and stored as multiple values.  When reading back the value multiple reads will occur in parallel and the value will be pieced back together for use.  When deleting a large value the library will take care of deleting all chunks that were created.  If a key with a large value is overwritten with a new value the library will provide a "clean-up" id. It takes up to 10 seconds for KV changes to be written to all data centers.  In order to maintain value consistency the old chunks are not removed immediately after being overwritten. Use `kv.clean('<cleanup id>')` to remove the old chunks.  It is recommended to call `clean` no earlier than 10 seconds after receiving a clean-up id.

## License
MIT license; see [LICENSE](./LICENSE).
