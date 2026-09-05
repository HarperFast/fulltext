# @harperfast/fulltext

Native Tantivy full-text indexing for Node.js, with a native filesystem backend and a planned
caller-owned rocksdb-js backend for Harper.

This repository is under active development. The initial scaffold exposes runtime capability
information through the native entry point; indexing and search APIs are tracked separately and
are not yet available.

## Requirements

- Node.js 22.18 or newer, or Node.js 24 or newer
- Rust 1.90 when building from source

The package never compiles or downloads native code during installation. A supported prebuilt
artifact must be present for the executing platform.

## Native usage

```js
import { runtimeInfo } from '@harperfast/fulltext/native';

const info = await runtimeInfo();
console.log(info.tantivyVersion);
```

`runtimeInfo()` is asynchronous so later search and indexing operations can remain off the Node.js
event loop without changing the public calling convention.

## Storage boundaries

The package is designed around two explicit entry points:

- `@harperfast/fulltext/native` uses Tantivy's native directory implementation and has no
  rocksdb-js dependency.
- `@harperfast/fulltext/rocks` will use a caller-owned rocksdb-js database through a versioned
  native capability lease. It is not exported until that contract is implemented and tested.

There is no generic storage selector and no fallback between backends. Harper will consume only the
Rocks entry point. The fulltext addon will not link its own copy of RocksDB.

## Development

```bash
npm ci --ignore-scripts
npm run build:debug
npm test
npm run lint
npm run format:check
```

Generated Node-API declarations in `ts/addon.d.ts` are private implementation types. Consumers use
only the types exported from a package entry point.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow and
[docs/scaffold-design.md](docs/scaffold-design.md) for the architecture behind the initial package.

## License

Apache-2.0
