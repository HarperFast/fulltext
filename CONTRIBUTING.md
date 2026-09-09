# Contributing

## Setup

Install Node.js 22.18 or newer and Rust 1.90, then install JavaScript dependencies without running
package lifecycle scripts:

```bash
npm ci --ignore-scripts
```

## Checks

Run the same primary checks used in CI:

```bash
npm run format:check
npm run lint
npm test
```

Rust tests exercise the engine and directory contract without Node.js. Node tests build the addon,
load it through the public `./native` entry point, verify panic containment, and install the output
of `npm pack` into a temporary consumer project.

## Design constraints

- Keep search, indexing, scheduling, and lifecycle behavior shared across storage backends.
- Keep backend choice explicit; do not introduce automatic fallback.
- Do not link RocksDB into this addon. Harper storage uses the bounded host transport and
  Harper-owned RocksDB operations; the historical rocksdb-js lease remains test-only.
- Do not expose generated Node-API declarations as the public TypeScript API.
- Keep CPU and sustained I/O work off the Node.js event loop. The promise-shaped capability call may
  synchronously load the addon once; no search, indexing, commit, or storage operation gets that
  exception.
- Add a direct test for each source module.

Open an issue before changing a public package entry point, native ABI, persistence contract, or
supported-platform matrix.
