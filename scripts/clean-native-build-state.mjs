import { createHash } from 'node:crypto';
import { readFileSync, rmSync, utimesSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import path from 'node:path';

const require = createRequire(import.meta.url);
const cargoManifest = readFileSync(new URL('../Cargo.toml', import.meta.url), 'utf8');
const cargoPackageName = /^name\s*=\s*"([^"]+)"/m.exec(cargoManifest)?.[1];
if (!cargoPackageName) {
	throw new Error('Cargo package name is missing');
}
const cargoArtifactName = cargoPackageName.replaceAll('-', '_');
const napiCliVersion = require('@napi-rs/cli/package.json').version;
const cwdHash = createHash('sha256').update(process.cwd()).update(napiCliVersion).digest('hex').slice(0, 8);

rmSync(new URL('../ts/addon.d.ts', import.meta.url), { force: true });
for (const suffix of ['napi_type_def.tmp', 'napi_wasi_register.tmp']) {
	rmSync(path.join(tmpdir(), `${cargoArtifactName}-${cwdHash}.${suffix}`), { force: true });
}
const now = new Date();
utimesSync(new URL('../src/lib.rs', import.meta.url), now, now);
