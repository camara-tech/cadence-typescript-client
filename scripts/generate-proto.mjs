#!/usr/bin/env node
// Generates TypeScript protobuf/gRPC code from the cadence IDL submodule
// (idls/proto/uber/cadence/api/v1) into src/generated using ts-proto.
import { execSync } from 'node:child_process';
import { existsSync, mkdirSync, rmSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const idlDir = process.env.CADENCE_IDL_DIR ?? resolve(repoRoot, '../../cadence-workflow/cadence/idls/proto');
const outDir = join(repoRoot, 'src', 'generated');

if (!existsSync(idlDir)) {
  console.error(`cadence IDL not found at ${idlDir}`);
  console.error('clone https://github.com/cadence-workflow/cadence next to this repo or set CADENCE_IDL_DIR');
  process.exit(1);
}

rmSync(outDir, { recursive: true, force: true });
mkdirSync(outDir, { recursive: true });

const protoc = execSync('which protoc', { encoding: 'utf8' }).trim();
const protocInclude = resolve(protoc, '..', '..', 'include'); // nix protobuf ships well-known types

const args = [
  `protoc`,
  `-I ${idlDir}`,
  `-I ${protocInclude}`,
  `--plugin=protoc-gen-ts_proto=${repoRoot}/node_modules/.bin/protoc-gen-ts_proto`,
  `--ts_proto_out=${outDir}`,
  `--ts_proto_opt=esModuleInterop=true,useDate=false,forceLong=string,snakeToCamel=true,outputClientImpl=true,addGrpcMetadata=false,importSuffix=.js`,
  ...[
    'uber/cadence/api/v1/service_domain.proto',
    'uber/cadence/api/v1/service_meta.proto',
    'uber/cadence/api/v1/service_workflow.proto',
    'uber/cadence/api/v1/service_worker.proto',
    'uber/cadence/api/v1/service_visibility.proto',
    'uber/cadence/api/v1/service_schedule.proto',
  ].map((p) => join(idlDir, p)),
];

execSync(args.join(' '), { stdio: 'inherit' });
console.log('proto codegen complete → src/generated');
